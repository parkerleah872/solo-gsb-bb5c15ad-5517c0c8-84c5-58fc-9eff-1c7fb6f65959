"""情景注入台 —— FastAPI 主程序。

职责：
- 房间 / 参与组 / 注入卡片的 REST 管理
- 可暂停 / 继续的虚拟时钟（暂停期间绝不推进）
- 后台 tick：按 (trigger_seconds, seq) 稳定次序揭示到期卡片
- 卡片回执：参与者对已揭示的本组可见卡片标记/撤销「已收到」，
  同一参与组仅保留一条回执；主持人实时看到每组回执/未回执汇总
- 限时观察席分享：主持人选择 1~3 个参与组并设置 5~120 分钟有效期，
  生成可撤销的随机观察链接；观察者只读，仅能收到已揭示的全员卡片与
  所选组卡片，拿不到未揭示卡、房间码、组加入码、回执明细与控制能力；
  到期或撤销时立即断开在线观察者，凭证此后一律失效
- WebSocket：先下发权威快照，再广播单调递增的增量事件
- 分组定向：参与者凭组加入码连接，快照与后续事件都按该组过滤；
  未命中的卡片在触发前后都不通过 REST 或 WebSocket 下发，主持人始终可见全部
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import sqlite3
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import NamedTuple

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import db as dbmod

log = logging.getLogger("inject")
logging.basicConfig(level=logging.INFO)

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
TICK_INTERVAL = float(os.environ.get("TICK_INTERVAL", "0.25"))
CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # 去掉易混淆字符
MAX_GROUPS = 8  # 每个房间最多 8 个参与组

db: sqlite3.Connection
# 所有读改写 + 广播都在这把锁下串行，保证“快照先于其后发生的事件”这一顺序。
state_lock = asyncio.Lock()


# ---------------------------------------------------------------- 工具

def now_ms() -> int:
    return int(time.time() * 1000)


def room_elapsed_ms(room: sqlite3.Row) -> int:
    """虚拟时钟当前值：running 时 = 冻结值 + 本次运行段，否则 = 冻结值。"""
    elapsed = room["elapsed_ms"]
    if room["status"] == "running" and room["last_resume_at"] is not None:
        elapsed += int((time.time() - room["last_resume_at"]) * 1000)
    return max(0, elapsed)


def room_public(room: sqlite3.Row, *, include_code: bool = True) -> dict:
    data = {
        "name": room["name"],
        "status": room["status"],
        "elapsed_ms": room_elapsed_ms(room),
        "server_now_ms": now_ms(),
    }
    if include_code:
        # 房间码即主持人凭证，绝不随参与者快照下发
        data["code"] = room["code"]
    return data


def group_public(group: sqlite3.Row) -> dict:
    return {
        "id": group["id"],
        "name": group["name"],
        "join_code": group["join_code"],
    }


def card_public(card: sqlite3.Row) -> dict:
    return {
        "id": card["id"],
        "seq": card["seq"],
        "trigger_seconds": card["trigger_seconds"],
        "title": card["title"],
        "content": card["content"],
        "group_id": card["group_id"],  # None = 全员
        "revealed": bool(card["revealed"]),
        "revealed_at_ms": card["revealed_at_ms"],
    }


def get_room_or_404(code: str) -> sqlite3.Row:
    room = db.execute("SELECT * FROM rooms WHERE code = ?", (code.upper(),)).fetchone()
    if room is None:
        raise HTTPException(status_code=404, detail="room not found")
    return room


def get_group_or_404(room_id: int, group_id: int) -> sqlite3.Row:
    group = db.execute(
        "SELECT * FROM groups WHERE id = ? AND room_id = ?", (group_id, room_id)
    ).fetchone()
    if group is None:
        raise HTTPException(status_code=404, detail="group not found")
    return group


def code_taken(code: str) -> bool:
    """房间码与组加入码共用同一命名空间，互不可重复。"""
    if db.execute("SELECT 1 FROM rooms WHERE code = ?", (code,)).fetchone():
        return True
    return db.execute("SELECT 1 FROM groups WHERE join_code = ?", (code,)).fetchone() is not None


def fresh_code() -> str:
    while True:
        code = "".join(secrets.choice(CODE_ALPHABET) for _ in range(6))
        if not code_taken(code):
            return code


def resolve_code(code: str) -> tuple[sqlite3.Row | None, int | None]:
    """把连接用码解析为 (房间, 视野)。视野 None = 主持人（全部可见），否则为参与组 id。"""
    room = db.execute("SELECT * FROM rooms WHERE code = ?", (code,)).fetchone()
    if room is not None:
        return room, None
    group = db.execute("SELECT * FROM groups WHERE join_code = ?", (code,)).fetchone()
    if group is not None:
        room = db.execute("SELECT * FROM rooms WHERE id = ?", (group["room_id"],)).fetchone()
        return room, group["id"]
    return None, None


# ---------------------------------------------------------------- 观察席分享

# 视野（scope）三种取值：None = 主持人（全量）；int = 参与组 id（玩家）；
# ObserverScope = 观察者（只读、仅已揭示、限定所选参与组）。
class ObserverScope(NamedTuple):
    token: str                     # 观察凭证（撤销/到期时按它断开连接）
    group_ids: frozenset[int]      # 主持人选定的 1~3 个参与组


def share_active(share: sqlite3.Row) -> bool:
    """凭证当前是否有效：未撤销且未到期。"""
    return not share["revoked"] and share["expires_at"] > time.time()


def observer_scope_from(share: sqlite3.Row) -> ObserverScope:
    return ObserverScope(token=share["token"],
                         group_ids=frozenset(json.loads(share["group_ids"])))


def get_active_share_or_410(token: str) -> sqlite3.Row:
    """解析观察凭证：不存在 404；已撤销/已到期 410（刷新重连均不可恢复）。"""
    share = db.execute(
        "SELECT * FROM observer_shares WHERE token = ?", (token,)
    ).fetchone()
    if share is None:
        raise HTTPException(status_code=404, detail="share not found")
    if not share_active(share):
        raise HTTPException(status_code=410, detail="share expired or revoked")
    return share


def share_public(share: sqlite3.Row) -> dict:
    """主持人视角的分享序列化。含观察凭证，只随主持人快照/事件下发，绝不给玩家与观察者。"""
    gids: list[int] = json.loads(share["group_ids"])
    names: dict[int, str] = {}
    if gids:
        marks = ",".join("?" * len(gids))
        for row in db.execute(f"SELECT id, name FROM groups WHERE id IN ({marks})", gids):
            names[row["id"]] = row["name"]
    revoked = bool(share["revoked"])
    expired = share["expires_at"] <= time.time()
    return {
        "token": share["token"],
        "group_ids": gids,
        "group_names": [names.get(gid, f"#{gid}") for gid in gids],
        "minutes": max(1, round((share["expires_at"] - share["created_at"]) / 60)),
        "created_at": share["created_at"],
        "expires_at": share["expires_at"],
        "status": "revoked" if revoked else ("expired" if expired else "active"),
    }


def build_snapshot(room: sqlite3.Row, scope: int | None | ObserverScope) -> dict:
    """权威全量状态，按视野过滤：主持人拿全部；参与者只拿全员卡片与本组卡片；
    观察者只拿已揭示的全员卡片与所选组卡片（只读，无回执数据）。

    回执数据同样按视野下发：主持人拿到每张卡的 expected/acked 汇总，
    参与者只拿到本组是否已回执（my_receipt），观察者完全拿不到回执。
    """
    if isinstance(scope, ObserverScope):
        gids = sorted(scope.group_ids)
        marks = ",".join("?" * len(gids))
        cards = db.execute(
            f"""SELECT * FROM cards
                WHERE room_id = ? AND revealed = 1
                  AND (group_id IS NULL OR group_id IN ({marks}))
                ORDER BY trigger_seconds, seq""",
            (room["id"], *gids),
        ).fetchall()
        groups = db.execute(
            f"SELECT * FROM groups WHERE id IN ({marks}) ORDER BY id", gids
        ).fetchall()
        last_id = db.execute(
            "SELECT COALESCE(MAX(id), 0) AS m FROM events WHERE room_id = ?", (room["id"],)
        ).fetchone()["m"]
        return {
            "kind": "snapshot",
            "last_event_id": last_id,
            "server_now_ms": now_ms(),
            "scope": {"role": "observer", "group_ids": gids},
            "room": room_public(room, include_code=False),       # 观察者拿不到房间码
            "groups": [{"id": g["id"], "name": g["name"]} for g in groups],  # 不含组加入码
            "cards": [card_public(c) for c in cards],            # 仅已揭示；无回执字段
        }
    if scope is None:
        cards = db.execute(
            "SELECT * FROM cards WHERE room_id = ? ORDER BY trigger_seconds, seq",
            (room["id"],),
        ).fetchall()
        groups = db.execute(
            "SELECT * FROM groups WHERE room_id = ? ORDER BY id", (room["id"],)
        ).fetchall()
    else:
        cards = db.execute(
            """SELECT * FROM cards
               WHERE room_id = ? AND (group_id IS NULL OR group_id = ?)
               ORDER BY trigger_seconds, seq""",
            (room["id"], scope),
        ).fetchall()
        groups = db.execute("SELECT * FROM groups WHERE id = ?", (scope,)).fetchall()
    last_id = db.execute(
        "SELECT COALESCE(MAX(id), 0) AS m FROM events WHERE room_id = ?", (room["id"],)
    ).fetchone()["m"]
    snapshot = {
        "kind": "snapshot",
        "last_event_id": last_id,
        "server_now_ms": now_ms(),
        "scope": {"role": "host" if scope is None else "player", "group_id": scope},
        "room": room_public(room, include_code=scope is None),
        "groups": [group_public(g) for g in groups],
        "cards": snapshot_cards(room, cards, groups, scope),
    }
    if scope is None:
        # 主持人快照附带全部观察分享（有效/失效），供控制台列出与管理
        shares = db.execute(
            "SELECT * FROM observer_shares WHERE room_id = ? ORDER BY id DESC", (room["id"],)
        ).fetchall()
        snapshot["shares"] = [share_public(s) for s in shares]
    return snapshot


def snapshot_cards(room: sqlite3.Row, cards: list[sqlite3.Row],
                   groups: list[sqlite3.Row], scope: int | None) -> list[dict]:
    """为快照中的卡片附加回执视图（按视野分别计算）。"""
    if scope is None:
        # 主持人：每张卡 expected = 全员卡取全部参与组 / 定向卡取目标组；acked = 已回执组
        acked_map: dict[int, set[int]] = {}
        for row in db.execute(
            "SELECT card_id, group_id FROM receipts WHERE room_id = ?", (room["id"],)
        ):
            acked_map.setdefault(row["card_id"], set()).add(row["group_id"])
        group_ids = [g["id"] for g in groups]
        out = []
        for card in cards:
            data = card_public(card)
            expected = group_ids if card["group_id"] is None else [card["group_id"]]
            acked = acked_map.get(card["id"], set())
            data["receipts"] = {
                "expected": expected,
                "acked": [gid for gid in expected if gid in acked],
            }
            out.append(data)
        return out
    # 参与者：仅本组是否已回执
    mine = {
        row["card_id"]
        for row in db.execute(
            "SELECT card_id FROM receipts WHERE room_id = ? AND group_id = ?",
            (room["id"], scope),
        )
    }
    out = []
    for card in cards:
        data = card_public(card)
        data["my_receipt"] = card["id"] in mine
        out.append(data)
    return out


# ---------------------------------------------------------------- 广播

def event_visible(scope: int | None | ObserverScope, type_: str, payload: dict) -> bool:
    """事件是否应投递给该视野。主持人全收；参与者只收全员/本组卡片事件与房间级事件；
    观察者只收已揭示卡片揭示/移除与房间级事件。"""
    if scope is None:
        return True
    if type_.startswith("share_"):
        return False  # 分享事件含观察凭证，仅主持人可见
    if isinstance(scope, ObserverScope):
        if type_ in ("card_revealed", "card_removed"):
            gid = payload.get("group_id")
            return gid is None or gid in scope.group_ids
        if type_ in ("group_renamed", "group_removed"):
            return payload.get("id") in scope.group_ids
        if type_ in ("card_added", "group_added", "receipt_updated"):
            return False  # 未揭示卡、组加入码、回执明细一律不下发
        return True  # clock / reset 等房间级事件
    if type_ in ("card_added", "card_revealed", "card_removed"):
        gid = payload.get("group_id")
        return gid is None or gid == scope
    if type_ == "receipt_updated":
        return payload.get("group_id") == scope  # 参与者只感知本组的回执变化
    if type_ == "group_renamed":
        return payload.get("id") == scope  # 参与者只需感知本组改名
    if type_ in ("group_added", "group_removed"):
        return False
    return True  # clock / reset 等房间级事件


class Hub:
    """按房间维护连接及其视野（None=主持人，int=参与组，ObserverScope=观察者），
    广播时按视野过滤。"""

    def __init__(self) -> None:
        self._rooms: dict[str, dict[WebSocket, int | None | ObserverScope]] = {}

    def add(self, code: str, ws: WebSocket, scope: int | None | ObserverScope) -> None:
        self._rooms.setdefault(code, {})[ws] = scope

    def discard(self, code: str, ws: WebSocket) -> None:
        subs = self._rooms.get(code)
        if subs is not None:
            subs.pop(ws, None)
            if not subs:
                self._rooms.pop(code, None)

    async def broadcast(self, code: str, message: dict) -> None:
        type_ = message.get("type", "")
        payload = message.get("payload") or {}
        for ws, scope in list(self._rooms.get(code, {}).items()):
            if not event_visible(scope, type_, payload):
                continue
            try:
                await ws.send_json(message)
            except Exception:
                self.discard(code, ws)

    async def close_scope(self, code: str, scope: int, message: dict) -> None:
        """组被删除后，主动断开该组所有参与者连接。"""
        for ws, ws_scope in list(self._rooms.get(code, {}).items()):
            if ws_scope != scope:
                continue
            try:
                await ws.send_json(message)
                await ws.close(code=4404)
            except Exception:
                pass
            self.discard(code, ws)

    async def close_observer(self, code: str, token: str, message: dict) -> None:
        """分享撤销/到期后，主动断开该观察凭证的所有连接。"""
        for ws, ws_scope in list(self._rooms.get(code, {}).items()):
            if not (isinstance(ws_scope, ObserverScope) and ws_scope.token == token):
                continue
            try:
                await ws.send_json(message)
                await ws.close(code=4403)
            except Exception:
                pass
            self.discard(code, ws)


hub = Hub()


async def publish(code: str, room_id: int, type_: str, payload: dict) -> None:
    """落库一条递增事件并按视野广播。必须在 state_lock 下调用。"""
    cur = db.execute(
        "INSERT INTO events (room_id, type, payload, created_at) VALUES (?, ?, ?, ?)",
        (room_id, type_, json.dumps(payload, ensure_ascii=False), time.time()),
    )
    db.commit()
    await hub.broadcast(code, {
        "kind": "event",
        "id": cur.lastrowid,
        "type": type_,
        "payload": payload,
        "server_now_ms": now_ms(),
    })


# ---------------------------------------------------------------- 时钟 tick

async def expire_due_shares() -> None:
    """把刚到期的观察分享标记失效：广播 share_expired 给主持人、断开在线观察者。

    须在 state_lock 下调用。expired_notified 持久化，保证重启后不会重复广播；
    到期判定以 expires_at 为准，凭证此后 REST/WS 一律 410，刷新重连不可恢复。
    """
    due = db.execute(
        """SELECT s.id, s.room_id, s.token, r.code AS room_code
           FROM observer_shares s JOIN rooms r ON r.id = s.room_id
           WHERE s.expires_at <= ? AND s.revoked = 0 AND s.expired_notified = 0""",
        (time.time(),),
    ).fetchall()
    for share in due:
        db.execute("UPDATE observer_shares SET expired_notified = 1 WHERE id = ?", (share["id"],))
        db.commit()
        await publish(share["room_code"], share["room_id"], "share_expired",
                      {"token": share["token"]})
        await hub.close_observer(share["room_code"], share["token"],
                                 {"kind": "error", "message": "观察链接已到期"})
        log.info("share %s expired, observers disconnected", share["token"][:8])


async def ticker_loop() -> None:
    """周期性扫描 running 房间，按 (trigger_seconds, seq) 稳定揭示到期卡片。"""
    while True:
        await asyncio.sleep(TICK_INTERVAL)
        try:
            async with state_lock:
                running = db.execute("SELECT * FROM rooms WHERE status = 'running'").fetchall()
                for room in running:
                    elapsed = room_elapsed_ms(room)
                    due = db.execute(
                        """SELECT * FROM cards
                           WHERE room_id = ? AND revealed = 0 AND trigger_seconds * 1000 <= ?
                           ORDER BY trigger_seconds, seq""",
                        (room["id"], elapsed),
                    ).fetchall()
                    for card in due:
                        db.execute(
                            "UPDATE cards SET revealed = 1, revealed_at_ms = ? WHERE id = ?",
                            (elapsed, card["id"]),
                        )
                        db.commit()
                        fresh = db.execute("SELECT * FROM cards WHERE id = ?", (card["id"],)).fetchone()
                        await publish(room["code"], room["id"], "card_revealed", card_public(fresh))
                        log.info("room %s revealed card #%s (%s)", room["code"], card["seq"], card["title"])
                await expire_due_shares()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("ticker error")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db
    db = dbmod.connect()
    dbmod.init_schema(db)  # 启动即完成数据库初始化
    task = asyncio.create_task(ticker_loop())
    log.info("inject console ready, db=%s", dbmod.DB_PATH)
    yield
    task.cancel()
    db.close()


app = FastAPI(title="情景注入台", lifespan=lifespan)


# ---------------------------------------------------------------- REST：房间

class RoomIn(BaseModel):
    name: str = Field(default="", max_length=120)


@app.post("/api/rooms", status_code=201)
async def create_room(body: RoomIn):
    async with state_lock:
        code = fresh_code()
        db.execute(
            "INSERT INTO rooms (code, name, created_at) VALUES (?, ?, ?)",
            (code, body.name.strip(), time.time()),
        )
        db.commit()
        return {"code": code, "name": body.name.strip()}


@app.get("/api/rooms/{code}")
async def get_room(code: str):
    """主持人快照：含全部卡片与全部分组（房间码即主持人凭证）。"""
    async with state_lock:
        room = get_room_or_404(code)
        return build_snapshot(room, None)


@app.get("/api/join/{join_code}")
async def join_room(join_code: str):
    """参与者入口：凭组加入码换取按该组过滤的快照（不含房间码与他组卡片）。"""
    async with state_lock:
        group = db.execute(
            "SELECT * FROM groups WHERE join_code = ?", (join_code.upper(),)
        ).fetchone()
        if group is None:
            raise HTTPException(status_code=404, detail="join code not found")
        room = db.execute("SELECT * FROM rooms WHERE id = ?", (group["room_id"],)).fetchone()
        return build_snapshot(room, group["id"])


# ---------------------------------------------------------------- REST：参与组

class GroupIn(BaseModel):
    name: str = Field(min_length=1, max_length=60)


@app.post("/api/rooms/{code}/groups", status_code=201)
async def add_group(code: str, body: GroupIn):
    async with state_lock:
        room = get_room_or_404(code)
        count = db.execute(
            "SELECT COUNT(*) AS c FROM groups WHERE room_id = ?", (room["id"],)
        ).fetchone()["c"]
        if count >= MAX_GROUPS:
            raise HTTPException(status_code=409, detail=f"at most {MAX_GROUPS} groups per room")
        cur = db.execute(
            "INSERT INTO groups (room_id, name, join_code, created_at) VALUES (?, ?, ?, ?)",
            (room["id"], body.name.strip(), fresh_code(), time.time()),
        )
        db.commit()
        group = db.execute("SELECT * FROM groups WHERE id = ?", (cur.lastrowid,)).fetchone()
        payload = group_public(group)
        await publish(room["code"], room["id"], "group_added", payload)
        return payload


@app.patch("/api/rooms/{code}/groups/{group_id}")
async def rename_group(code: str, group_id: int, body: GroupIn):
    async with state_lock:
        room = get_room_or_404(code)
        get_group_or_404(room["id"], group_id)
        name = body.name.strip()
        db.execute("UPDATE groups SET name = ? WHERE id = ?", (name, group_id))
        db.commit()
        payload = {"id": group_id, "name": name}
        await publish(room["code"], room["id"], "group_renamed", payload)
        return payload


@app.delete("/api/rooms/{code}/groups/{group_id}")
async def delete_group(code: str, group_id: int):
    async with state_lock:
        room = get_room_or_404(code)
        get_group_or_404(room["id"], group_id)
        refs = db.execute(
            "SELECT COUNT(*) AS c FROM cards WHERE group_id = ?", (group_id,)
        ).fetchone()["c"]
        if refs:
            raise HTTPException(status_code=409, detail=f"group is targeted by {refs} card(s)")
        db.execute("DELETE FROM groups WHERE id = ?", (group_id,))
        db.commit()
        await publish(room["code"], room["id"], "group_removed", {"id": group_id})
        # 加入码即刻失效，踢下仍在连接的该组参与者
        await hub.close_scope(room["code"], group_id, {"kind": "error", "message": "所在参与组已被主持人删除"})
        return {"ok": True}


# ---------------------------------------------------------------- REST：卡片

class CardIn(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    content: str = Field(default="", max_length=4000)
    trigger_seconds: int = Field(ge=0, le=24 * 3600)
    group_id: int | None = None  # None = 全员，否则仅该参与组可见


@app.post("/api/rooms/{code}/cards", status_code=201)
async def add_card(code: str, body: CardIn):
    async with state_lock:
        room = get_room_or_404(code)
        if room["status"] == "running":
            raise HTTPException(status_code=409, detail="clock is running; pause before editing cards")
        if body.group_id is not None:
            get_group_or_404(room["id"], body.group_id)  # 目标组必须属于本房间
        seq = db.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 AS s FROM cards WHERE room_id = ?", (room["id"],)
        ).fetchone()["s"]
        cur = db.execute(
            "INSERT INTO cards (room_id, seq, trigger_seconds, title, content, group_id)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (room["id"], seq, body.trigger_seconds, body.title.strip(), body.content, body.group_id),
        )
        db.commit()
        card = db.execute("SELECT * FROM cards WHERE id = ?", (cur.lastrowid,)).fetchone()
        payload = card_public(card)
        await publish(room["code"], room["id"], "card_added", payload)
        return payload


@app.delete("/api/rooms/{code}/cards/{card_id}")
async def delete_card(code: str, card_id: int):
    async with state_lock:
        room = get_room_or_404(code)
        if room["status"] == "running":
            raise HTTPException(status_code=409, detail="clock is running; pause before editing cards")
        card = db.execute(
            "SELECT * FROM cards WHERE id = ? AND room_id = ?", (card_id, room["id"])
        ).fetchone()
        if card is None:
            raise HTTPException(status_code=404, detail="card not found")
        db.execute("DELETE FROM cards WHERE id = ?", (card_id,))
        db.commit()
        # 带上 group_id，广播过滤才能判断该事件是否属于某参与组的视野
        await publish(room["code"], room["id"], "card_removed",
                      {"id": card_id, "group_id": card["group_id"]})
        return {"ok": True}


# ---------------------------------------------------------------- REST：回执

def resolve_receipt_target(join_code: str, card_id: int) -> tuple[sqlite3.Row, sqlite3.Row]:
    """把参与者回执请求解析为 (参与组, 卡片)。

    参与者凭组加入码操作，且只能操作本组可见的卡片（全员卡或目标为本组的卡）；
    不可见的卡片一律 404，不向参与者暴露其存在。
    """
    group = db.execute(
        "SELECT * FROM groups WHERE join_code = ?", (join_code.upper(),)
    ).fetchone()
    if group is None:
        raise HTTPException(status_code=404, detail="join code not found")
    card = db.execute(
        """SELECT * FROM cards
           WHERE id = ? AND room_id = ? AND (group_id IS NULL OR group_id = ?)""",
        (card_id, group["room_id"], group["id"]),
    ).fetchone()
    if card is None:
        raise HTTPException(status_code=404, detail="card not found")
    return group, card


@app.post("/api/join/{join_code}/cards/{card_id}/receipt")
async def ack_card(join_code: str, card_id: int):
    """参与者标记「已收到」。幂等：同一参与组对同一卡片仅保留一条回执。"""
    async with state_lock:
        group, card = resolve_receipt_target(join_code, card_id)
        if not card["revealed"]:
            raise HTTPException(status_code=409, detail="card not revealed yet")
        existing = db.execute(
            "SELECT 1 FROM receipts WHERE card_id = ? AND group_id = ?",
            (card_id, group["id"]),
        ).fetchone()
        if existing is None:
            db.execute(
                "INSERT INTO receipts (room_id, card_id, group_id, created_at)"
                " VALUES (?, ?, ?, ?)",
                (group["room_id"], card_id, group["id"], time.time()),
            )
            db.commit()
            room = db.execute("SELECT * FROM rooms WHERE id = ?", (group["room_id"],)).fetchone()
            await publish(room["code"], room["id"], "receipt_updated",
                          {"card_id": card_id, "group_id": group["id"], "acked": True})
        return {"card_id": card_id, "group_id": group["id"], "acked": True}


@app.delete("/api/join/{join_code}/cards/{card_id}/receipt")
async def unack_card(join_code: str, card_id: int):
    """参与者撤销回执。幂等：本就没有回执时直接返回当前状态。"""
    async with state_lock:
        group, card = resolve_receipt_target(join_code, card_id)
        cur = db.execute(
            "DELETE FROM receipts WHERE card_id = ? AND group_id = ?",
            (card_id, group["id"]),
        )
        db.commit()
        if cur.rowcount:
            room = db.execute("SELECT * FROM rooms WHERE id = ?", (group["room_id"],)).fetchone()
            await publish(room["code"], room["id"], "receipt_updated",
                          {"card_id": card_id, "group_id": group["id"], "acked": False})
        return {"card_id": card_id, "group_id": group["id"], "acked": False}


# ---------------------------------------------------------------- REST：观察席分享

class ShareIn(BaseModel):
    group_ids: list[int] = Field(min_length=1, max_length=3)  # 1～3 个参与组
    minutes: int = Field(ge=5, le=120)                        # 有效期 5～120 分钟


@app.post("/api/rooms/{code}/shares", status_code=201)
async def create_share(code: str, body: ShareIn):
    """主持人创建限时观察分享：随机凭证 + 到期时刻，落库持久化、可随时撤销。"""
    async with state_lock:
        room = get_room_or_404(code)
        group_ids = list(dict.fromkeys(body.group_ids))  # 去重保序
        if not group_ids:
            raise HTTPException(status_code=400, detail="select at least one group")
        for gid in group_ids:
            get_group_or_404(room["id"], gid)  # 每个目标组都必须属于本房间
        token = secrets.token_urlsafe(16)  # 128 位随机观察凭证，不可枚举
        db.execute(
            "INSERT INTO observer_shares (room_id, token, group_ids, expires_at, created_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (room["id"], token, json.dumps(group_ids), time.time() + body.minutes * 60, time.time()),
        )
        db.commit()
        share = db.execute("SELECT * FROM observer_shares WHERE token = ?", (token,)).fetchone()
        payload = share_public(share)
        await publish(room["code"], room["id"], "share_created", payload)
        return payload


@app.delete("/api/rooms/{code}/shares/{token}")
async def revoke_share(code: str, token: str):
    """撤销观察分享（幂等）：凭证即刻失效，该凭证的在线观察者立即被断开。"""
    async with state_lock:
        room = get_room_or_404(code)
        share = db.execute(
            "SELECT * FROM observer_shares WHERE token = ? AND room_id = ?",
            (token, room["id"]),
        ).fetchone()
        if share is None:
            raise HTTPException(status_code=404, detail="share not found")
        if not share["revoked"]:
            db.execute("UPDATE observer_shares SET revoked = 1 WHERE id = ?", (share["id"],))
            db.commit()
            await publish(room["code"], room["id"], "share_revoked", {"token": token})
        await hub.close_observer(room["code"], token,
                                 {"kind": "error", "message": "观察链接已被主持人撤销"})
        return {"ok": True, "token": token, "status": "revoked"}


@app.get("/api/observe/{token}")
async def observe_snapshot(token: str):
    """观察者入口：凭有效观察凭证换取只读快照。

    快照只含已揭示的全员卡片与所选组卡片；不含房间码、组加入码、
    未揭示卡片与回执明细。凭证撤销或到期后一律 410，刷新重连不可恢复。
    """
    async with state_lock:
        share = get_active_share_or_410(token)
        room = db.execute("SELECT * FROM rooms WHERE id = ?", (share["room_id"],)).fetchone()
        return build_snapshot(room, observer_scope_from(share))


# ---------------------------------------------------------------- REST：时钟控制

async def _clock_action(code: str, expect: tuple[str, ...], action: str, sql: str, params: tuple):
    async with state_lock:
        room = get_room_or_404(code)
        if room["status"] not in expect:
            raise HTTPException(status_code=409, detail=f"cannot {action} while status={room['status']}")
        db.execute(sql, (*params, room["id"]))
        db.commit()
        fresh = db.execute("SELECT * FROM rooms WHERE id = ?", (room["id"],)).fetchone()
        payload = {"action": action, "status": fresh["status"], "elapsed_ms": room_elapsed_ms(fresh)}
        await publish(room["code"], room["id"], "clock", payload)
        return payload


@app.post("/api/rooms/{code}/start")
async def start_room(code: str):
    return await _clock_action(
        code, ("idle",), "started",
        "UPDATE rooms SET status = 'running', elapsed_ms = 0, last_resume_at = ? WHERE id = ?",
        (time.time(),),
    )


@app.post("/api/rooms/{code}/pause")
async def pause_room(code: str):
    async with state_lock:
        room = get_room_or_404(code)
        if room["status"] != "running":
            raise HTTPException(status_code=409, detail=f"cannot pause while status={room['status']}")
        elapsed = room_elapsed_ms(room)  # 冻结当前值，暂停期间不再推进
        db.execute(
            "UPDATE rooms SET status = 'paused', elapsed_ms = ?, last_resume_at = NULL WHERE id = ?",
            (elapsed, room["id"]),
        )
        db.commit()
        payload = {"action": "paused", "status": "paused", "elapsed_ms": elapsed}
        await publish(room["code"], room["id"], "clock", payload)
        return payload


@app.post("/api/rooms/{code}/resume")
async def resume_room(code: str):
    return await _clock_action(
        code, ("paused",), "resumed",
        "UPDATE rooms SET status = 'running', last_resume_at = ? WHERE id = ?",
        (time.time(),),
    )


@app.post("/api/rooms/{code}/reset")
async def reset_room(code: str):
    """归零时钟并收回已揭示卡片；参与组、加入码与卡片目标全部保留。

    回执随揭示一并收回：未揭示卡片不可回执，新一轮演练从零开始统计。
    """
    async with state_lock:
        room = get_room_or_404(code)
        if room["status"] == "running":
            raise HTTPException(status_code=409, detail="pause before reset")
        db.execute(
            "UPDATE rooms SET status = 'idle', elapsed_ms = 0, last_resume_at = NULL WHERE id = ?",
            (room["id"],),
        )
        db.execute("UPDATE cards SET revealed = 0, revealed_at_ms = NULL WHERE room_id = ?", (room["id"],))
        db.execute("DELETE FROM receipts WHERE room_id = ?", (room["id"],))
        db.commit()
        payload = {"action": "reset", "status": "idle", "elapsed_ms": 0}
        await publish(room["code"], room["id"], "reset", payload)
        return payload


# ---------------------------------------------------------------- WebSocket

@app.websocket("/ws/{code}")
async def room_ws(websocket: WebSocket, code: str, last_event_id: int = 0):
    """重连协议：客户端带上已应用的最大事件 ID。

    码可以是房间码（主持人，全量视野）或组加入码（参与者，按组过滤）。
    服务端在锁内先登记连接、再发权威快照（快照水位不低于客户端水位），
    之后的增量事件按递增 ID 广播并按视野过滤；客户端丢弃 id <= 水位的事件，状态不回退。
    """
    code = code.upper()
    await websocket.accept()
    async with state_lock:
        room, scope = resolve_code(code)
        if room is None:
            await websocket.send_json({"kind": "error", "message": "room or join code not found"})
            await websocket.close(code=4404)
            return
        hub_key = room["code"]
        hub.add(hub_key, websocket, scope)
        snapshot = build_snapshot(room, scope)
        snapshot["last_event_id"] = max(snapshot["last_event_id"], last_event_id)
        await websocket.send_json(snapshot)
    try:
        while True:
            await websocket.receive_text()  # 客户端无需上行；仅用于感知断线
    except WebSocketDisconnect:
        pass
    finally:
        hub.discard(hub_key, websocket)


@app.websocket("/ws/observe/{token}")
async def observe_ws(websocket: WebSocket, token: str, last_event_id: int = 0):
    """观察者实时通道：与参与者同一套「先快照后增量」协议，事件按观察视野过滤。

    凭证失效（撤销/到期）时拒绝接入；在线连接由撤销/到期路径主动断开（4403）。
    """
    await websocket.accept()
    async with state_lock:
        share = db.execute(
            "SELECT * FROM observer_shares WHERE token = ?", (token,)
        ).fetchone()
        if share is None or not share_active(share):
            await websocket.send_json({"kind": "error", "message": "观察链接已失效或已被撤销"})
            await websocket.close(code=4403)
            return
        room = db.execute("SELECT * FROM rooms WHERE id = ?", (share["room_id"],)).fetchone()
        scope = observer_scope_from(share)
        hub_key = room["code"]
        hub.add(hub_key, websocket, scope)
        snapshot = build_snapshot(room, scope)
        snapshot["last_event_id"] = max(snapshot["last_event_id"], last_event_id)
        await websocket.send_json(snapshot)
    try:
        while True:
            await websocket.receive_text()  # 观察者只读，无上行；仅用于感知断线
    except WebSocketDisconnect:
        pass
    finally:
        hub.discard(hub_key, websocket)


# ---------------------------------------------------------------- 静态资源

@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
