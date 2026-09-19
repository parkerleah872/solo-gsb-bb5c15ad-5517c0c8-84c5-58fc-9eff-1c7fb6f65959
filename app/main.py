"""情景注入台 —— FastAPI 主程序。

职责：
- 房间 / 参与组 / 注入卡片的 REST 管理
- 可暂停 / 继续的虚拟时钟（暂停期间绝不推进）
- 后台 tick：按 (trigger_seconds, seq) 稳定次序揭示到期卡片
- 卡片回执：参与者对已揭示的本组可见卡片标记/撤销「已收到」，
  同一参与组仅保留一条回执；主持人实时看到每组回执/未回执汇总
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
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

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
OBS_TOKEN_BYTES = 24  # 观察凭证随机熵（urlsafe token，约 32 字符）
OBS_MIN_MINUTES = 5
OBS_MAX_MINUTES = 120
SHARE_SWEEP_INTERVAL = 1.0  # 到期观察席扫描周期（秒）

# 连接视野：
# - None                主持人（全量）
# - int                 参与者（某参与组）
# - ObserverView        限时观察席（全员已揭示卡 + 所选组已揭示卡，只读）
Scope = Optional[object]


@dataclass(frozen=True)
class ObserverView:
    """观察者视野：只可看全员 + 所选组的**已揭示**卡片，且无任何控制/回执能力。"""

    token: str
    group_ids: frozenset[int]

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


def group_public_safe(group: sqlite3.Row) -> dict:
    """观察者视野下的分组信息：只给 id/名称，绝不下发组加入码。"""
    return {"id": group["id"], "name": group["name"]}


def share_public(row: sqlite3.Row) -> dict:
    """观察席分享视图（仅主持人可见）：含状态与所选组 id，绝不返回凭证以外的秘密。"""
    active = not row["revoked"] and row["expires_at"] > time.time()
    return {
        "id": row["id"],
        "token": row["token"],
        "group_ids": json.loads(row["group_ids"]),
        "created_at_ms": int(row["created_at"] * 1000),
        "expires_at_ms": int(row["expires_at"] * 1000),
        "server_now_ms": now_ms(),
        "revoked": bool(row["revoked"]),
        "active": active,
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


def build_snapshot(room: sqlite3.Row, scope: Scope) -> dict:
    """权威全量状态，按视野过滤：
    - 主持人：全部卡片/分组，含房间码与回执汇总，另含观察席分享列表；
    - 参与者：全员卡片与本组卡片（含未揭示条目，回执仅 my_receipt）；
    - 观察者：仅全员与所选组的**已揭示**卡片，无房间码、无加入码、无任何回执字段。
    """
    if isinstance(scope, ObserverView):
        return build_observer_snapshot(room, scope)
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
    snap = {
        "kind": "snapshot",
        "last_event_id": last_id,
        "server_now_ms": now_ms(),
        "scope": {"role": "host" if scope is None else "player", "group_id": scope},
        "room": room_public(room, include_code=scope is None),
        "groups": [group_public(g) for g in groups],
        "cards": snapshot_cards(room, cards, groups, scope),
    }
    if scope is None:
        snap["observer_shares"] = [
            share_public(r)
            for r in db.execute(
                "SELECT * FROM observer_shares WHERE room_id = ? ORDER BY id", (room["id"],)
            )
        ]
    return snap


def build_observer_snapshot(room: sqlite3.Row, view: ObserverView) -> dict:
    """观察者快照：只含已揭示的全员卡 + 所选组卡，不含任何秘密字段与回执。"""
    gids = sorted(view.group_ids)
    # 所选组可能已不存在：按现存组过滤，且绝不透出加入码
    groups = (
        db.execute(f"SELECT * FROM groups WHERE room_id = ? AND id IN ({','.join('?' * len(gids))}) ORDER BY id",
                   (room["id"], *gids)).fetchall()
        if gids else []
    )
    existing = {g["id"] for g in groups}
    if existing:
        placeholders = ",".join("?" * len(existing))
        cards = db.execute(
            f"""SELECT * FROM cards
                WHERE room_id = ? AND revealed = 1
                  AND (group_id IS NULL OR group_id IN ({placeholders}))
                ORDER BY trigger_seconds, seq""",
            (room["id"], *existing),
        ).fetchall()
    else:
        cards = db.execute(
            """SELECT * FROM cards
               WHERE room_id = ? AND revealed = 1 AND group_id IS NULL
               ORDER BY trigger_seconds, seq""",
            (room["id"],),
        ).fetchall()
    share = db.execute(
        "SELECT * FROM observer_shares WHERE token = ?", (view.token,)
    ).fetchone()
    last_id = db.execute(
        "SELECT COALESCE(MAX(id), 0) AS m FROM events WHERE room_id = ?", (room["id"],)
    ).fetchone()["m"]
    return {
        "kind": "snapshot",
        "last_event_id": last_id,
        "server_now_ms": now_ms(),
        "scope": {
            "role": "observer",
            "group_ids": sorted(existing),
            "expires_at_ms": int(share["expires_at"] * 1000) if share else None,
        },
        "room": room_public(room, include_code=False),
        "groups": [group_public_safe(g) for g in groups],
        # card_public 只含卡片公开字段，天然不含任何回执信息
        "cards": [card_public(c) for c in cards],
    }


def snapshot_cards(room: sqlite3.Row, cards: list[sqlite3.Row],
                   groups: list[sqlite3.Row], scope: int | None) -> list[dict]:
    """为主持人/参与者快照中的卡片附加回执视图（按视野分别计算）。观察者不走这里。"""
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

def event_visible(scope: Scope, type_: str, payload: dict) -> bool:
    """事件是否应投递给该视野。

    主持人全收；参与者只收全员/本组卡片事件、本组改名与房间级事件；
    观察者只读，只收命中范围的已揭示相关事件，回执与观察席管理事件一律不下发。
    """
    if scope is None:
        return True
    if isinstance(scope, ObserverView):
        if type_ in ("card_revealed", "card_removed"):
            gid = payload.get("group_id")
            return gid is None or gid in scope.group_ids
        if type_ == "group_renamed":
            return payload.get("id") in scope.group_ids
        if type_ == "group_removed":
            return payload.get("id") in scope.group_ids
        if type_ in ("clock", "reset"):
            return True
        # card_added（未揭示卡）、receipt_updated、group_added、观察席管理等全部拦截
        return False
    if type_ in ("card_added", "card_revealed", "card_removed"):
        gid = payload.get("group_id")
        return gid is None or gid == scope
    if type_ == "receipt_updated":
        return payload.get("group_id") == scope  # 参与者只感知本组的回执变化
    if type_ == "group_renamed":
        return payload.get("id") == scope  # 参与者只需感知本组改名
    if type_ in ("group_added", "group_removed", "observer_share_added", "observer_share_revoked"):
        return False
    return True  # clock / reset 等房间级事件


class Hub:
    """按房间维护连接及其视野（None=主持人，int=参与组，ObserverView=观察席），广播时按视野过滤。"""

    def __init__(self) -> None:
        self._rooms: dict[str, dict[WebSocket, Scope]] = {}

    def add(self, code: str, ws: WebSocket, scope: Scope) -> None:
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
        await self._close_matching(
            code, message,
            lambda s: type(s) is int and s == scope,
        )

    async def close_observer(self, code: str, token: str, message: dict) -> None:
        """观察凭证失效（到期/撤销）时，立即断开使用该凭证的全部连接。"""
        await self._close_matching(
            code, message,
            lambda s: isinstance(s, ObserverView) and s.token == token,
        )

    def has_observer(self, code: str, token: str) -> bool:
        return any(
            isinstance(s, ObserverView) and s.token == token
            for s in self._rooms.get(code, {}).values()
        )

    async def _close_matching(self, code: str, message: dict, match) -> None:
        for ws, ws_scope in list(self._rooms.get(code, {}).items()):
            if not match(ws_scope):
                continue
            try:
                await ws.send_json(message)
                await ws.close(code=4404)
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
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("ticker error")


async def share_expiry_loop() -> None:
    """周期性扫描到期观察席：凭证状态持久化在库中，到期即立即踢断在线观察者。

    刷新/重连时入口接口会再次校验 expires_at；到期后无法靠重连恢复权限。
    """
    while True:
        await asyncio.sleep(SHARE_SWEEP_INTERVAL)
        try:
            async with state_lock:
                due = db.execute(
                    "SELECT * FROM observer_shares WHERE revoked = 0 AND expires_at <= ?",
                    (time.time(),),
                ).fetchall()
                for share in due:
                    room = db.execute(
                        "SELECT * FROM rooms WHERE id = ?", (share["room_id"],)
                    ).fetchone()
                    if room is None or not hub.has_observer(room["code"], share["token"]):
                        continue
                    await hub.close_observer(
                        room["code"], share["token"],
                        {"kind": "error", "message": "观察链接已过期"},
                    )
                    log.info("observer share %s expired and disconnected", share["token"][:8])
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("share expiry sweep error")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db
    db = dbmod.connect()
    dbmod.init_schema(db)  # 启动即完成数据库初始化
    ticker = asyncio.create_task(ticker_loop())
    expiry = asyncio.create_task(share_expiry_loop())
    log.info("inject console ready, db=%s", dbmod.DB_PATH)
    yield
    ticker.cancel()
    expiry.cancel()
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


@app.get("/api/observe/{token}")
async def observer_snapshot(token: str):
    """观察者入口：凭随机观察链接换取只读快照。

    只含已揭示的全员卡与所选组卡；无房间码、组加入码、回执明细与任何控制能力。
    凭证不存在返回 404；已撤销/已过期返回 410，刷新不能恢复。
    """
    async with state_lock:
        share = db.execute(
            "SELECT * FROM observer_shares WHERE token = ?", (token,)
        ).fetchone()
        if share is None:
            raise HTTPException(status_code=404, detail="observer link not found")
        if share["revoked"]:
            raise HTTPException(status_code=410, detail="observer link has been revoked")
        if share["expires_at"] <= time.time():
            raise HTTPException(status_code=410, detail="observer link has expired")
        room = db.execute("SELECT * FROM rooms WHERE id = ?", (share["room_id"],)).fetchone()
        if room is None:
            raise HTTPException(status_code=410, detail="observer link is no longer valid")
        view = ObserverView(
            token=share["token"], group_ids=frozenset(json.loads(share["group_ids"]))
        )
        return build_observer_snapshot(room, view)


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


# ---------------------------------------------------------------- REST：限时观察席

class ObserverShareIn(BaseModel):
    group_ids: list[int] = Field(min_length=1, max_length=3)  # 主持人勾选 1~3 个参与组
    ttl_minutes: int = Field(ge=OBS_MIN_MINUTES, le=OBS_MAX_MINUTES)  # 有效期 5~120 分钟


def _fresh_observer_token() -> str:
    while True:
        token = secrets.token_urlsafe(OBS_TOKEN_BYTES)
        if db.execute("SELECT 1 FROM observer_shares WHERE token = ?", (token,)).fetchone() is None:
            return token


@app.post("/api/rooms/{code}/observer-shares", status_code=201)
async def create_observer_share(code: str, body: ObserverShareIn):
    """主持人生成限时观察链接：随机可撤销凭证，有效期 5~120 分钟。"""
    async with state_lock:
        room = get_room_or_404(code)
        gids = sorted(set(body.group_ids))
        if not (1 <= len(gids) <= 3):
            raise HTTPException(status_code=422, detail="choose 1 to 3 distinct groups")
        placeholders = ",".join("?" * len(gids))
        owned = {
            row["id"]
            for row in db.execute(
                f"SELECT id FROM groups WHERE room_id = ? AND id IN ({placeholders})",
                (room["id"], *gids),
            )
        }
        if len(owned) != len(gids):
            raise HTTPException(status_code=422, detail="group not found in this room")
        now = time.time()
        token = _fresh_observer_token()
        cur = db.execute(
            "INSERT INTO observer_shares (room_id, token, group_ids, created_at, expires_at, revoked)"
            " VALUES (?, ?, ?, ?, ?, 0)",
            (room["id"], token, json.dumps(gids), now, now + body.ttl_minutes * 60),
        )
        db.commit()
        share = db.execute(
            "SELECT * FROM observer_shares WHERE id = ?", (cur.lastrowid,)
        ).fetchone()
        payload = share_public(share)
        # 观察席管理事件只投递给主持人（event_visible 对其他视野一律拦截）
        await publish(room["code"], room["id"], "observer_share_added", payload)
        return payload


@app.get("/api/rooms/{code}/observer-shares")
async def list_observer_shares(code: str):
    """主持人查看全部观察席（有效 / 已过期 / 已撤销），凭证及状态均持久化。"""
    async with state_lock:
        room = get_room_or_404(code)
        rows = db.execute(
            "SELECT * FROM observer_shares WHERE room_id = ? ORDER BY id", (room["id"],)
        ).fetchall()
        return {"shares": [share_public(r) for r in rows]}


@app.delete("/api/rooms/{code}/observer-shares/{share_id}")
async def revoke_observer_share(code: str, share_id: int):
    """主持人撤销观察席：立即踢断现有连接，后续 REST/WS 访问一律失效。"""
    async with state_lock:
        room = get_room_or_404(code)
        share = db.execute(
            "SELECT * FROM observer_shares WHERE id = ? AND room_id = ?",
            (share_id, room["id"]),
        ).fetchone()
        if share is None:
            raise HTTPException(status_code=404, detail="observer share not found")
        if not share["revoked"]:
            db.execute(
                "UPDATE observer_shares SET revoked = 1 WHERE id = ?", (share_id,)
            )
            db.commit()
            await publish(
                room["code"], room["id"], "observer_share_revoked",
                {"id": share_id, "token": share["token"]},
            )
            # 立即断开使用该凭证的全部观察者连接
            await hub.close_observer(
                room["code"], share["token"],
                {"kind": "error", "message": "观察链接已被主持人撤销"},
            )
        return {"ok": True, "id": share_id, "active": False}


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
async def observer_ws(websocket: WebSocket, token: str, last_event_id: int = 0):
    """观察者连接：仅推送已揭示的全员卡 + 所选组卡增量，无任何控制能力。

    凭证不存在 / 已撤销 / 已过期均在握手后立即断开（4404），不注册进 Hub；
    因此到期扫描或主持人撤销踢断后，刷新重连会在这里被再次拒绝，权限不可恢复。
    """
    await websocket.accept()
    async with state_lock:
        share = db.execute(
            "SELECT * FROM observer_shares WHERE token = ?", (token,)
        ).fetchone()
        if share is None:
            await websocket.send_json({"kind": "error", "message": "观察链接无效"})
            await websocket.close(code=4404)
            return
        if share["revoked"]:
            await websocket.send_json({"kind": "error", "message": "观察链接已被主持人撤销"})
            await websocket.close(code=4404)
            return
        if share["expires_at"] <= time.time():
            await websocket.send_json({"kind": "error", "message": "观察链接已过期"})
            await websocket.close(code=4404)
            return
        room = db.execute("SELECT * FROM rooms WHERE id = ?", (share["room_id"],)).fetchone()
        view = ObserverView(
            token=share["token"], group_ids=frozenset(json.loads(share["group_ids"]))
        )
        hub_key = room["code"]
        hub.add(hub_key, websocket, view)
        snapshot = build_observer_snapshot(room, view)
        snapshot["last_event_id"] = max(snapshot["last_event_id"], last_event_id)
        await websocket.send_json(snapshot)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        hub.discard(hub_key, websocket)


# ---------------------------------------------------------------- 静态资源

@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
