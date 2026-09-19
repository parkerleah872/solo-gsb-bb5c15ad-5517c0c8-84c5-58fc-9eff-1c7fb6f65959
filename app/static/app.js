'use strict';

/* 情景注入台前端
 *
 * 同步协议：
 * 1. 打开 WebSocket 时带上 last_event_id（已应用的最大事件 ID）；
 * 2. 服务端先推权威快照（snapshot）——全量替换本地状态，水位 = snapshot.last_event_id；
 * 3. 之后只应用 id > 水位的增量事件，旧事件一律丢弃，状态绝不回退。
 *
 * 分组定向：主持人用房间码连接（全量视野），参与者用组加入码连接；
 * 服务端已按组过滤快照与事件，未命中本组的卡片不会到达本端。
 */

const $ = (sel) => document.querySelector(sel);

const state = {
  code: null,      // 主持人 = 房间码；参与者 = 组加入码
  role: null,      // 'host' | 'player'
  roomName: '',
  status: 'idle',
  cards: [],
  groups: [],      // 主持人：全部参与组；参与者：仅本组
  groupId: null,   // 参与者本组 id（主持人为 null）
  lastEventId: 0,
  clock: { status: 'idle', elapsedMs: 0, syncedAt: performance.now() },
  ws: null,
  reconnectDelay: 1000,
};

const STATUS_TEXT = { idle: '待机', running: '进行中', paused: '已暂停' };

/* ---------------------------------------------------------- 视图切换 */

function showLanding() {
  $('#view-landing').classList.remove('hidden');
  $('#view-room').classList.add('hidden');
}

function showRoom() {
  $('#view-landing').classList.add('hidden');
  $('#view-room').classList.remove('hidden');
  $('#host-console').classList.toggle('hidden', state.role !== 'host');
  $('#cards-title').textContent = state.role === 'host' ? '注入卡片（全部）' : '已揭示的情景注入';
}

/* ---------------------------------------------------------- 渲染 */

function fmt(ms) {
  const s = Math.max(0, Math.floor(ms / 1000));
  const h = String(Math.floor(s / 3600)).padStart(2, '0');
  const m = String(Math.floor((s % 3600) / 60)).padStart(2, '0');
  const sec = String(s % 60).padStart(2, '0');
  return `${h}:${m}:${sec}`;
}

function groupName(id) {
  const g = state.groups.find((g) => g.id === id);
  return g ? g.name : `#${id}`;
}

function renderMeta() {
  $('#room-name').textContent = state.roomName || '未命名演练';
  $('#room-code').textContent = state.code;
  const st = $('#status');
  st.textContent = STATUS_TEXT[state.status] || state.status;
  st.className = 'status' + (state.status === 'running' ? ' running' : state.status === 'paused' ? ' paused' : '');
  const pg = $('#player-group');
  if (state.role === 'player' && state.groupId != null) {
    pg.textContent = `参与组：${groupName(state.groupId)}`;
    pg.classList.remove('hidden');
  } else {
    pg.classList.add('hidden');
  }
  renderControls();
}

function renderControls() {
  if (state.role !== 'host') return;
  const s = state.status;
  $('#btn-start').disabled = s !== 'idle';
  $('#btn-pause').disabled = s !== 'running';
  $('#btn-resume').disabled = s !== 'paused';
  $('#btn-reset').disabled = s === 'idle';
  const editing = s !== 'running';
  $('#card-form').querySelectorAll('input, textarea, select, button').forEach((el) => { el.disabled = !editing; });
  $('#card-form-hint').textContent = editing ? '' : '时钟运行中，暂停后才能编辑卡片';
}

/* 主持人：参与组列表 + 卡片目标下拉 */
function renderGroups() {
  if (state.role !== 'host') return;
  $('#group-count').textContent = state.groups.length;
  const list = $('#group-list');
  list.innerHTML = '';
  for (const g of state.groups) {
    const li = document.createElement('li');
    li.className = 'group-item';

    const name = document.createElement('span');
    name.className = 'group-name';
    name.textContent = g.name;
    li.appendChild(name);

    const code = document.createElement('button');
    code.className = 'code-chip group-code';
    code.textContent = g.join_code;
    code.title = '点击复制加入码';
    code.onclick = () => {
      navigator.clipboard?.writeText(g.join_code);
      code.textContent = '已复制';
      setTimeout(() => { code.textContent = g.join_code; }, 1200);
    };
    li.appendChild(code);

    const rename = document.createElement('button');
    rename.className = 'group-op';
    rename.textContent = '改名';
    rename.onclick = async () => {
      const name2 = (prompt('新的组名称', g.name) || '').trim();
      if (!name2 || name2 === g.name) return;
      try {
        await api('PATCH', `/api/rooms/${state.code}/groups/${g.id}`, { name: name2 });
      } catch (err) { showError(err); }
    };
    li.appendChild(rename);

    const del = document.createElement('button');
    del.className = 'group-op group-del';
    del.textContent = '删除';
    del.onclick = async () => {
      if (!confirm(`删除参与组「${g.name}」？其加入码将立即失效。`)) return;
      try {
        await api('DELETE', `/api/rooms/${state.code}/groups/${g.id}`);
      } catch (err) { showError(err); }
    };
    li.appendChild(del);

    list.appendChild(li);
  }

  // 卡片目标下拉：全员 + 各参与组；尽量保留当前选择
  const sel = $('#card-target');
  const cur = sel.value;
  sel.innerHTML = '';
  const all = document.createElement('option');
  all.value = '';
  all.textContent = '全员';
  sel.appendChild(all);
  for (const g of state.groups) {
    const opt = document.createElement('option');
    opt.value = String(g.id);
    opt.textContent = `仅：${g.name}`;
    sel.appendChild(opt);
  }
  sel.value = [...sel.options].some((o) => o.value === cur) ? cur : '';
}

function sortedCards() {
  return [...state.cards].sort((a, b) => a.trigger_seconds - b.trigger_seconds || a.seq - b.seq);
}

function renderCards() {
  const list = $('#card-list');
  list.innerHTML = '';
  const all = sortedCards();
  const visible = state.role === 'host' ? all : all.filter((c) => c.revealed);
  $('#cards-empty').classList.toggle('hidden', visible.length > 0);
  if (state.role === 'player') {
    $('#cards-empty').textContent = all.some((c) => c.revealed) ? '' : '等待主持人注入情景…';
  } else {
    $('#cards-empty').textContent = '暂无卡片，请在上方添加';
  }
  for (const card of visible) {
    const li = document.createElement('li');
    li.className = 'card-item ' + (card.revealed ? 'revealed' : 'pending');

    const meta = document.createElement('div');
    meta.className = 'card-meta';
    const left = document.createElement('span');
    const trig = document.createElement('span');
    trig.className = 'card-trigger';
    trig.textContent = `T+${fmt(card.trigger_seconds * 1000)}`;
    left.appendChild(trig);
    left.appendChild(document.createTextNode(` · #${card.seq} `));
    const target = document.createElement('span');
    target.className = 'badge ' + (card.group_id == null ? 'badge-all' : 'badge-target');
    target.textContent = card.group_id == null ? '全员' : `仅 ${groupName(card.group_id)}`;
    left.appendChild(target);
    meta.appendChild(left);

    const right = document.createElement('span');
    if (card.revealed) {
      right.innerHTML = `<span class="badge badge-revealed">已揭示 ${fmt(card.revealed_at_ms || 0)}</span>`;
    } else {
      right.innerHTML = '<span class="badge badge-pending">待触发</span>';
    }
    if (state.role === 'player' && card.revealed) {
      right.appendChild(receiptButton(card));
    }
    if (state.role === 'host' && state.status !== 'running') {
      const del = document.createElement('button');
      del.className = 'card-del';
      del.textContent = '删除';
      del.onclick = () => api('DELETE', `/api/rooms/${state.code}/cards/${card.id}`).catch(showError);
      right.appendChild(del);
    }
    meta.appendChild(right);

    const title = document.createElement('div');
    title.className = 'card-title';
    title.textContent = card.title;

    li.appendChild(meta);
    li.appendChild(title);
    if (card.content) {
      const content = document.createElement('div');
      content.className = 'card-content';
      content.textContent = card.content;
      li.appendChild(content);
    }
    if (state.role === 'host' && card.revealed) {
      const summary = receiptSummary(card);
      if (summary) li.appendChild(summary);
    }
    list.appendChild(li);
  }
}

/* 参与者：已揭示卡片上的「已收到」开关（可撤销） */
function receiptButton(card) {
  const btn = document.createElement('button');
  btn.className = 'receipt-btn' + (card.my_receipt ? ' acked' : '');
  btn.textContent = card.my_receipt ? '✓ 已收到（点击撤销）' : '标记已收到';
  btn.onclick = async () => {
    btn.disabled = true;
    try {
      const res = await api(card.my_receipt ? 'DELETE' : 'POST',
        `/api/join/${state.code}/cards/${card.id}/receipt`);
      card.my_receipt = res.acked; // 同组其他连接会通过 receipt_updated 事件同步
      renderCards();
    } catch (err) {
      showError(err);
      btn.disabled = false;
    }
  };
  return btn;
}

/* 主持人：已揭示卡片的每组回执/未回执汇总（全员卡统计所有组，定向卡只统计目标组） */
function receiptSummary(card) {
  const r = card.receipts;
  if (!r || r.expected.length === 0) return null; // 房间内还没有参与组
  const box = document.createElement('div');
  box.className = 'receipt-summary';

  const count = document.createElement('span');
  const done = r.acked.length === r.expected.length;
  count.className = 'receipt-count' + (done ? ' done' : '');
  count.textContent = `回执 ${r.acked.length}/${r.expected.length}`;
  box.appendChild(count);

  for (const gid of r.expected) {
    const got = r.acked.includes(gid);
    const chip = document.createElement('span');
    chip.className = 'receipt-chip ' + (got ? 'receipt-ok' : 'receipt-wait');
    chip.textContent = (got ? '✓ ' : '未回执 · ') + groupName(gid);
    box.appendChild(chip);
  }
  return box;
}

function renderAll() {
  renderMeta();
  renderGroups();
  renderCards();
}

/* 本地时钟渲染：以快照/事件同步点为基准，running 时本地外推，暂停即冻结 */
setInterval(() => {
  if (!state.code) return;
  const c = state.clock;
  const elapsed = c.status === 'running' ? c.elapsedMs + (performance.now() - c.syncedAt) : c.elapsedMs;
  $('#clock').textContent = fmt(elapsed);
}, 200);

function syncClock(status, elapsedMs) {
  state.status = status;
  state.clock = { status, elapsedMs, syncedAt: performance.now() };
}

/* ---------------------------------------------------------- 状态应用 */

function applySnapshot(msg) {
  state.roomName = msg.room.name;
  state.cards = msg.cards;
  state.groups = msg.groups || [];
  state.groupId = msg.scope && msg.scope.role === 'player' ? msg.scope.group_id : null;
  state.lastEventId = msg.last_event_id; // 权威水位
  syncClock(msg.room.status, msg.room.elapsed_ms);
  renderAll();
}

function applyEvent(msg) {
  if (msg.id <= state.lastEventId) return; // 不回退：丢弃旧事件
  state.lastEventId = msg.id;
  const p = msg.payload;
  switch (msg.type) {
    case 'card_added':
      if (!state.cards.some((c) => c.id === p.id)) {
        initReceiptState(p); // 新卡必然零回执（未揭示不可回执）
        state.cards.push(p);
      }
      break;
    case 'card_removed':
      state.cards = state.cards.filter((c) => c.id !== p.id);
      break;
    case 'card_revealed': {
      const card = state.cards.find((c) => c.id === p.id);
      if (card) Object.assign(card, p);
      else { initReceiptState(p); state.cards.push(p); }
      break;
    }
    case 'receipt_updated': {
      // 服务端已按视野过滤：主持人收全部，参与者只收本组
      const card = state.cards.find((c) => c.id === p.card_id);
      if (!card) break;
      if (state.role === 'host' && card.receipts) {
        const i = card.receipts.acked.indexOf(p.group_id);
        if (p.acked && i < 0) card.receipts.acked.push(p.group_id);
        if (!p.acked && i >= 0) card.receipts.acked.splice(i, 1);
      } else if (state.role === 'player' && p.group_id === state.groupId) {
        card.my_receipt = p.acked;
      }
      break;
    }
    case 'group_added':
      if (!state.groups.some((g) => g.id === p.id)) state.groups.push(p);
      // 新组自动成为所有全员卡片的待回执方
      state.cards.forEach((c) => {
        if (c.group_id == null && c.receipts && !c.receipts.expected.includes(p.id)) {
          c.receipts.expected.push(p.id);
        }
      });
      break;
    case 'group_renamed': {
      const g = state.groups.find((g) => g.id === p.id);
      if (g) g.name = p.name;
      break;
    }
    case 'group_removed':
      state.groups = state.groups.filter((g) => g.id !== p.id);
      state.cards.forEach((c) => {
        if (c.receipts) {
          c.receipts.expected = c.receipts.expected.filter((id) => id !== p.id);
          c.receipts.acked = c.receipts.acked.filter((id) => id !== p.id);
        }
      });
      break;
    case 'clock':
      syncClock(p.status, p.elapsed_ms);
      break;
    case 'reset':
      state.cards.forEach((c) => {
        c.revealed = false;
        c.revealed_at_ms = null;
        if (c.receipts) c.receipts.acked = []; // 回执随揭示一并收回
        if ('my_receipt' in c) c.my_receipt = false;
      });
      syncClock('idle', 0);
      break;
  }
  renderAll();
}

/* 事件流里新增的卡片不带回执字段（新卡/新揭示必然零回执），按角色补默认值 */
function initReceiptState(card) {
  if (state.role === 'host') {
    card.receipts = {
      expected: card.group_id == null ? state.groups.map((g) => g.id) : [card.group_id],
      acked: [],
    };
  } else {
    card.my_receipt = false;
  }
}

/* ---------------------------------------------------------- WebSocket */

function setConn(up) {
  const el = $('#conn');
  el.textContent = up ? '已连接' : '连接断开，重连中…';
  el.className = 'conn ' + (up ? 'conn-up' : 'conn-down');
}

function connect() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const ws = new WebSocket(`${proto}://${location.host}/ws/${state.code}?last_event_id=${state.lastEventId}`);
  state.ws = ws;

  ws.onopen = () => { state.reconnectDelay = 1000; setConn(true); };
  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    if (msg.kind === 'snapshot') applySnapshot(msg);
    else if (msg.kind === 'event') applyEvent(msg);
    else if (msg.kind === 'error') { alert(msg.message); leaveRoom(); }
  };
  ws.onclose = () => {
    setConn(false);
    if (!state.code) return;
    setTimeout(connect, state.reconnectDelay);
    state.reconnectDelay = Math.min(state.reconnectDelay * 2, 5000);
  };
}

/* ---------------------------------------------------------- API */

async function api(method, path, body) {
  const res = await fetch(path, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : {},
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    const detail = await res.json().catch(() => ({}));
    throw new Error(detail.detail || `请求失败 (${res.status})`);
  }
  return res.json();
}

function showError(err) {
  for (const sel of ['#landing-error', '#room-error']) {
    const el = $(sel);
    el.textContent = err.message || String(err);
    setTimeout(() => { el.textContent = ''; }, 4000);
  }
}

/* ---------------------------------------------------------- 流程 */

function enterRoom(code, role) {
  state.code = code;
  state.role = role;
  state.lastEventId = 0;
  state.cards = [];
  state.groups = [];
  state.groupId = null;
  location.hash = `#/${role}/${code}`;
  localStorage.setItem(`inject-role-${code}`, role);
  showRoom();
  renderAll();
  connect();
}

function leaveRoom() {
  if (state.ws) { state.ws.onclose = null; state.ws.close(); }
  state.code = null;
  location.hash = '';
  showLanding();
}

$('#btn-create').onclick = async () => {
  try {
    const name = $('#create-name').value.trim();
    const room = await api('POST', '/api/rooms', { name });
    enterRoom(room.code, 'host');
  } catch (err) { showError(err); }
};

$('#btn-join').onclick = async () => {
  const code = $('#join-code').value.trim().toUpperCase();
  if (!code) return;
  try {
    await api('GET', `/api/join/${code}`);
    enterRoom(code, 'player');
  } catch (err) { showError(new Error('加入码无效，请向主持人确认组加入码')); }
};

$('#room-code').onclick = () => {
  navigator.clipboard?.writeText(state.code);
  $('#room-code').textContent = '已复制';
  setTimeout(() => { $('#room-code').textContent = state.code; }, 1200);
};

$('#btn-start').onclick  = () => api('POST', `/api/rooms/${state.code}/start`).catch(showError);
$('#btn-pause').onclick  = () => api('POST', `/api/rooms/${state.code}/pause`).catch(showError);
$('#btn-resume').onclick = () => api('POST', `/api/rooms/${state.code}/resume`).catch(showError);
$('#btn-reset').onclick  = () => api('POST', `/api/rooms/${state.code}/reset`).catch(showError);

$('#group-form').onsubmit = async (e) => {
  e.preventDefault();
  const name = $('#group-name').value.trim();
  if (!name) return;
  try {
    await api('POST', `/api/rooms/${state.code}/groups`, { name });
    $('#group-name').value = '';
    $('#group-name').focus();
  } catch (err) { showError(err); }
};

$('#card-form').onsubmit = async (e) => {
  e.preventDefault();
  const body = {
    title: $('#card-title').value.trim(),
    content: $('#card-content').value.trim(),
    trigger_seconds: parseInt($('#card-trigger').value, 10) || 0,
    group_id: $('#card-target').value ? parseInt($('#card-target').value, 10) : null,
  };
  try {
    await api('POST', `/api/rooms/${state.code}/cards`, body);
    $('#card-title').value = '';
    $('#card-content').value = '';
    $('#card-trigger').value = '';
    $('#card-title').focus();
  } catch (err) { showError(err); }
};

/* 刷新恢复：根据 hash 与本地角色记录重进房间（走完整的快照+增量协议） */
(function boot() {
  const m = location.hash.match(/^#\/(host|player)\/([A-Z0-9]{6})$/);
  if (m) enterRoom(m[2], m[1]);
  else showLanding();
})();
