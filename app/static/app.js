'use strict';

/* 情景注入台前端
 *
 * 同步协议：
 * 1. 打开 WebSocket 时带上 last_event_id（已应用的最大事件 ID）；
 * 2. 服务端先推权威快照（snapshot）——全量替换本地状态，水位 = snapshot.last_event_id；
 * 3. 之后只应用 id > 水位的增量事件，旧事件一律丢弃，状态绝不回退。
 *
 * 三种角色：
 * - 主持人：房间码连接（全量视野，含观察分享管理）；
 * - 参与者：组加入码连接（服务端按组过滤快照与事件）；
 * - 观察者：限时观察链接（#/observe/{token}）进入，只读——只能看到已揭示的
 *   全员卡片与分享所选组卡片，拿不到房间码、组加入码、未揭示卡片与回执明细；
 *   链接到期或被撤销时服务端主动断开，重连也会被拒绝。
 */

const $ = (sel) => document.querySelector(sel);

const state = {
  code: null,      // 主持人 = 房间码；参与者 = 组加入码；观察者 = 观察凭证
  role: null,      // 'host' | 'player' | 'observer'
  roomName: '',
  status: 'idle',
  cards: [],
  groups: [],      // 主持人：全部参与组；参与者：仅本组；观察者：分享所选组
  shares: [],      // 主持人：全部观察分享（有效/失效）
  groupId: null,   // 参与者本组 id（主持人/观察者为 null）
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
  $('#cards-title').textContent =
    state.role === 'host' ? '注入卡片（全部）'
    : state.role === 'observer' ? '已揭示的情景注入（观察席 · 只读）'
    : '已揭示的情景注入';
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
  // 观察者的 code 是观察凭证而非房间码，不显示「加入码」行
  $('.room-code').classList.toggle('hidden', state.role === 'observer');
  $('#room-code').textContent = state.code;
  const st = $('#status');
  st.textContent = STATUS_TEXT[state.status] || state.status;
  st.className = 'status' + (state.status === 'running' ? ' running' : state.status === 'paused' ? ' paused' : '');
  const pg = $('#player-group');
  if (state.role === 'player' && state.groupId != null) {
    pg.textContent = `参与组：${groupName(state.groupId)}`;
    pg.classList.remove('hidden');
  } else if (state.role === 'observer') {
    const names = state.groups.map((g) => g.name).join('、');
    pg.textContent = `观察席（只读）· 可见组：${names || '全员'}`;
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

  // 观察分享表单的组复选框（1～3 个）；尽量保留已勾选项
  const checked = new Set(
    [...document.querySelectorAll('#share-groups input:checked')].map((el) => el.value)
  );
  const sg = $('#share-groups');
  sg.innerHTML = '';
  for (const g of state.groups) {
    const label = document.createElement('label');
    label.className = 'share-group-chip';
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.value = String(g.id);
    cb.checked = checked.has(String(g.id));
    label.appendChild(cb);
    label.appendChild(document.createTextNode(g.name));
    sg.appendChild(label);
  }
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
  if (state.role === 'host') {
    $('#cards-empty').textContent = '暂无卡片，请在上方添加';
  } else {
    $('#cards-empty').textContent = all.some((c) => c.revealed) ? '' : '等待主持人注入情景…';
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

/* ---------------------------------------------------------- 观察席分享 */

const SHARE_STATUS_TEXT = { active: '有效', expired: '已过期', revoked: '已撤销' };

function shareStatus(s) {
  if (s.status === 'revoked') return 'revoked';
  return s.expires_at * 1000 > Date.now() ? 'active' : 'expired';
}

function shareUrl(s) {
  return `${location.origin}/#/observe/${s.token}`;
}

function upsertShare(s) {
  const i = state.shares.findIndex((x) => x.token === s.token);
  if (i >= 0) state.shares[i] = s;
  else state.shares.unshift(s); // 服务端按 id DESC 下发，新分享排最前
}

/* 主持人：分享列表（有效/失效），可复制链接、可撤销 */
function renderShares() {
  if (state.role !== 'host') return;
  const list = $('#share-list');
  list.innerHTML = '';
  $('#shares-empty').classList.toggle('hidden', state.shares.length > 0);
  for (const s of state.shares) {
    const st = shareStatus(s);
    const li = document.createElement('li');
    li.className = 'share-item';

    const main = document.createElement('div');
    main.className = 'share-main';

    const status = document.createElement('span');
    status.className = `badge share-status-${st}`;
    status.textContent = SHARE_STATUS_TEXT[st];
    main.appendChild(status);

    const groups = document.createElement('span');
    groups.className = 'share-groups-label';
    groups.textContent = (s.group_names || []).join('、');
    main.appendChild(groups);

    const mins = document.createElement('span');
    mins.className = 'share-remaining';
    mins.textContent = `${s.minutes} 分钟`;
    main.appendChild(mins);

    if (st === 'active') {
      const remain = document.createElement('span');
      remain.className = 'share-remaining';
      remain.dataset.expiresAt = s.expires_at;
      remain.dataset.token = s.token;
      remain.textContent = remainingText(s.expires_at);
      main.appendChild(remain);
    }
    li.appendChild(main);

    const ops = document.createElement('span');
    ops.className = 'share-ops';

    const copy = document.createElement('button');
    copy.className = 'group-op';
    copy.textContent = '复制链接';
    copy.onclick = () => {
      navigator.clipboard?.writeText(shareUrl(s));
      copy.textContent = '已复制';
      setTimeout(() => { copy.textContent = '复制链接'; }, 1200);
    };
    ops.appendChild(copy);

    if (st === 'active') {
      const revoke = document.createElement('button');
      revoke.className = 'group-op group-del';
      revoke.textContent = '撤销';
      revoke.onclick = async () => {
        if (!confirm('撤销该观察链接？在线观察者将立即断线，链接此后永久失效。')) return;
        try {
          await api('DELETE', `/api/rooms/${state.code}/shares/${s.token}`);
        } catch (err) { showError(err); }
      };
      ops.appendChild(revoke);
    }
    li.appendChild(ops);
    list.appendChild(li);
  }
}

function remainingText(expiresAt) {
  const mins = Math.ceil((expiresAt * 1000 - Date.now()) / 60000);
  return mins > 1 ? `剩余 ${mins} 分钟` : '剩余不足 1 分钟';
}

/* 倒计时文本就地更新（不重建列表，避免打断点击）；到期翻转时整体重渲染 */
function updateShareCountdowns() {
  let flip = false;
  for (const el of document.querySelectorAll('.share-remaining[data-expires-at]')) {
    const s = state.shares.find((x) => x.token === el.dataset.token);
    if (!s || s.status !== 'active') continue;
    const exp = parseFloat(el.dataset.expiresAt);
    if (exp * 1000 <= Date.now()) {
      s.status = 'expired';
      flip = true;
    } else {
      el.textContent = remainingText(exp);
    }
  }
  if (flip) renderShares();
}

function renderAll() {
  renderMeta();
  renderGroups();
  renderShares();
  renderCards();
}

/* 本地时钟渲染：以快照/事件同步点为基准，running 时本地外推，暂停即冻结 */
setInterval(() => {
  if (!state.code) return;
  const c = state.clock;
  const elapsed = c.status === 'running' ? c.elapsedMs + (performance.now() - c.syncedAt) : c.elapsedMs;
  $('#clock').textContent = fmt(elapsed);
  if (state.role === 'host') updateShareCountdowns();
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
  state.shares = msg.shares || []; // 仅主持人快照携带
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
    case 'share_created':
      upsertShare(p); // 仅主持人会收到（事件含观察凭证，服务端已过滤）
      break;
    case 'share_revoked':
    case 'share_expired': {
      const s = state.shares.find((x) => x.token === p.token);
      if (s) s.status = msg.type === 'share_revoked' ? 'revoked' : 'expired';
      break;
    }
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

/* 事件流里新增的卡片不带回执字段（新卡/新揭示必然零回执），按角色补默认值；
   观察者只读，不附加任何回执状态 */
function initReceiptState(card) {
  if (state.role === 'host') {
    card.receipts = {
      expected: card.group_id == null ? state.groups.map((g) => g.id) : [card.group_id],
      acked: [],
    };
  } else if (state.role === 'player') {
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
  const path = state.role === 'observer' ? `/ws/observe/${state.code}` : `/ws/${state.code}`;
  const ws = new WebSocket(`${proto}://${location.host}${path}?last_event_id=${state.lastEventId}`);
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
  state.shares = [];
  state.groupId = null;
  location.hash = `#/${role}/${code}`;
  localStorage.setItem(`inject-role-${code}`, role);
  showRoom();
  renderAll();
  connect();
}

/* 观察者：凭限时观察链接进入（只读视野，凭证失效后服务端拒绝重连） */
function enterObserver(token) {
  state.code = token;
  state.role = 'observer';
  state.lastEventId = 0;
  state.cards = [];
  state.groups = [];
  state.shares = [];
  state.groupId = null;
  location.hash = `#/observe/${token}`;
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

$('#share-form').onsubmit = async (e) => {
  e.preventDefault();
  const ids = [...document.querySelectorAll('#share-groups input:checked')]
    .map((el) => parseInt(el.value, 10));
  const minutes = parseInt($('#share-minutes').value, 10);
  if (ids.length < 1 || ids.length > 3) {
    showError(new Error('请选择 1～3 个参与组'));
    return;
  }
  if (!Number.isInteger(minutes) || minutes < 5 || minutes > 120) {
    showError(new Error('有效期须为 5～120 分钟'));
    return;
  }
  try {
    const share = await api('POST', `/api/rooms/${state.code}/shares`,
      { group_ids: ids, minutes });
    upsertShare(share); // share_created 事件到达时按 token 去重
    renderShares();
    document.querySelectorAll('#share-groups input:checked').forEach((el) => { el.checked = false; });
    $('#share-minutes').value = 30;
  } catch (err) { showError(err); }
};

/* 刷新恢复：根据 hash 与本地角色记录重进房间（走完整的快照+增量协议）；
   观察链接同样从 hash 恢复——凭证已失效时服务端会拒绝并提示 */
(function boot() {
  const m = location.hash.match(/^#\/(host|player)\/([A-Z0-9]{6})$/);
  if (m) { enterRoom(m[2], m[1]); return; }
  const mo = location.hash.match(/^#\/observe\/([A-Za-z0-9_-]+)$/);
  if (mo) { enterObserver(mo[1]); return; }
  showLanding();
})();
