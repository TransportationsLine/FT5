'use strict';
/**
 * FootBall — خادم Node/WebSocket
 * - الحسابات تبقى في Google Sheet عبر Apps Script (نفس API_URL)
 * - هذا الخادم: مطابقة اللاعبين، إدارة حالات (بحث / مشغول)، ترحيل اللعب المباشر، احتساب النتيجة
 * نظام اللعب: المضيف (الفريق الأحمر) يشغّل المحاكاة ويرسل لقطات، والضيف (الأخضر) يرسل مدخلاته.
 *
 * تشغيل:  npm install && API_URL="https://script.google.com/macros/s/.../exec" node server.js
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = +process.env.PORT || 3000;
const API_URL = process.env.API_URL || '';
const ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);

const LEVEL_STEP_MS = 8000;        // كل 8 ثوانٍ انتظار يتسع فرق المستوى المسموح بمقدار 1
const SEARCH_TIMEOUT_MS = 60000;   // أقصى مدة بحث
const HOST_SILENCE_MS = 15000;     // انقطاع لقطات المضيف أكثر من هذا = انسحاب
const MATCH_MIN_MS = 150000;       // لا تُقبل نهاية مباراة قبل هذه المدة
const MATCH_MAX_MS = 20 * 60000;   // سقف أمان
const MAX_GOALS = 30;

const hash = t => crypto.createHash('sha256').update(t).digest('hex');
const send = (ws, o) => { if (ws && ws.readyState === WebSocket.OPEN) ws.send(typeof o === 'string' ? o : JSON.stringify(o)); };

/** استدعاء Apps Script بتوكن اللاعب */
async function api(action, token, data = {}) {
  if (!API_URL) return { ok: false, error: 'no_api' };
  try {
    const r = await fetch(API_URL, {
      method: 'POST', redirect: 'follow', signal: AbortSignal.timeout(25000),
      body: JSON.stringify({ action, token, ...data })
    });
    return await r.json();
  } catch (_) { return { ok: false, error: 'network' }; }
}

/* ---------- HTTP: يقدّم ملفات اللعبة (PWA) ويصلح لفحص الصحة ----------
   قائمة بيضاء ثابتة: لا يُقدَّم أي ملف غير هذه (لا يمكن الوصول إلى server.js أو غيره) */
const STATIC = {
  '/':              ['index.html',    'text/html; charset=utf-8',          'no-cache'],
  '/index.html':    ['index.html',    'text/html; charset=utf-8',          'no-cache'],
  '/sw.js':         ['sw.js',         'text/javascript; charset=utf-8',    'no-cache'],
  '/manifest.json': ['manifest.json', 'application/manifest+json',         'no-cache'],
  '/icon-192.png':  ['icon-192.png',  'image/png',                         'public, max-age=86400'],
  '/icon-512.png':  ['icon-512.png',  'image/png',                         'public, max-age=86400']
};
const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/health') { res.writeHead(200); return res.end('ok'); }
  const f = STATIC[url];
  if (!f) { res.writeHead(404); return res.end(); }
  fs.readFile(path.join(__dirname, f[0]), (err, buf) => {
    if (err) { res.writeHead(404); return res.end(f[0] + ' not found'); }
    res.writeHead(200, { 'content-type': f[1], 'cache-control': f[2], 'x-content-type-options': 'nosniff' });
    res.end(buf);
  });
});

const wss = new WebSocketServer({
  server, maxPayload: 16 * 1024,
  verifyClient: ({ origin }) => !ORIGINS.length || ORIGINS.includes(origin)
});

const queue = new Set();      // اللاعبون في حالة بحث
const matches = new Map();    // المباريات الجارية
const busy = new Set();       // هاشات التوكن الموجودة في بحث أو مباراة (حساب واحد = اتصال واحد)

/* ---------- البحث ---------- */
function leaveQueue(ws) {
  if (!queue.delete(ws)) return;
  if (ws.u) { busy.delete(ws.u.th); api('state', ws.u.token, { state: 'active' }); }
  ws.state = ws.readyState === WebSocket.OPEN ? 'idle' : 'gone';
}

/** كل ثانية: يزاوج الباحثين بأقرب مستوى، ويتسع المدى المسموح مع طول الانتظار */
function tick() {
  const now = Date.now(), list = [...queue].sort((a, b) => a.since - b.since), used = new Set();
  for (const a of list) {
    if (used.has(a)) continue;
    let best = null, bd = Infinity;
    for (const b of list) {
      if (b === a || used.has(b)) continue;
      const dl = Math.abs(a.u.level - b.u.level);
      const allowed = Math.max(Math.floor((now - a.since) / LEVEL_STEP_MS), Math.floor((now - b.since) / LEVEL_STEP_MS));
      if (dl <= allowed && dl < bd) { bd = dl; best = b; }
    }
    if (best) { used.add(a); used.add(best); startMatch(a, best); }
    else if (now - a.since > SEARCH_TIMEOUT_MS) { used.add(a); send(a, { t: 'nomatch' }); leaveQueue(a); }
  }
}
setInterval(tick, 1000);

function startMatch(x, y) {
  const [h, g] = Math.random() < .5 ? [x, y] : [y, x];
  queue.delete(h); queue.delete(g);
  const id = Date.now().toString(36) + '-' + crypto.randomBytes(4).toString('hex');
  const mt = { id, h, g, score: { red: 0, green: 0 }, t0: Date.now(), lastSnap: Date.now(), lastGoal: 0, done: false };
  matches.set(id, mt);
  for (const w of [h, g]) { w.state = 'match'; w.match = mt; api('state', w.u.token, { state: 'busy' }); }
  send(h, { t: 'start', role: 'host', matchId: id, opp: { name: g.u.name, level: g.u.level } });
  send(g, { t: 'start', role: 'guest', matchId: id, opp: { name: h.u.name, level: h.u.level } });
}

/* ---------- إنهاء المباراة ---------- */
function release(w) { busy.delete(w.u.th); w.match = null; w.state = w.readyState === WebSocket.OPEN ? 'idle' : 'gone'; }

/** forfeitBy: الاتصال الذي انسحب/انقطع (إن وُجد) */
async function finalize(mt, forfeitBy = null) {
  if (mt.done) return;
  mt.done = true; matches.delete(mt.id);
  let { red, green } = mt.score;
  if (forfeitBy) {                                   // الباقي يفوز ولو كان متأخراً
    if (forfeitBy === mt.g) { if (red <= green) red = Math.min(MAX_GOALS, green + 1); }
    else { if (green <= red) green = Math.min(MAX_GOALS, red + 1); }
  }
  const rep = async (w, gf, ga) => {
    const r = await api('report', w.u.token, { type: 'official', goalsFor: gf, goalsAgainst: ga, matchId: mt.id });
    send(w, {
      t: 'result', score: { red, green }, profile: r.ok ? r.profile : null, leveledUp: !!r.leveledUp,
      fail: !r.ok, forfeit: !!forfeitBy, won: gf > ga
    });
    release(w);
  };
  await Promise.all([rep(mt.h, red, green), rep(mt.g, green, red)]);
}

/** حراسة: مضيف صامت أو مباراة طويلة جداً */
setInterval(() => {
  const now = Date.now();
  for (const mt of [...matches.values()]) {
    if (now - mt.lastSnap > HOST_SILENCE_MS || now - mt.t0 > MATCH_MAX_MS) {
      send(mt.g, { t: 'opp_left' });
      finalize(mt, mt.h);
    }
  }
}, 5000);

/* ---------- الاتصالات ---------- */
wss.on('connection', ws => {
  ws.state = 'idle'; ws.cnt = 0; ws.isAlive = true; ws.u = null; ws.match = null;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('error', () => {});

  ws.on('close', () => {
    if (ws.state === 'search') return leaveQueue(ws);
    const mt = ws.match;
    if (ws.state === 'match' && mt && !mt.done) {
      send(ws === mt.h ? mt.g : mt.h, { t: 'opp_left' });
      finalize(mt, ws);                              // الانسحاب = خسارة
    }
    ws.state = 'gone';
  });

  ws.on('message', async (raw, isBinary) => {
    if (isBinary || ++ws.cnt > 200) { if (ws.cnt > 200) ws.close(1008, 'rate'); return; }
    const s = raw.toString();
    let m; try { m = JSON.parse(s); } catch (_) { return; }
    if (!m || typeof m.t !== 'string') return;
    const mt = ws.match;

    switch (m.t) {
      case 'find': {
        if (ws.state !== 'idle') return;
        if (!/^[0-9a-f]{64}$/.test(String(m.token || ''))) return send(ws, { t: 'err', e: 'bad_token' });
        ws.state = 'verifying';
        const r = await api('ping', m.token);        // يتحقق من التوكن ويرجع الملف الشخصي (ويرفض المحظور)
        if (ws.readyState !== WebSocket.OPEN) { ws.state = 'gone'; return; }
        if (!r.ok) { ws.state = 'idle'; return send(ws, { t: 'err', e: r.error || 'server' }); }
        const th = hash(m.token);
        if (busy.has(th)) { ws.state = 'idle'; return send(ws, { t: 'err', e: 'already' }); }
        busy.add(th);
        ws.u = { token: m.token, th, name: String(r.profile.name || 'لاعب').slice(0, 20), level: +r.profile.level || 1 };
        ws.state = 'search'; ws.since = Date.now(); queue.add(ws);
        api('state', m.token, { state: 'search' });
        return send(ws, { t: 'queued' });
      }
      case 'cancel':
        if (ws.state === 'search') leaveQueue(ws);
        return;

      /* المضيف -> الضيف */
      case 'snap':
        if (ws.state === 'match' && mt && !mt.done && ws === mt.h && s.length < 8192) { mt.lastSnap = Date.now(); send(mt.g, s); }
        return;
      /* الضيف -> المضيف */
      case 'in': case 'act':
        if (ws.state === 'match' && mt && !mt.done && ws === mt.g && s.length < 512) send(mt.h, s);
        return;

      /* المضيف يبلّغ عن الأهداف، والخادم هو من يحسب النتيجة */
      case 'goal': {
        if (ws.state !== 'match' || !mt || mt.done || ws !== mt.h) return;
        const now = Date.now(); if (now - mt.lastGoal < 3000) return;
        mt.lastGoal = now;
        if (m.team === 'red' && mt.score.red < MAX_GOALS) mt.score.red++;
        else if (m.team === 'green' && mt.score.green < MAX_GOALS) mt.score.green++;
        return;
      }
      case 'end':
        if (ws.state !== 'match' || !mt || mt.done || ws !== mt.h) return;
        if (Date.now() - mt.t0 < MATCH_MIN_MS) return;
        return void finalize(mt);
    }
  });
});

setInterval(() => {
  for (const c of wss.clients) {
    c.cnt = 0;
  }
}, 1000);
setInterval(() => {
  for (const c of wss.clients) {
    if (!c.isAlive) { c.terminate(); continue; }
    c.isAlive = false; try { c.ping(); } catch (_) {}
  }
}, 20000);

server.listen(PORT, () => console.log('FootBall server on :' + PORT + (API_URL ? '' : '  (تحذير: API_URL غير مضبوط)')));
