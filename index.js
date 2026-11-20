"use strict";
const fs = require("fs"), path = require("path"), { login } = require("ws3-fca"), botSettings = require("./bot-settings"), { createProxyManager } = require("./proxy-config");
const proxyManager = createProxyManager({ initialEnabled: botSettings.getProxyEnabled(), persistEnabled: botSettings.setProxyEnabled }), proxyStartup = proxyManager.initialize();
const { getBanatConversationReply, getTriggerReply } = require("./triggers"), { sendBanatReplyWithTyping } = require("./banat-human"), { isDuplicateMessage } = require("./message-dedupe"), { isBotAdmin } = require("./admin-access");
const { getGroupSettings, updateGroup: updateGroupSettings, getReactionSettings, isQuietNow, shouldReact } = require("./bot-settings"), { classifyBanatTarget, setBanatConversationMode, isBanatConversationModeActive } = require("./banat-targeting");
const { createRaidMode } = require("./raid-mode"), { createEclipseControlsStore } = require("./eclipse-controls-store"), { createCountdownEngine } = require("./countdown-engine"), { createGroupTitleGuard } = require("./group-title-guard"), { createNicknameLockGuard } = require("./nickname-lock-guard"), { createReactionAudience, validateReactionTarget } = require("./reaction-audience"), { executeMassSetNickname } = require("./mass-setnn"), { createSpamMode } = require("./spam-mode"), { isTimeCheck, formatTimeCheck } = require("./time-check");

const raidMode = createRaidMode({ getApi: () => messengerApi, namespace: "celestial-bot" }), eclipseControlsStore = createEclipseControlsStore({ namespace: "celestial-bot" }), countdownEngine = createCountdownEngine({ store: eclipseControlsStore, getApi: () => messengerApi }), groupTitleGuard = createGroupTitleGuard({ store: eclipseControlsStore, getApi: () => messengerApi }), nicknameLockGuard = createNicknameLockGuard({ store: eclipseControlsStore, getApi: () => messengerApi });
const spamMode = createSpamMode({ getApi: () => messengerApi, isAuthorized: () => !botPaused && messengerConnected, sendMessage: (api, m, t) => new Promise((res, rej) => trafficSendMessage(api, m, t, (e, i) => e ? rej(e) : res(i))) });

let persistentReactionSettings = null, botUserID = "", messengerApi = null, botPaused = false, messengerConnected = false, loginError = null, lastConnectedAt = null, lastDisconnectedAt = null, lastListenerError = null, lastMessageAt = null, loginInProgress = false, loginGeneration = 0, activeListenerGeneration = 0, loginRetryTimer = null, loginAttempt = 0, queuedReplacementSession = null, globalActive = 0;
const activeThreads = new Set(), threadQueues = new Map(), threadLastSent = new Map(), threadCooldown = new Map(), PORT = Number(process.env.PORT || 10000), DEFAULT_ON = /^(1|true|yes|on)$/i.test(process.env.BANAT_DEFAULT_ON || "false"), GLOBAL_SEND_LIMIT = Math.max(1, Number(process.env.BANAT_GLOBAL_SEND_LIMIT || 2)), THREAD_COOLDOWN_MS = Math.max(0, Number(process.env.BANAT_THREAD_COOLDOWN_MS ?? 2500)), RETRY_DELAYS = [1500, 4000, 8000], MAX_LOGIN_BACKOFF_MS = 300000, LISTENER_RETRY_MS = 30000, RATE_LIMIT_RETRY_MS = 120000;

const sleep = ms => new Promise(r => setTimeout(r, ms));
function readSession() { const d = require("./session-store").read(); if (d) return d; const raw = process.env.FB_COOKIES || process.env.FB_APPSTATE; if (raw) { try { return JSON.parse(String(raw).trim()); } catch (_) { return String(raw).trim(); } } for (const f of ["appstate.json", "cookies.json"]) { const p = path.join(process.cwd(), f); if (fs.existsSync(p)) return JSON.parse(fs.readFileSync(p, "utf8")); } throw new Error("No session found."); }
function normalizeSession(v) { if (typeof v === "string") { const c = v.trim(); if (!c) throw new Error("Empty."); return c; } const e = Array.isArray(v) ? v : Array.isArray(v?.appState) ? v.appState : Array.isArray(v?.cookies) ? v.cookies : null; if (!e) throw new Error("Invalid session."); const p = e.map(x => { const k = x?.key ?? x?.name, val = x?.value; return k != null && val != null ? String(k).trim() + "=" + String(val) : null; }).filter(Boolean); if (!p.length) throw new Error("No cookies."); return p.join("; "); }
function enqueue(t, j) { const k = String(t), c = threadQueues.get(k) || Promise.resolve(), n = c.catch(() => {}).then(j).finally(() => { if (threadQueues.get(k) === n) threadQueues.delete(k); }); threadQueues.set(k, n); return n; }
async function acquireGlobalSlot() { while (globalActive >= GLOBAL_SEND_LIMIT) await sleep(150); globalActive++; }
function releaseGlobalSlot() { globalActive = Math.max(0, globalActive - 1); }
function is1545012(e) { return /1545012|temporarily unavailable|message could not be sent/i.test(JSON.stringify(e || "")); }

function trafficSendMessage(api, message, threadID, callback, replyToMessageID = null) {
  const key = String(threadID);
  return enqueue(key, async () => {
    const now = Date.now(), cU = Number(threadCooldown.get(key) || 0);
    if (cU > now) { await sleep(cU - now); threadCooldown.delete(key); }
    const gs = getGroupSettings(key), sC = gs.cooldownMs == null ? THREAD_COOLDOWN_MS : Math.max(0, Number(gs.cooldownMs)), sL = now - Number(threadLastSent.get(key) || 0);
    if (sL < sC) await sleep(sC - sL);
    await acquireGlobalSlot();
    try {
      let lastError = null;
      for (let a = 0; a <= RETRY_DELAYS.length; a++) {
        try {
          const result = await new Promise((res, rej) => {
            let s = false; const done = (e, i) => { if (s) return; s = true; e ? rej(e) : res(i); };
            try { const r = replyToMessageID ? api.sendMessage(message, threadID, done, replyToMessageID) : api.sendMessage(message, threadID, done); if (r && typeof r.then === "function") r.then(i => done(null, i)).catch(done); } catch (e) { rej(e); }
          });
          threadLastSent.set(key, Date.now()); callback(null, result); return;
        } catch (error) {
          lastError = error; if (!is1545012(error) || a >= RETRY_DELAYS.length) break;
          threadCooldown.set(key, Date.now() + Math.min(15000, RETRY_DELAYS[a])); await sleep(RETRY_DELAYS[a]); threadCooldown.delete(key);
        }
      }
      if (is1545012(lastError)) threadCooldown.set(key, Date.now() + 60000);
      callback(lastError);
    } finally { releaseGlobalSlot(); }
  });
}

function sendCommandReply(api, event, message) {
  const t = String(event.threadID);
  new Promise((res, rej) => { let s = false; const d = (e, i) => { if (s) return; s = true; e ? rej(e) : res(i); }; try { const r = event.messageID ? api.sendMessage(message, t, d, event.messageID) : api.sendMessage(message, t, d); if (r && typeof r.then === "function") r.then(i => d(null, i)).catch(d); } catch (e) { d(e); } }).catch(() => {});
}

function handleBanatCommand(api, event, body) {
  const t = String(event.threadID), sub = (String(body).trim().split(/\s+/)[1] || "status").toLowerCase(), changes = ["on", "enable", "start", "off", "disable", "stop", "toggle"].includes(sub);
  if (changes && !isBotAdmin(event.senderID)) return sendCommandReply(api, event, "⛔ admin only.");
  if (sub === "on" || sub === "enable" || sub === "start") { updateGroupSettings(t, { banatMode: "on" }); setBanatConversationMode(t, true, event.senderID); activeThreads.add(t); return sendCommandReply(api, event, "banat is on. say whatever u want 😭"); }
  if (sub === "off" || sub === "disable" || sub === "stop") { updateGroupSettings(t, { banatMode: "off" }); setBanatConversationMode(t, false); activeThreads.delete(t); return sendCommandReply(api, event, "banat off. peace 😭"); }
  if (sub === "toggle") { const c = getGroupSettings(t), cur = c.banatMode === "on" || (c.banatMode === "inherit" && (activeThreads.has(t) || isBanatConversationModeActive(t))), nxt = !cur; updateGroupSettings(t, { banatMode: nxt ? "on" : "off" }); setBanatConversationMode(t, nxt, event.senderID); nxt ? activeThreads.add(t) : activeThreads.delete(t); return sendCommandReply(api, event, nxt ? "banat is on 😭" : "banat is off"); }
  if (sub === "status") return sendCommandReply(api, event, (activeThreads.has(t) || isBanatConversationModeActive(t)) ? "banat: ON 🟢" : "banat: OFF 🔴");
  sendCommandReply(api, event, "!banat on · off · toggle · status");
}

async function sendBanat(api, event, text) { const g = getGroupSettings(String(event.threadID)); return sendBanatReplyWithTyping(api, text, String(event.threadID), event.messageID || null, { trafficSendMessage, incomingText: event.body || "", typingEnabled: g.typingEnabled, replyDelayMs: g.replyDelayMs === null ? undefined : g.replyDelayMs }); }
function reactToMessage(api, event, emoji) { const t = String(event?.threadID || ""), m = String(event?.messageID || ""); if (!t || !m) return; try { if (typeof api?.setMessageReactionMqtt === "function") api.setMessageReactionMqtt(emoji, m, t, () => {}); else if (typeof api?.setMessageReaction === "function") api.setMessageReaction(emoji, m, () => {}, true); } catch (_) {} }

function onMessage(api, event) {
  if (!event || (event.type && !["message", "message_reply"].includes(event.type)) || botPaused || isDuplicateMessage(event.messageID) || (event.senderID && botUserID && String(event.senderID) === String(botUserID))) return;
  const body = String(event.body || "").trim(); if (!body) return;
  if (body.startsWith("!banat")) return handleBanatCommand(api, event, body);
  if (isTimeCheck(body)) return sendCommandReply(api, event, formatTimeCheck());
  const t = String(event.threadID), gs = getGroupSettings(t), r = persistentReactionSettings || getReactionSettings(), sid = String(event.senderID || "");
  if (r.enabled && ["incoming_messages", "both"].includes(r.target)) {
    reactionAudience.isAllowed(r, event, api).then(a => {
      const cur = persistentReactionSettings || getReactionSettings(), adm = r.targetMode === "admins" && isBotAdmin(sid) && (!r.targetThreadID || r.targetThreadID === t);
      if ((a || adm) && cur.targetMode === r.targetMode && JSON.stringify(cur.targetUserIds || []) === JSON.stringify(r.targetUserIds || []) && cur.targetThreadID === r.targetThreadID && cur.emoji === r.emoji && shouldReact(t, "incoming_messages")) reactToMessage(api, event, r.emoji);
    }).catch(() => {});
  }
  if (isQuietNow(gs) || gs.banatMode === "off") return;
  const active = gs.banatMode === "on" || (gs.banatMode === "inherit" && (activeThreads.has(t) || isBanatConversationModeActive(t))), tgt = classifyBanatTarget({ event, body, eclipseUserID: botUserID });
  if (active) { const rep = getBanatConversationReply(body, t); if (rep) sendBanat(api, event, rep).catch(() => {}); return; }
  if (tgt.shouldRespond) { const rep = getTriggerReply(body, t) || getBanatConversationReply(body, t); if (rep) sendBanat(api, event, rep).catch(() => {}); }
}

function start(api, gen, sess = null) {
  if (messengerApi && messengerApi !== api) { activeListenerGeneration = 0; try { messengerApi.stopListenMqtt?.(); } catch (_) {} }
  messengerApi = api; activeListenerGeneration = gen; messengerConnected = true; loginError = null; loginAttempt = 0; lastConnectedAt = new Date().toISOString(); lastListenerError = null;
  try { botUserID = String(api.getCurrentUserID?.() || ""); } catch (_) {}
  try { const ref = api.getAppState?.(); if (Array.isArray(ref) && ref.length) require("./session-store").save(JSON.stringify(ref)); else if (sess) require("./session-store").save(sess); } catch (_) {}
  try { if (typeof api.setOptions === "function") api.setOptions({ listenEvents: true }); } catch (_) {}
  if (raidMode.isInitialized()) raidMode.start();
  if (eclipseControlsStore.isInitialized()) { countdownEngine.restore(api).catch(() => {}); groupTitleGuard.start(); nicknameLockGuard.start(); }
  api.listenMqtt((err, event) => {
    if (gen !== activeListenerGeneration) return;
    if (err) {
      messengerConnected = false; spamMode.stopAll("disconnected"); lastDisconnectedAt = new Date().toISOString(); lastListenerError = String(err.message || err); messengerApi = null; activeListenerGeneration = 0;
      try { api.stopListenMqtt?.(); } catch (_) {}
      if (/checkpoint|2fa|invalid|expired|rejected|not logged in/i.test(lastListenerError)) return loginError = "Session rejected. Paste a fresh one.";
      const delay = /1675004|rate.?limit/i.test(lastListenerError) ? RATE_LIMIT_RETRY_MS : LISTENER_RETRY_MS;
      if (!loginRetryTimer) loginRetryTimer = setTimeout(() => { loginRetryTimer = null; if (!messengerConnected && !loginInProgress) loginBot(); }, delay);
      return;
    }
    if (event) { messengerConnected = true; lastMessageAt = new Date().toISOString(); try { raidMode.handleEvent(event).catch(() => {}); countdownEngine.onMessage(api, event).catch(() => {}); groupTitleGuard.handleEvent(event).catch(() => {}); nicknameLockGuard.handleEvent(event).catch(() => {}); } catch (_) {} 
      if (DEFAULT_ON && event?.threadID && event?.senderID && String(event.senderID) !== botUserID) { const k = String(event.threadID); if (!activeThreads.has(k)) { activeThreads.add(k); setBanatConversationMode(k, true, event.senderID); } }
      onMessage(api, event); }
  });
  console.log(`[CELESTIAL] online${botUserID ? " as " + botUserID : ""}`);
}

function loginBot(sess = null) {
  const p = proxyManager.getStatus(); if (!p.ready) return loginError = p.error || "Proxy not ready.", false;
  if (loginInProgress) { if (sess) queuedReplacementSession = sess; return true; }
  if (messengerConnected && messengerApi && activeListenerGeneration > 0 && !sess) return false;
  let c; try { const sup = sess || readSession(); c = normalizeSession(require("./session-store").normalizeSessionInput(typeof sup === "string" ? sup : JSON.stringify(sup))); } catch (e) { return loginError = String(e.message || e), false; }
  loginInProgress = true; const gen = ++loginGeneration; let settled = false;
  const fail = e => {
    if (settled || gen !== loginGeneration) return; settled = true; loginInProgress = false; loginError = String(e.message || e);
    if (queuedReplacementSession) { const q = queuedReplacementSession; queuedReplacementSession = null; return loginBot(q); }
    if (sess && messengerConnected && messengerApi) return loginError = "Replacement rejected.";
    loginAttempt++; const d = Math.min(2 ** loginAttempt * 1000, MAX_LOGIN_BACKOFF_MS);
    if (!loginRetryTimer) loginRetryTimer = setTimeout(() => { loginRetryTimer = null; if (!messengerConnected && !loginInProgress) loginBot(); }, d);
  };
  try {
    const res = login(c, proxyManager.getLoginOptions(), (e, api) => { if (gen !== loginGeneration || settled) return; if (e || !api) return fail(e || new Error("No API")); settled = true; loginInProgress = false; if (loginRetryTimer) { clearTimeout(loginRetryTimer); loginRetryTimer = null; } start(api, gen, sess); if (queuedReplacementSession) { const q = queuedReplacementSession; queuedReplacementSession = null; setTimeout(() => loginBot(q), 0); } });
    if (res && typeof res.then === "function") res.then(api => { if (gen !== loginGeneration || settled || !api) return; settled = true; loginInProgress = false; if (loginRetryTimer) { clearTimeout(loginRetryTimer); loginRetryTimer = null; } start(api, gen, sess); if (queuedReplacementSession) { const q = queuedReplacementSession; queuedReplacementSession = null; setTimeout(() => loginBot(q), 0); } }).catch(fail);
  } catch (e) { fail(e); }
  return true;
}

function fetchMessengerGroups(api) { if (!api?.getThreadList) return Promise.reject(new Error("Group list unavailable")); return new Promise((res, rej) => { let s = false; const d = (e, l) => { if (s) return; s = true; e ? rej(e) : res((Array.isArray(l) ? l : []).filter(x => x && (x.threadID || x.threadId)).map(x => ({ threadID: String(x.threadID || x.threadId), name: String(x.name || x.threadName || "Unnamed"), isGroup: x.isGroup !== false, participantCount: Number(x.participantIDs?.length || 0) })).filter(x => x.isGroup)); }; try { const r = api.getThreadList(100, null, [], d); if (r?.then) r.then(v => d(null, v)).catch(d); } catch (e) { d(e); } }); }
async function fetchMessengerGroupMembers(api, t) { if (!t || !api?.getThreadInfo) throw new Error("Member lookup unavailable"); const i = await new Promise((res, rej) => { let s = false; const d = (e, v) => { if (s) return; s = true; e ? rej(e) : res(v); }; try { const r = api.getThreadInfo(String(t), d); if (r?.then) r.then(v => d(null, v)).catch(d); } catch (e) { d(e); } }); const ids = [...new Set((Array.isArray(i?.participantIDs) ? i.participantIDs : []).map(String).filter(Boolean))], names = new Map(); const add = v => { if (Array.isArray(v)) v.forEach(x => { if (x?.id || x?.userID) names.set(String(x.id || x.userID), x); }); else if (v && typeof v === "object") Object.entries(v).forEach(([k, x]) => { if (x && typeof x === "object") names.set(String(k), x); }); }; add(i?.userInfo); const miss = ids.filter(x => !String(names.get(x)?.name || names.get(x)?.firstName || "").trim()); if (miss.length && api.getUserInfo) { try { const f = await new Promise((res, rej) => { let s = false; const d = (e, v) => { if (s) return; s = true; e ? rej(e) : res(v); }; try { const r = api.getUserInfo(miss, d); if (r?.then) r.then(v => d(null, v)).catch(d); } catch (e) { d(e); } }); add(f); } catch (_) {} } const adm = new Set(Array.isArray(i?.adminIDs) ? i.adminIDs.map(String) : []); return ids.map(id => { const p = names.get(id) || {}, n = p.name || p.fullName || p.firstName || ""; return { userID: id, name: String(n || "Unknown").slice(0, 100), isAdmin: adm.has(id), nameResolved: !!n }; }).sort((a, b) => a.name.localeCompare(b.name)); }

const reactionAudience = createReactionAudience({ ttlMs: 120000, getAdmins: async (api, t) => (await fetchMessengerGroupMembers(api, t)).filter(m => m.isAdmin).map(m => m.userID) });

try {
  require("./dashboard-api")(PORT, () => ({ ok: true, facebook_connected: messengerConnected, session_active: messengerConnected, bot_running: messengerConnected && !botPaused, login_in_progress: loginInProgress, login_error: loginError, proxy: proxyManager.getStatus(), last_connected_at: lastConnectedAt, last_disconnected_at: lastDisconnectedAt, last_message_at: lastMessageAt, updated_at: new Date().toISOString() }), {
    setPaused: p => { botPaused = p; if (p) spamMode.stopAll("paused"); }, getPaused: () => botPaused, proxyManager,
    onProxyEnabled: () => { if (!messengerConnected && !loginInProgress) loginBot(); },
    saveSession: v => { loginBot(require("./session-store").normalizeSessionInput(v)); return { saved: true, validating: true }; },
    getSessionStatus: () => require("./session-store").status(), clearSession: () => require("./session-store").clear(),
    getGroups: () => fetchMessengerGroups(messengerApi), getGroupMembers: t => fetchMessengerGroupMembers(messengerApi, t),
    massSetNickname: async (t, u, n) => { if (!eclipseControlsStore.isInitialized()) throw new Error("Neon unavailable."); return executeMassSetNickname({ api: messengerApi, threadID: t, userIDs: u, nickname: n, getMembers: id => fetchMessengerGroupMembers(messengerApi, id), delayMs: 1200, saveNickname: (id, u, v) => eclipseControlsStore.saveNicknameLock(id, u, v) }); },
    countdown: { getActive: () => countdownEngine.getActive(), getHistory: () => eclipseControlsStore.listCountdownHistory(50), getReceiptSettings: () => eclipseControlsStore.getCountdownReceiptSettings(), updateReceiptSettings: i => eclipseControlsStore.updateCountdownReceiptSettings(i), getGroup: t => eclipseControlsStore.getGroup(t), updateGroup: async (t, i) => { if (i.countdownTargetId) { const ms = await fetchMessengerGroupMembers(messengerApi, t), m = ms.find(x => x.userID === String(i.countdownTargetId)); if (!m) throw new Error("Target not in group."); i = { ...i, countdownTargetName: m.name }; } const b = await eclipseControlsStore.getGroup(t), s = await eclipseControlsStore.updateGroup(t, i); if (!s.countdownEnabled || String(b.countdownTargetId || "") !== String(s.countdownTargetId || "")) await countdownEngine.cancel(t); if (s.autolockEnabled) try { await groupTitleGuard.repair(String(t), "
