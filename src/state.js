// Ladder state transitions — pure functions over a plain object, shared by the Durable Object
// (src/state-do.js) and the in-memory store used in tests.
//
//   state = {
//     health: { [model]: { failures, firstFailureAt, lastFailureAt, class, skipUntil } },
//                          // skipUntil: epoch ms = skip everyone until then;
//                          //           null = config-class (dead until a success clears it);
//                          //            0  = context-class (recorded, but the rung stays eligible)
//     keys:   { active: <pool index>,
//               exhausted: { [index]: <epoch ms until usable> },   // общий парк: виден ВСЕМ
//               paid:       { [index]: <epoch ms until usable> } }, // парк платного тира (#341)
//                          // the GO credential pool's rotation state.
//                          //
//                          // Две карты, потому что лимит НЕ равнозначен для разных тиров (#341,
//                          // инцидент 2026-10-28): 429 от ПЛАТНОГО ранга сажал ключ в общий
//                          // exhausted, и когда так садились все четыре, у бесплатного ранга
//                          // исчезал spare-обход — ровно одна попытка на припаркованном ключе,
//                          // дальше отказ. При этом за тот же час beam-free отвечал 61 разу:
//                          // бесплатный тир не расходует платное allowance (#69) и лимит
//                          // платного ранга ему ничего не говорит. `paid` читают только платные
//                          // ранги; `exhausted` читают все, он остаётся местом для настоящих
//                          // свойств АККАУНТА (мёртвый ключ, нет оплаты).
//     or:     { parked: { [index]: <epoch ms until usable> } },
//     pins:   { [pinKey]: { rung, lastUsedAt } }, // conversation pins (epic #17)
//                          // the OPENROUTER credential pool's account parks (issue #297) — a
//                          // separate namespace: `keys` belongs to the Go pool, and a Go key's
//                          // exhaustion must never park an OpenRouter account (and back)
//   }
//
// Backoff mirrors trained-assist-agent's model-health.js: transient failures skip a model for
// base × multiplier^(n-1) — counted PER MODEL, so every rung starts its own schedule; failures
// older than the window stop counting. The base is 2 s on purpose (owner 2026-10-05): a flake
// clears in one hop instead of costing 15 s of a conversation, and consecutive failures still
// back off (2 → 4 → 8 → 16 → 32 → 64 → 128 → 256, cap 5 min) so a dead rung stays quiet.
export const DEFAULT_BACKOFF = Object.freeze({ baseMs: 2000, multiplier: 2, capMs: 300000, failureWindowMs: 900000 });

export function emptyState() {
  return { health: {}, keys: { active: 0, exhausted: {}, paid: {} }, or: { parked: {} }, pins: {} };
}

export function backoffFor(failures, policy = DEFAULT_BACKOFF) {
  const n = Math.max(1, Number(failures) || 1);
  return Math.min(policy.baseMs * Math.pow(policy.multiplier, n - 1), policy.capMs);
}

export function recordFailure(state, model, { cls = 'transient', retryAfterMs } = {}, now = Date.now(), policy = DEFAULT_BACKOFF) {
  let e = state.health[model];
  if (!e || (e.firstFailureAt && now - e.firstFailureAt > policy.failureWindowMs)) e = { failures: 0, firstFailureAt: now };
  e.failures += 1;
  e.lastFailureAt = now;
  e.class = cls;
  if (cls === 'config') e.skipUntil = null;
  // Recorded but never shared: a context-class failure is a property of THIS request (the prompt
  // did not fit), so the rung stays eligible for everyone else. `0` is the deliberate "not
  // skipped" marker — `null` means the opposite (config = dead until a success clears it) and
  // both readers key off that: `skipped()` tests `=== null`, the gate tests `until > now`.
  else if (cls === 'context') e.skipUntil = 0;
  else if (cls === 'transient') e.skipUntil = now + backoffFor(e.failures, policy);
  else e.skipUntil = now + (Number.isFinite(retryAfterMs) ? Math.max(0, retryAfterMs) : backoffFor(e.failures, policy));
  state.health[model] = e;
  return state;
}

export function recordSuccess(state, model) {
  delete state.health[model];
  return state;
}

// Skip `models` until `untilMs` (all Go keys parked). At least a minute, so a key whose TTL is
// about to lapse doesn't bounce the ladder straight back onto a gateway that just failed.
export function park(state, models, untilMs, now = Date.now()) {
  const until = Math.max(untilMs, now + 60 * 1000);
  for (const m of models) {
    const e = state.health[m] || { failures: 0, firstFailureAt: now };
    state.health[m] = { ...e, failures: e.failures + 1, lastFailureAt: now, class: 'quota', skipUntil: until };
  }
  return state;
}

// Park the key that FAILED (`failedIndex`, default: the active one) for ttlMs and move to the next
// usable one. → { rotated: true, fromIndex, toIndex } | { rotated: false, retryAt }
//
// Concurrency (incident 2026-09-29): calls A and B both start on key 0; A hits the weekly limit and
// rotates to key 1; B then hits the same limit on key 0. Parking "the active key" at that point
// parked the HEALTHY key 1 for 6h and took Go down for the whole fleet with a working key in the
// pool. The failed key is what gets parked; if another call already moved `active` onto a usable
// key, that counts as rotated.
// Park the key that FAILED (`failedIndex`, default: the active one) for ttlMs and move to the next
// usable one. → { rotated: true, fromIndex, toIndex } | { rotated: false, retryAt }
//
// `scope` decides BOTH where the park is recorded and which parks this caller still respects (#341):
//
//   'account' (по умолчанию, поведение прежних версий) — в общий `exhausted`, читают все. Это
//     место для настоящих свойств АККАУНТА: мёртвый ключ (401/unauthorized), нет оплаты, и для
//     лимита, который видел БЕСПЛАТНЫЙ ранг — там мы консервативны и не ослабляем фоллбэк.
//
//   'paid' — в `keys.paid`, читают только платные ранги. Для них следующий пригодный ключ — это
//     ещё и ключ без платного парка, так что хвост не садится на только что упавший ключ.
//     Бесплатный ранг эту карту не читает: лимит платного тира ему ничего не говорит (см. шапку
//     модуля, инцидент 2026-10-28).
//
// Concurrency (incident 2026-09-29): calls A and B both start on key 0; A hits the weekly limit and
// rotates to key 1; B then hits the same limit on key 0. Parking "the active key" at that point
// parked the HEALTHY key 1 for 6h and took Go down for the whole fleet with a working key in the
// pool. The failed key is what gets parked; if another call already moved `active` onto a usable
// key, that counts as rotated.
export function rotateKey(state, poolSize, ttlMs, now = Date.now(), failedIndex = null, scope = 'account') {
  const exhausted = state.keys.exhausted || {};
  const paid = state.keys.paid || {};
  for (const map of [exhausted, paid]) for (const k of Object.keys(map)) if (!(map[k] > now)) delete map[k];
  const last = Math.max(0, poolSize - 1);
  const active = Math.min(state.keys.active || 0, last);
  const from = Number.isInteger(failedIndex) && failedIndex >= 0 && failedIndex <= last ? failedIndex : active;
  (scope === 'paid' ? paid : exhausted)[from] = now + ttlMs;
  state.keys.exhausted = exhausted;
  state.keys.paid = paid;
  // Пригоден ключ, который не заблокирован ничем, что ЭТОТ вызов уважает: общий парк — всеми,
  // платный — ещё и платными. `active` тоже двигается по этому же правилу.
  const blocked = (i) => (exhausted[i] > now) || (scope === 'paid' && paid[i] > now);
  if (active !== from && !blocked(active)) return { rotated: true, fromIndex: from, toIndex: active };
  for (let step = 1; step < poolSize; step++) {
    const i = (from + step) % poolSize;
    if (!blocked(i)) {
      state.keys.active = i;
      return { rotated: true, fromIndex: from, toIndex: i };
    }
  }
  let retryAt = Infinity;
  for (let i = 0; i < poolSize; i++) retryAt = Math.min(retryAt, blocked(i) ? (exhausted[i] || paid[i] || now) : now);
  return { rotated: false, retryAt: Number.isFinite(retryAt) ? retryAt : now + ttlMs };
}

// Ops lever: clear every Go key park and every Go rung skip (e.g. a key was parked by mistake or a
// limit was lifted early). OpenRouter health and conversation pins are left alone — but the
// OpenRouter ACCOUNT parks are limits too, so they are released as well: after the operator tops
// an account up, this is the lever that makes it usable again without waiting for the TTL
// (issue #297).
export function resetKeys(state) {
  state.keys = { active: 0, exhausted: {}, paid: {} };
  state.or = { ...(state.or || {}), parked: {} };
  for (const m of Object.keys(state.health)) if (m.startsWith('opencode-go/')) delete state.health[m];
  return state;
}

// Park an OpenRouter ACCOUNT (index in the OpenRouter credential pool) until `untilMs`.
//
// The free-model allowance is counted per account, so the account is what runs out: parking the
// ACCOUNT (and not the rung) keeps the other accounts of the pool serving the same rung, while the
// capped account waits for its own reset (issue #297). At least a minute, so an account that just
// refused is not probed again in the same breath.
export function parkAccount(state, index, untilMs, now = Date.now()) {
  const key = Number(index);
  if (!Number.isInteger(key) || key < 0) return state;
  const until = Math.max(Number(untilMs) || 0, now + 60 * 1000);
  const parked = { ...((state.or && state.or.parked) || {}) };
  parked[key] = Math.max(parked[key] || 0, until);
  state.or = { ...(state.or || {}), parked };
  return state;
}

// The live (not yet expired) account parks — the read view the ladder picks an account from.
export function parkedAccounts(state, now = Date.now()) {
  const parked = { ...((state.or && state.or.parked) || {}) };
  for (const k of Object.keys(parked)) if (!(parked[k] > now)) delete parked[k];
  return parked;
}

// Read view: expired key exhaustion cleared; the active key moved off a parked one if another is
// usable (e.g. after its TTL lapsed).
export function snapshot(state, poolSize, now = Date.now()) {
  const exhausted = { ...(state.keys.exhausted || {}) };
  const paid = { ...(state.keys.paid || {}) };
  for (const map of [exhausted, paid]) for (const k of Object.keys(map)) if (!(map[k] > now)) delete map[k];
  // `active` сдвигается только с ОБЩЕГО парка: платный парк не делает ключ непригодным для
  // бесплатного тира, поэтому ворочать из-за него общий курсор — значит вредить тому, кому он
  // не мешает. Платные ранги сами обходят платный парк в run() (#341).
  let active = state.keys.active || 0;
  if (poolSize > 0 && exhausted[active] > now) {
    for (let i = 0; i < poolSize; i++) if (!(exhausted[i] > now)) { active = i; break; }
  }
  return { health: state.health, keys: { active, exhausted, paid }, or: { parked: parkedAccounts(state, now) } };
}

// ── Sticky rung pins (epic #17) ───────────────────────────────────────────────
// A pin remembers the rung that served ONE conversation (keyed by Kh = sha256(conversation id))
// so that rung's provider-side prompt cache keeps working turn after turn. One conversation, one
// pin; the pin lives separately from the health blob and only expires by TTL / context overflow.
//
//   pin = { rung: <full rung id>, lastUsedAt: <epoch ms> }
//   state.pins = { [Kh]: pin }                    // in-memory view (tests)
//   storage key 'pin:<Kh>' → pin                  // Durable Object (src/state-do.js)
export const PIN_TTL_MS = 30 * 60 * 1000; // ≥ 10-min idle window of the OpenRouter prompt cache
export const PIN_CAP = 5000; // safety net — TTL is the real limiter

// A pin VALUE is alive while its last touch is within the TTL window.
export function pinFresh(p, now = Date.now(), ttl = PIN_TTL_MS) {
  return !!(p && p.rung && now - p.lastUsedAt <= ttl);
}

// Next pin value for a write, or null when the write is throttled away: a hit on the SAME rung
// within ttl/2 refreshes nothing, so a healthy success costs no storage write (today's hot path
// does not write on a healthy first-rung answer and must not start now).
export function pinDirty(prev, rung, now = Date.now(), ttl = PIN_TTL_MS, throttleMs = null) {
  const t = throttleMs ?? Math.max(1, Math.floor(ttl / 2));
  if (prev && prev.rung === rung && now - prev.lastUsedAt < t) return null;
  return { rung, lastUsedAt: now };
}

// Count and group a pins map, pruning expired entries in place (used by memoryStore and the DO
// sweep). Returns { count, byRung: { [rung]: n } }.
export function pinStats(pins, now = Date.now(), ttl = PIN_TTL_MS) {
  const byRung = {};
  let count = 0;
  if (pins) for (const [k, p] of Object.entries(pins)) {
    if (!pinFresh(p, now, ttl)) { delete pins[k]; continue; }
    count++;
    byRung[p.rung] = (byRung[p.rung] || 0) + 1;
  }
  return { count, byRung };
}

// In-memory store with the same async interface as the Durable Object — tests, local runs.
export function memoryStore(poolSize = 0, initial = emptyState()) {
  const state = { ...initial, health: { ...(initial.health || {}) }, keys: { ...(initial.keys || {}) }, or: { ...(initial.or || {}) }, pins: { ...(initial.pins || {}) } };
  return {
    state,
    async snapshot(poolSizeArg, pinKey) {
      const s = snapshot(state, poolSizeArg ?? poolSize);
      const p = pinKey ? state.pins[String(pinKey)] : null;
      return { ...s, pin: pinFresh(p) ? p : null };
    },
    async recordFailure(model, f, extra) {
      recordFailure(state, model, f);
      if (extra && extra.pinRemove) delete state.pins[String(extra.pinRemove.pinKey)];
    },
    async recordSuccess(model, extra) {
      recordSuccess(state, model);
      if (extra && extra.pin) {
        const k = String(extra.pin.pinKey);
        const v = pinDirty(state.pins[k], extra.pin.rung);
        if (v) state.pins[k] = v;
      }
    },
    async rotateKey(size, ttlMs, failedIndex, scope) { return rotateKey(state, size, ttlMs, Date.now(), failedIndex, scope); },
    async resetKeys() { resetKeys(state); },
    async park(models, untilMs) { park(state, models, untilMs); },
    async parkAccount(index, untilMs) { parkAccount(state, index, untilMs); },
    async pinStats() { return pinStats(state.pins); },
  };
}
