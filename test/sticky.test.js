// Sticky rung per conversation (epic #17, plan 4bc36b06). Unit + integration for the pin layer
// (state.js), the routing policy (ladder.js run()), the route (handler.js) and observability.
// The full acceptance scenario lives in test/sandbox/ (run via `npm run test:sandbox`).

import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { run } from '../src/ladder.js';
import { handle } from '../src/handler.js';
import { memoryStore, pinFresh, pinDirty, pinStats, emptyState } from '../src/state.js';

const config = JSON.parse(fs.readFileSync(new URL('../config/ladders.json', import.meta.url)));
// Behaviour here pins real Go/OpenRouter rungs, so run against a config with the zen-rings head
// removed: those rungs are cold in tests and would failover, shifting every LADDER index.
const noZen = (l) => l.filter((m) => !m.startsWith('zen-rings/'));
const GOCFG = { ...config, ladders: {}, providers: {} };
for (const [name, roles] of Object.entries(config.ladders)) {
  GOCFG.ladders[name] = Object.fromEntries(Object.entries(roles).map(([r, l]) => [r, noZen(l)]));
}
const LADDER = GOCFG.ladders.service.build;
const short = m => m.replace(/^opencode-go\/|^openrouter\//, '');
const env = { OPENCODE_GO_API_KEYS: 'oc_a,oc_b', OPENROUTER_API_KEY: 'or_key' };
const ENV = { LADDER_TOKEN: 't', OPENCODE_GO_API_KEYS: 'oc_a,oc_b', OPENROUTER_API_KEY: 'or_key' };
const msg = { model: 'service', messages: [{ role: 'user', content: 'hi' }] };
const Kh = 'a'.repeat(64); // any sha256-shaped key

// ── S1: pure pin layer ──────────────────────────────────────────────────────────────────────────
test('pin layer: freshness by TTL, dirty-write throttle, stats prune', () => {
  const t0 = 1_000_000_000;
  const p = { rung: 'opencode-go/m', lastUsedAt: t0 };
  assert.equal(pinFresh(p, t0 + 1000), true);
  assert.equal(pinFresh(p, t0 + 30 * 60 * 1000 + 1), false, 'expired past the pin TTL');
  assert.equal(pinFresh(null), false);

  assert.equal(pinDirty(null, 'opencode-go/m', t0).rung, 'opencode-go/m', 'first write always persists');
  const freshHit = pinDirty(p, 'opencode-go/m', t0 + 10_000);
  assert.equal(freshHit, null, 'hit on the same rung inside ttl/2 is throttled — no storage write');
  const staleHit = pinDirty(p, 'opencode-go/m', t0 + 1_000_000);
  assert.equal(staleHit.lastUsedAt, t0 + 1_000_000, 'hit older than ttl/2 refreshes lastUsedAt');
  const moved = pinDirty(p, 'opencode-go/other', t0 + 10_000);
  assert.equal(moved.rung, 'opencode-go/other', 'a rung change always persists');
});

test('memoryStore: snapshot returns the pinned rung, recordSuccess persists it, recordFailure drops it', async () => {
  const store = memoryStore(2);
  assert.equal((await store.snapshot(undefined, Kh)).pin, null, 'no pin yet');

  await store.recordSuccess('opencode-go/m', { pin: { pinKey: Kh, rung: 'opencode-go/m' } });
  const s1 = await store.snapshot(undefined, Kh);
  assert.equal(s1.pin.rung, 'opencode-go/m');
  assert.equal((await store.snapshot(undefined, 'other-key')).pin, null, 'other conversations see nothing');

  await store.recordSuccess('opencode-go/m', { pin: { pinKey: Kh, rung: 'openrouter/x/y' } });
  assert.equal((await store.snapshot(undefined, Kh)).pin.rung, 'openrouter/x/y', 're-pin on a new rung');

  await store.recordFailure('opencode-go/m', { cls: 'transient' }, { pinRemove: { pinKey: Kh, model: 'opencode-go/m' } });
  assert.equal((await store.snapshot(undefined, Kh)).pin, null, 'pinRemove deletes the conversation pin');
  assert.deepEqual(await store.pinStats(), { count: 0, byRung: {} });
});

test('pinStats groups by rung and prunes expired entries', () => {
  const pins = {
    [Kh]: { rung: 'opencode-go/m', lastUsedAt: Date.now() },
    ['b'.repeat(64)]: { rung: 'opencode-go/m', lastUsedAt: Date.now() },
    ['c'.repeat(64)]: { rung: 'openrouter/x/y', lastUsedAt: Date.now() - 31 * 60 * 1000 }, // expired
  };
  assert.deepEqual(pinStats(pins), { count: 2, byRung: { 'opencode-go/m': 2 } }, 'expired rung pruned, not counted');
  assert.equal(pins['c'.repeat(64)], undefined, 'expired entry pruned in place');
});

// ── shared fixtures for the run()-level slices ──────────────────────────────────────────────────
function recFetch(behaviour, calls) {
  return async (url, init) => {
    const body = JSON.parse(init.body);
    const auth = init.headers.Authorization;
    calls.push({ url, model: body.model, auth, session: init.headers['x-opencode-session'], sid: init.headers['x-session-id'], body });
    const r = (behaviour[body.model] || (() => ({ status: 200, content: 'ok' })))({ auth, body });
    if (r === 'throw') throw new Error('network down');
    const data = r.status === 200 ? { id: 'x', object: 'chat.completion', choices: [{ message: { role: 'assistant', content: r.content } }], usage: { prompt_tokens: 3 } } : null;
    return { ok: r.status === 200, status: r.status, json: async () => data, text: async () => r.error || '' };
  };
}
const R0 = LADDER[0]; // opencode-go/mimo-v2.6-flash
const R1 = LADDER[1]; // opencode-go/beam-free (was deepseek-v4.1-flash, dropped #49)
const OR = LADDER.findLast(m => m.startsWith('openrouter/'));
const OR_TWO = LADDER.filter(m => m.startsWith('openrouter/'))[1];

// ── S4: routing order and statuses ─────────────────────────────────────────────────────────────
test('S4: the pinned rung is tried FIRST and ignores the global skipUntil; a hit reports pin=hit', async () => {
  const store = memoryStore(2);
  store.state.pins[Kh] = { rung: R0, lastUsedAt: Date.now() };
  // R0 is in shared transient backoff — a fresh conversation would skip it entirely
  store.state.health[R0] = { failures: 2, firstFailureAt: Date.now(), lastFailureAt: Date.now() - 1000, skipUntil: Date.now() + 60000, class: 'transient' };
  const calls = [];
  const r = await run(msg, { env, config: GOCFG, store, fetchImpl: recFetch({}, calls), conversation: Kh });
  assert.equal(r.ok, true);
  assert.equal(r.model, R0, 'pinned rung is tried despite the shared skip');
  assert.equal(r.pin, 'hit');
  assert.equal(calls[0].model, short(R0));
});

test('S4: a stale pin (rung left the ladder) behaves as a new conversation and re-pins on success', async () => {
  const store = memoryStore(2);
  store.state.pins[Kh] = { rung: 'openrouter/retired/model', lastUsedAt: Date.now() };
  const calls = [];
  const r = await run(msg, { env, config: GOCFG, store, fetchImpl: recFetch({}, calls), conversation: Kh });
  assert.equal(r.model, R0, 'stale pin is ignored — fresh pick');
  assert.equal(r.pin, 'new');
  assert.equal(store.state.pins[Kh].rung, R0, 're-pinned to the rung that answered');
});

test('S4: without a pin key nothing pin-related happens (byte-for-byte today)', async () => {
  const store = memoryStore(2);
  const r = await run(msg, { env, config: GOCFG, store, fetchImpl: recFetch({}, []), conversation: null });
  assert.equal(r.pin, null);
  assert.equal(Object.keys(store.state.pins).length, 0, 'no pin written for unkeyed callers');
});

// ── S5: failure policy on a pinned rung ────────────────────────────────────────────────────────
test('S5: transient error on a pinned OpenRouter rung → one same-rung retry, pin NOT moved', async () => {
  const store = memoryStore(2);
  // pin the first paid rung; park the Go rungs so R6 does not steal the pin's rung-first order
  store.state.pins[Kh] = { rung: OR, lastUsedAt: Date.now() };
  for (const m of LADDER) if (m.startsWith('opencode-go/')) store.state.health[m] = { failures: 2, firstFailureAt: Date.now(), lastFailureAt: Date.now() - 1000, skipUntil: Date.now() + 60000, class: 'transient' };
  const beh = { [short(OR)]: (() => { let n = 0; return () => ({ status: ++n === 1 ? 500 : 200, error: 'boom', content: 'ok' }); })() };
  const calls = [];
  const r = await run(msg, { env, config: GOCFG, store, fetchImpl: recFetch(beh, calls), conversation: Kh });
  assert.equal(r.ok, true);
  assert.equal(r.model, OR, 'same rung answered the retry');
  assert.equal(r.pin, 'hit', 'the pin never moved');
  assert.equal(r.attempts.filter(a => a.outcome === 'pin-retry').length, 1, 'exactly one same-rung retry');
  assert.equal(store.state.pins[Kh].rung, OR);
});

test('S5: hard failure of the pinned rung → ONE switch down, pin=moved to the answering rung', async () => {
  const store = memoryStore(2);
  store.state.pins[Kh] = { rung: R0, lastUsedAt: Date.now() };
  const beh = { [short(R0)]: () => ({ status: 500, error: 'boom' }) };
  const r = await run(msg, { env, config: GOCFG, store, fetchImpl: recFetch(beh, []), conversation: Kh });
  assert.equal(r.model, R1, 'moved one rung down');
  assert.equal(r.pin, 'moved');
  assert.equal(store.state.pins[Kh].rung, R1, 're-pinned to the rung that answered');
});

test('S5: context overflow while stuck on the pinned rung retorts the error, invalidates the pin (⚫-1)', async () => {
  const store = memoryStore(2);
  store.state.pins[Kh] = { rung: R0, lastUsedAt: Date.now() };
  const beh = { [short(R0)]: () => ({ status: 400, error: 'This request exceeds the context window of the model' }) };
  const r = await run(msg, { env, config: GOCFG, store, fetchImpl: recFetch(beh, []), conversation: Kh });
  assert.equal(r.ok, false);
  assert.equal(r.status, 400, 'the overflow error is returned, not a retry loop');
  assert.equal(r.pin, 'gone');
  assert.equal(store.state.pins[Kh], undefined, 'pin invalidated');
});

// ── S6: return from the paid tail ─────────────────────────────────────────────────────────────
test('S6: a pin parked on the paid tail does NOT hold a conversation when Go is healthy — Go first, re-pin', async () => {
  const store = memoryStore(2);
  store.state.pins[Kh] = { rung: OR, lastUsedAt: Date.now() };
  const calls = [];
  const r = await run(msg, { env, config: GOCFG, store, fetchImpl: recFetch({}, calls), conversation: Kh });
  assert.equal(r.model, R0, 'healthy Go answered before the pinned paid rung');
  assert.equal(r.pin, 'moved');
  assert.equal(store.state.pins[Kh].rung, R0, 'pin moved back to Go');
  assert.equal(calls[0].model, short(R0));
});

test('S6: a pin on a Go rung is not moved off the Go tier', async () => {
  const store = memoryStore(2);
  store.state.pins[Kh] = { rung: R0, lastUsedAt: Date.now() };
  const r = await run(msg, { env, config: GOCFG, store, fetchImpl: recFetch({}, []), conversation: Kh });
  assert.equal(r.model, R0);
  assert.equal(r.pin, 'hit');
});

// ── S7: stable per-conversation session headers upstream ──────────────────────────────────────
test('S7: keyed calls send the shared Kh — Go x-opencode-session, OpenRouter x-session-id; unkeyed keeps a random session', async () => {
  const callsKeyed = [];
  await run(msg, { env, config: GOCFG, store: memoryStore(2), fetchImpl: recFetch({}, callsKeyed), conversation: Kh });
  await run(msg, { env, config: GOCFG, store: memoryStore(2), fetchImpl: recFetch({}, callsKeyed), conversation: Kh });
  const goCalls = callsKeyed.filter(c => c.url.includes('opencode.ai'));
  assert.ok(goCalls.length >= 2);
  assert.ok(goCalls.every(c => c.session === `ladder-${Kh}`), 'stable session header — the Go-side cache can work');
  const orCalls = callsKeyed.filter(c => c.url.includes('openrouter.ai'));
  assert.ok(orCalls.every(c => c.sid === Kh), 'OpenRouter side carries the same stable id');

  const callsUnkeyed = [];
  await run(msg, { env, config: GOCFG, store: memoryStore(2), fetchImpl: recFetch({}, callsUnkeyed), conversation: null });
  const u = callsUnkeyed.filter(c => c.url.includes('opencode.ai'));
  assert.ok(u[0].session && u[0].session !== `ladder-${Kh}`, 'unkeyed calls keep a random session as before');
});

// ── S8: observability on the wire ─────────────────────────────────────────────────────────────
test('S8: the x-ladder-attempts header carries pin= for keyed calls (and not for unkeyed)', async () => {
  const store = memoryStore(2);
  const post = (headers) => handle(new Request('https://l.test/v1/chat/completions', {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: 'Bearer t', ...headers },
    body: JSON.stringify({ model: 'service', messages: [{ role: 'user', content: 'hi' }] }),
  }), ENV, { store, fetchImpl: recFetch({}, []) });
  const keyed = await post({ 'x-session-affinity': 'conv-a' });
  assert.match(keyed.headers.get('x-ladder-attempts'), /pin=new/, 'first keyed turn reports pin=new');
  const hit = await post({ 'x-session-affinity': 'conv-a' });
  assert.match(hit.headers.get('x-ladder-attempts'), /pin=hit/, 'second keyed turn reports pin=hit');
  const unkeyed = await post({});
  assert.doesNotMatch(unkeyed.headers.get('x-ladder-attempts'), /pin=/, 'unkeyed callers see no pin status at all');
});

test('S8: GET /v1/state exposes pin stats alongside health and keys', async () => {
  const store = memoryStore(2);
  store.state.pins[Kh] = { rung: R0, lastUsedAt: Date.now() };
  const r = await handle(new Request('https://l.test/v1/state', {
    headers: { authorization: 'Bearer t' },
  }), ENV, { store });
  assert.equal(r.status, 200);
  const body = await r.json();
  assert.deepEqual(body.pins, { count: 1, byRung: { [R0]: 1 } });
  assert.ok(body.health !== undefined && body.keys !== undefined, 'existing state fields untouched');
});

// 2026-10-08: платный хвост мёртв на 402 (2833 отказа с 04.10), а `:free`-ранги едут на
// отдельном дневном разрешении — за сутки лестница отдала через них 348 ответов при потолке
// около 1000 (#86). Ни то, ни другое не было видно без консоли OpenRouter.
test('GET /v1/or-usage: лимит ключа и баланс аккаунта, ключ не попадает в ответ', async () => {
  const seen = [];
  const fetchImpl = async (url, init) => {
    seen.push({ url, auth: init.headers.Authorization });
    // как у реального fetch: читаем текст, а JSON разбираем сами (как это делает fetchOrUsage)
    if (url.endsWith('/auth/key')) {
      const body = JSON.stringify({ data: { usage: 12.5, limit: 50, is_free_tier: false, status: 'active' } });
      return { ok: true, status: 200, text: async () => body, json: async () => JSON.parse(body) };
    }
    const body = JSON.stringify({ data: { total_credits: 0, total_usage: 3.75 } });
    return { ok: true, status: 200, text: async () => body, json: async () => JSON.parse(body) };
  };
  const ENV2 = { LADDER_TOKEN: 't', OPENROUTER_API_KEY: 'sk-or-SECRET' };
  const r = await handle(new Request('https://l.test/v1/or-usage', { headers: { authorization: 'Bearer t' } }), ENV2, { fetchImpl });
  assert.equal(r.status, 200);
  const body = await r.json();
  assert.equal(seen.length, 2, 'оба запроса: ключ и баланс');
  assert.ok(seen.every((s) => s.auth === 'Bearer sk-or-SECRET'), 'ключ уходит только в заголовке провайдеру');
  assert.equal(body.key.data.limit, 50, 'лимит ключа дошёл');
  assert.equal(body.credits.data.total_credits, 0, 'баланс дошёл — на него смотрит платный хвост');
  assert.ok(!JSON.stringify(body).includes('sk-or-SECRET'), 'секрет никогда не сериализуется');
});

test('GET /v1/or-usage: без ключа и с ошибкой провайдера — ответ, а не исключение', async () => {
  const noKey = await handle(new Request('https://l.test/v1/or-usage', { headers: { authorization: 'Bearer t' } }), { LADDER_TOKEN: 't' }, { fetchImpl: async () => ({ ok: true, status: 200, json: async () => ({}) }) });
  assert.equal(noKey.status, 200, 'настроенного ключа нет — это состояние, а не сбой');
  assert.match((await noKey.json()).error, /OPENROUTER_API_KEY/);

  const failing = await handle(new Request('https://l.test/v1/or-usage', { headers: { authorization: 'Bearer t' } }), { LADDER_TOKEN: 't', OPENROUTER_API_KEY: 'k' }, {
    fetchImpl: async () => ({ ok: false, status: 503, text: async () => 'upstream down', json: async () => ({}) }),
  });
  const b = await failing.json();
  assert.equal(failing.status, 200);
  assert.match(b.key.error, /HTTP 503/, 'сломанный провайдер виден как поле, а не как падение');
});

test('S8: GET /v1/go-usage polls every pool key; the raw key never reaches the response (#91)', async () => {
  const seen = [];
  const fetchImpl = async (url, init) => {
    assert.match(url, /\/zen\/go\/v1\/usage$/, 'polls the Go usage endpoint');
    const key = init.headers.Authorization.replace('Bearer ', '');
    seen.push(key);
    return { ok: true, status: 200, json: async () => ({ usage: {
      rolling: { status: 'ok', percent: key === 'oc_a' ? 3 : 7, resetsAt: '2026-10-03T03:07:05Z' },
      weekly: { status: 'rate-limited', percent: 100, resetsAt: '2026-10-05T00:00:00Z' },
      monthly: { status: 'ok', percent: 50, resetsAt: '2026-11-02T19:07:56Z' },
    } }) };
  };
  const r = await handle(new Request('https://l.test/v1/go-usage', {
    headers: { authorization: 'Bearer t' },
  }), ENV, { fetchImpl });
  assert.equal(r.status, 200);
  const body = await r.json();
  assert.deepEqual(seen.sort(), ['oc_a', 'oc_b'], 'one poll per pool key');
  assert.equal(body.keys.length, 2);
  assert.deepEqual(body.keys[0], { keyIndex: 0, rolling: { status: 'ok', percent: 3, resetsAt: '2026-10-03T03:07:05Z' }, weekly: { status: 'rate-limited', percent: 100, resetsAt: '2026-10-05T00:00:00Z' }, monthly: { status: 'ok', percent: 50, resetsAt: '2026-11-02T19:07:56Z' } });
  const raw = JSON.stringify(body);
  assert.ok(!raw.includes('oc_a') && !raw.includes('oc_b'), 'keys are never serialized into the response');
});