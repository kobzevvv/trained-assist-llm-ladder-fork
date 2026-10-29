import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { run, parseJson, upstreamRequest, sanitizeAppSlug, sanitizeAppTitle, MIN_TOKENS, REASONING_MIN_TOKENS, REASONING_MODELS, minTokensFor, keyFaultOf, KEY_QUOTA_TTL_MS, KEY_WEEKLY_TTL_MS, APP_REFERER_BASE, DEFAULT_APP_SLUG, DEFAULT_APP_TITLE, resetFreeGoKeyCursor, rungsFor, ringWaitMs, RING_WAIT_MS, ringGuardRetry, orderBenchmarkedGoRungs, resetGoUsageMemo } from '../src/ladder.js';
import { estimateTokens } from '../src/size-policy.js';
import { handle } from '../src/handler.js';
import { memoryStore, backoffFor, rotateKey, emptyState, snapshot, resetKeys } from '../src/state.js';

const config = JSON.parse(fs.readFileSync(new URL('../config/ladders.json', import.meta.url)));
const LADDER = config.ladders.service.build; // canonical name since the 2026-10-03 refactor
const FREE = config.ladders.free.build;
const CONVERSATION = config.ladders.conversation.build;
// Minimal D1 double for the pool tables — the ladder tests never write to it, but
// the zen-rings rung calls prepare() before it knows the pool is cold.
function zenDbStub() {
  const stmt = {
    first: async () => null,
    all: async () => ({ results: [] }),
    run: async () => ({ meta: { changes: 0 } }),
  };
  return { prepare: () => ({ ...stmt, bind: () => stmt }), all: async () => ({ results: [] }) };
}

const env = { OPENCODE_GO_API_KEYS: 'oc_a,oc_b', OPENROUTER_API_KEY: 'or_key', ZEN_DB: zenDbStub() };

test('orderBenchmarkedGoRungs: context-specific speed chooses first model and retains fallback', () => {
  const rungs = ['opencode-go/incumbent-free', 'openrouter/free'];
  const rows = [
    { model_id: 'opencode-go/incumbent-free', available: 1, bench_status: 'ready', benchmark: { tiers: { small: { attempts: 2, successes: 2, p50_ms: 400 }, large: { attempts: 2, successes: 2, p50_ms: 500 } } } },
    { model_id: 'opencode-go/new-free', available: 1, bench_status: 'ready', benchmark: { tiers: { small: { attempts: 2, successes: 2, p50_ms: 200 }, large: { attempts: 2, successes: 2, p50_ms: 900 } } } },
    { model_id: 'opencode-go/flaky-free', available: 1, bench_status: 'ready', benchmark: { tiers: { small: { attempts: 2, successes: 1, p50_ms: 100 } } } },
  ];
  assert.deepEqual(orderBenchmarkedGoRungs(rungs, rows, 500), ['opencode-go/new-free', 'opencode-go/incumbent-free', 'openrouter/free']);
  assert.deepEqual(orderBenchmarkedGoRungs(rungs, rows, 20_000), ['opencode-go/incumbent-free', 'opencode-go/new-free', 'openrouter/free']);
});

// Behaviour tests run against a config with the zen-rings head removed: those rungs are cold in
// tests (no D1 rows, no workers), so every call would failover — and the GOLADDER[0]/GOFREE[0]
// indexes below address Go/OpenRouter rungs. Config-assertion tests keep the real `config`.
const noZen = (l) => l.filter((m) => !m.startsWith('zen-rings/'));
const GOCFG = { ...config, ladders: {} };
for (const [name, roles] of Object.entries(config.ladders)) {
  GOCFG.ladders[name] = Object.fromEntries(Object.entries(roles).map(([r, l]) => [r, noZen(l)]));
}
// Ключевые тесты (#45/#69, ротация ключей) проверяют поведение, когда ГОЛОВОЙ — платный Go:
// именно он сжигает ключи и паркуется. В продовых лестницах с 2026-10-07 голова — бесплатный
// Go (beam-free), который надбавку не ест, поэтому такие тесты получают свой порядок, а не
// привязку к тому, что стоит в конфиге сегодня.
const PAID_GO_HEAD = ['opencode-go/mimo-v2.6-flash', 'opencode-go/beam-free',
  'openrouter/nvidia/nemotron-3-super-120b-a12b:free'];
const PAID_FIRST = { ...GOCFG, ladders: { ...GOCFG.ladders, service: { ...GOCFG.ladders.service, build: PAID_GO_HEAD } } };
const GOLADDER = GOCFG.ladders.service.build;
const GOFREE = GOCFG.ladders.free.build;
const GOCONVERSATION = GOCFG.ladders.conversation.build;
const short = m => m.replace(/^opencode-go\/|^openrouter\//, '');

// behaviour[model](ctx) → { status, content, finish?, usage? } | 'throw'
function fakeFetch(behaviour, calls) {
  return async (url, init) => {
    const body = JSON.parse(init.body);
    const auth = init.headers.Authorization;
    calls.push({ url, model: body.model, auth, session: init.headers['x-opencode-session'], body });
    const r = (behaviour[body.model] || (() => ({ status: 200, content: 'ok' })))({ auth, body });
    if (r === 'throw') throw new Error('network down');
    const data = r.status === 200 ? { id: 'x', object: 'chat.completion', choices: [{ finish_reason: r.finish, message: { role: 'assistant', content: r.content } }], usage: r.usage || { prompt_tokens: 3 } } : null;
    return { ok: r.status === 200, status: r.status, json: async () => data, text: async () => r.error || '' };
  };
}
const msg = { model: 'service', messages: [{ role: 'user', content: 'hi' }] };

// Floor/diag assertions are pinned to EXPLICIT rung ids — never to GOLADDER[0]: #39 reordered the
// real service ladder (a free rung first) and that alone turned the previous version of
// these tests red (#40 → CI fail), and #67 reordered it again (mimo first). DIAG_REASONING is
// measured reasoning → 3000 floor; DIAG_SECOND exists only so the failover half of the diag tests
// has a stable second rung.
const DIAG_REASONING = 'opencode-go/mimo-v2.6-flash';
const DIAG_SECOND = 'opencode-go/deepseek-v4.1-flash';
const DIAG_CFG = { ...config, ladders: { ...config.ladders, service: { build: [DIAG_REASONING, DIAG_SECOND] } } }; // keyed by the canonical name

// Владелец 2026-10-08: первой платной Go сразу за zen поставили сначала Muse Spark 1.3
// Contributor ($0.10/$0.20 — самая дешёвая в прайсе), но она отдаёт `400 … This Go model trains
// on request data. Allow paid endpoints that train on request data in your workspace's Privacy
// settings` — нужен тумблер в консоли OpenCode. Решение владельца: «ну давай тогда
// mimo-v2.6-flash, раз так сложно». MUSE остаётся константой только ради проверки ОТСУТСТВИЯ.
const MUSE = 'opencode-go/muse-spark-1.3-contributor';
const MIMO = 'opencode-go/mimo-v2.6-flash';
// Бесплатный ранг с окном 1M: единственный, куда влезает жирная сессия opencode (замер 08.10 —
// payload дорос до 788 КБ ≈ 197K токенов, а потолок Go 100K, зен 50/20 КБ, у sante окно 262K
// и он один и раз в несколько минут уходит в 429-пропуск). Без такого ранга лестница отвечает
// `every rung failed` в 0.5 с, и вызывающий ретраит впустую.
const BIG = 'openrouter/nvidia/nemotron-3-ultra-550b-a55b:free';
// Второе большое окно: 512K, probe=ok, уже живёт в service/free. Замер 2026-10-08: у сессии
// payload дошёл до 298 988 реальных токенов (окно sante 262 144 не берёт), а 1M-ранг в тот же
// момент отдавал `503 Service temporarily overloaded` → 5-мин пропуск → `every rung failed`.
// Оба проверены вживую на 1 070 159 байта: dots 12.4 с, 1M 10.6 с.
const DOTS = 'openrouter/dots-studio/dots-3-note-preview:free';

test('config: service = free Go → free OpenRouter → paid Go, без zen (#329: кольцо retired)', () => {
  // Порядок: бесплатный Go, пять бесплатных OpenRouter, платный Go. zen-rings вырезан (#329):
  // анонимный free-доступ убит апстримом (429 для всех IP), один чистый фиксированный IP дал бы
  // ~1000/модель/день — копейки для нашего трафика (решение владельца).
  assert.deepEqual(LADDER, [
    'opencode-go/beam-free',
    'openrouter/nvidia/nemotron-3-super-120b-a12b:free',
    'openrouter/nvidia/nemotron-3-ultra-550b-a55b:free',
    'openrouter/cohere/north-mini-code:free',
    'openrouter/dots-studio/dots-3-note-preview:free',
    'openrouter/nvidia/nemotron-3-nano-omni-30b-a3b-reasoning:free',
    'opencode-go/mimo-v2.6-flash',
  ]);
  assert.ok(!LADDER.some(m => m.startsWith('zen-rings/')), 'ни одного zen-ранга в service (#329)');
  for (const role of ['build', 'plan', 'explore', 'general', 'review']) {
    assert.deepEqual(config.ladders.service[role], LADDER, role);
  }
  // Владелец 2026-10-08: «на OR нет денег — платный должен быть на Go». Каждый платный ранг
  // OpenRouter отвечал 402 и сжигал хоп, поэтому из НЕ-ФРИ лестниц они убраны целиком.
  const paid = m => m.startsWith('openrouter/') && !m.endsWith(':free');
  assert.equal(LADDER.filter(paid).length, 0, 'ни одного платного ранга OpenRouter — деньги на Go');
  assert.ok(LADDER.filter(m => m.startsWith('openrouter/') && m.endsWith(':free')).length === 5,
    'пять бесплатных OpenRouter стоят перед платным хвостом (#36; sante убран — мёртвый слаг, #318)');
  // Владелец 2026-10-08: «после zen сделать первой платной Go самую дешёвую». Muse (400 Privacy
  // settings) убрана → первой идёт mimo $0.28, платный сегмент от дешёвых к дорогим.
  assert.equal(LADDER[LADDER.length - 1], MIMO, 'mimo — последняя платная Go, после всех бесплатных');
  assert.ok(!LADDER.includes(MUSE), 'Muse убрана из лестницы: 400 trains on request data (Privacy settings)');
  // Инвариант: все бесплатные Go/zen/OpenRouter стоят перед платным Go.
  const firstGoPaid = LADDER.findIndex(m => m.startsWith('opencode-go/') && !m.endsWith('-free'));
  const lastFree = Math.max(
    LADDER.findLastIndex(m => m.startsWith('opencode-go/') && m.endsWith('-free')),
    LADDER.findLastIndex(m => m.endsWith(':free')),
  );
  assert.ok(firstGoPaid > lastFree, `бесплатный сегмент целиком перед платным Go (последний free=${lastFree}, первый paid=${firstGoPaid})`);
  // Бесплатный Go — первый.
  assert.equal(LADDER[0], 'opencode-go/beam-free', 'бесплатный Go открывает лестницу');
  assert.deepEqual(LADDER.slice(1, 3),
    ['openrouter/nvidia/nemotron-3-super-120b-a12b:free', 'openrouter/nvidia/nemotron-3-ultra-550b-a55b:free'],
    'за ним сразу бесплатный OpenRouter (zen вырезан — #329)');
});

// #42 → #329 (2026-10-28): zen вырезан из free целиком — анонимный free-доступ убит апстримом
// (429 FreeUsageLimitError с любого egress: residential, GHA Azure, CF-коло); лимит ~1000/модель/
// день на чистый фиксированный IP — копейки (решение владельца). free остаётся жёстким $0.
test('config: free = 6 $0 rungs (Go head + OR ×5), без zen (#329)', () => {
  assert.deepEqual(FREE, [
    'opencode-go/beam-free',
    'openrouter/nvidia/nemotron-3-super-120b-a12b:free',
    'openrouter/nvidia/nemotron-3-ultra-550b-a55b:free',
    'openrouter/cohere/north-mini-code:free',
    'openrouter/dots-studio/dots-3-note-preview:free',
    'openrouter/nvidia/nemotron-3-nano-omni-30b-a3b-reasoning:free',
  ]);
  assert.equal(FREE.length, 6, '6 $0 rungs: beam-free + OR ×5 (nemotron-ultra и sante убраны — мёртвые слаги, #318)');
  assert.ok(FREE.every(m => m.endsWith('-free') || m.endsWith(':free')),
    'every free rung is $0 — no Go subscription, no paid OpenRouter');
  assert.ok(!FREE.some(m => m.startsWith('zen-rings/')), 'zen-rings вырезан (#329)');
  assert.equal(FREE[0], 'opencode-go/beam-free', 'the working head is free Go');
  assert.deepEqual(config.ladders.free, { build: FREE }, 'free is the build-role ladder');
});

test('config: aliases — only the legacy deepseek alias remains, resolving to build', () => {
  // One name, one ladder — but the legacy `deepseek` name still reaches stored opencode
  // profiles, so it resolves on READ to the build ladder (owner 2026-10-05).
  assert.deepEqual(config.aliases, { deepseek: 'build' });
  for (const gone of ['cheap', 'free-ladder', 'conversations', 'picture', 'picture advanced', 'free_100percent']) {
    assert.equal(config.ladders[gone], undefined, `${gone} is not a ladder`);
    assert.equal(rungsFor(config, gone), null, `${gone} must NOT resolve`);
  }
  assert.deepEqual(rungsFor(config, 'deepseek'), config.ladders.build.build, 'deepseek resolves to build');
  for (const keep of ['service', 'conversation', 'vision', 'vision advanced', 'free', 'build', 'build advanced', 'plan', 'explore', 'general', 'review', 'doctor', 'research']) {
    assert.ok(config.ladders[keep], `${keep} exists`);
  }
});

test('first Go rung answers; Go gets the session header, non-stream, reasoning-safe max_tokens', async () => {
  const calls = [];
  const r = await run({ ...msg, max_tokens: 5 }, { env, config: GOCFG, store: memoryStore(2), fetchImpl: fakeFetch({}, calls) });
  assert.equal(r.ok, true);
  assert.equal(r.model, GOLADDER[0]);
  assert.match(calls[0].url, /opencode\.ai\/zen\/go\/v1\/chat\/completions$/);
  assert.ok(calls[0].session);
  assert.equal(calls[0].body.stream, false);
  // #38: the caller's 5 is raised to THIS rung's floor — assert the clamp logic, not the identity
  // of whatever the ladder orders first (identity is test 'reasoning-модель получает 3000…').
  assert.equal(calls[0].body.max_tokens, minTokensFor(GOLADDER[0]), 'floor follows the first rung class');
  assert.ok(calls[0].body.max_tokens >= MIN_TOKENS, 'caller max_tokens=5 is raised to at least the common floor');
  assert.equal(calls[0].auth, 'Bearer oc_a');
});

test('failing rung → next rung; the failed one is skipped on the next call', async () => {
  const store = memoryStore(2);
  const beh = { [short(GOLADDER[0])]: () => ({ status: 500, error: 'boom' }) };
  const r = await run(msg, { env, config: GOCFG, store, fetchImpl: fakeFetch(beh, []) });
  assert.equal(r.model, GOLADDER[1]);
  const calls = [];
  await run(msg, { env, config: GOCFG, store, fetchImpl: fakeFetch(beh, calls) });
  assert.equal(calls[0].model, short(GOLADDER[1]));
});

test('json guard: non-JSON fails the rung, fenced JSON is accepted', async () => {
  const beh = {
    [short(GOLADDER[0])]: () => ({ status: 200, content: 'sure thing' }),
    [short(GOLADDER[1])]: () => ({ status: 200, content: '```json\n{"kind":"none"}\n```' }),
  };
  const r = await run({ ...msg, response_format: { type: 'json_object' } }, { env, config: GOCFG, store: memoryStore(2), fetchImpl: fakeFetch(beh, []) });
  assert.equal(r.model, GOLADDER[1]);
  assert.deepEqual(parseJson(r.content), { kind: 'none' });
});

// ── #34: guard failures carry finish_reason + usage so chronic empty answers are diagnosable ───
// usage mirrors the real failure at the #38 floor: reasoning ate all 3000 tokens, content empty.
const EMPTY_USAGE = { prompt_tokens: 840, completion_tokens: 3000, completion_tokens_details: { reasoning_tokens: 3000 } };
const EMPTY_DIAG = 'finish=length, out=3000, reasoning=3000, prompt=840, max_tokens=3000';

test('empty answer error carries finish_reason + usage — reasoning eating the floor is visible', async () => {
  const beh = { [short(DIAG_REASONING)]: () => ({ status: 200, content: '', finish: 'length', usage: EMPTY_USAGE }) };
  const r = await run(msg, { env, config: DIAG_CFG, store: memoryStore(2), fetchImpl: fakeFetch(beh, []) });
  assert.equal(r.ok, true, 'the ladder still fails over to the next rung');
  const err = r.attempts.find(a => a.outcome === 'error').error;
  assert.ok(err.startsWith(`empty answer (${EMPTY_DIAG})`), `prefix match on the diag, got: ${err}`);
});

test('invalid JSON error carries finish_reason + usage too (#34)', async () => {
  const beh = {
    [short(DIAG_REASONING)]: () => ({ status: 200, content: 'not json', finish: 'length', usage: EMPTY_USAGE }),
    [short(DIAG_SECOND)]: () => ({ status: 200, content: '```json\n{"ok":true}\n```' }),
  };
  const r = await run({ ...msg, response_format: { type: 'json_object' } }, { env, config: DIAG_CFG, store: memoryStore(2), fetchImpl: fakeFetch(beh, []) });
  assert.equal(r.ok, true);
  const err = r.attempts.find(a => a.outcome === 'error').error;
  assert.ok(err.startsWith(`invalid JSON (${EMPTY_DIAG})`), `prefix match on the diag, got: ${err}`);
});

test('#34: diag token counts are never read as key faults or quota — instrumentation cannot move the ladder', async () => {
  // usage numbers chosen to collide with the classifiers on purpose: 429/401/503
  const usage = { prompt_tokens: 429, completion_tokens: 401, completion_tokens_details: { reasoning_tokens: 503 } };
  const beh = { [short(DIAG_REASONING)]: () => ({ status: 200, content: '', finish: 'length', usage }) };
  const store = memoryStore(2);
  const r = await run(msg, { env, config: DIAG_CFG, store, fetchImpl: fakeFetch(beh, []) });
  assert.ok(r.attempts.find(a => a.outcome === 'error').error.includes('prompt=429'), 'the numbers are still logged');
  assert.equal(r.attempts.filter(a => a.outcome === 'key-rotated').length, 0, 'no key rotation from usage numbers');
  assert.equal(store.state.keys.active, 0, 'key state untouched');
  assert.equal(r.attempts.filter(a => a.outcome === 'key-probe').length, 1, 'still exactly one non-key probe');
  assert.equal(store.state.health[DIAG_REASONING].class, 'transient', 'empty answer stays transient, not quota');
});

// ── #45: a completed-but-guard-failed answer gets ONE same-rung retry before the ladder moves ──
test('guard-fail → one same-rung retry: recovery keeps the rung (no hop, no health-skip), a second fail moves down', async () => {
  // flake #1 only: the retry answers → the rung keeps serving, nothing is recorded against it
  let calls = 0;
  const store = memoryStore(2);
  const beh = { [short(GOLADDER[0])]: () => (++calls === 1
    ? { status: 200, content: '', finish: 'length', usage: EMPTY_USAGE }
    : { status: 200, content: '{"ok":true}' }) };
  const r = await run({ ...msg, response_format: { type: 'json_object' } }, { env, config: GOCFG, store, fetchImpl: fakeFetch(beh, []) });
  assert.equal(r.ok, true);
  assert.equal(r.model, GOLADDER[0], 'retried rung answered — no hop down the ladder');
  assert.ok(r.attempts.some(a => a.outcome === 'guard-retry'), 'the retry is visible in attempts (flake rate stays measurable)');
  assert.ok(!r.attempts.some(a => a.outcome === 'error'), 'a recovered flake never surfaces as an error');
  assert.equal(store.state.health[GOLADDER[0]], undefined, 'no health-skip — one flake must not punish other callers');

  // flake #2 both attempts empty → exactly one retry, then the ladder descends and the failure IS recorded
  const store2 = memoryStore(2);
  const beh2 = {
    [short(GOLADDER[0])]: () => ({ status: 200, content: '', finish: 'length', usage: EMPTY_USAGE }),
    [short(GOLADDER[1])]: () => ({ status: 200, content: '{"ok":true}' }),
  };
  const r2 = await run({ ...msg, response_format: { type: 'json_object' } }, { env, config: GOCFG, store: store2, fetchImpl: fakeFetch(beh2, []) });
  assert.equal(r2.ok, true);
  assert.equal(r2.model, GOLADDER[1], 'a persistent guard-fail still moves down the ladder');
  assert.equal(r2.attempts.filter(a => a.outcome === 'guard-retry').length, 1, 'exactly one retry per rung');
  assert.ok(r2.attempts.some(a => a.outcome === 'error'), 'the rung still reports the failure');
  assert.ok(store2.state.health[GOLADDER[0]], 'persistent guard-fail is recorded in shared health');
});

// ── #38: the max_tokens floor is per-rung — 3000 for the empirical REASONING_MODELS list, 1500 for the rest ─
test('reasoning-модель получает 3000, обычная — 1500', async () => {
  // every rung of the empirical list clamps to REASONING_MIN_TOKENS (caller asking for less is raised)
  // zen rungs build their request only with the relay token present (keyless → null), so give it one
  const envZ = { ...env, OPENCODE_ZEN_RELAY_TOKEN: 'zr_t' };
  for (const m of REASONING_MODELS) {
    assert.equal(upstreamRequest(envZ, m, { messages: [] }, 0).body.max_tokens, REASONING_MIN_TOKENS, m);
  }
  // #42: the four zen rungs are IN the list — the loop above only proves the list clamps, so pin
  // the zen ids explicitly; dropping one must fail here, not in prod
  for (const z of ['opencode-zen/mimo-v2.6-flash-free', 'opencode-zen/mimo-v2.5-free', 'opencode-zen/nemotron-3.5-lightning-free', 'opencode-zen/big-pickle']) {
    assert.ok(REASONING_MODELS.includes(z), `${z} must stay in REASONING_MODELS`);
    assert.equal(minTokensFor(z), REASONING_MIN_TOKENS, z);
    assert.equal(upstreamRequest(envZ, z, { messages: [] }, 0).body.max_tokens, REASONING_MIN_TOKENS, z);
  }
  // a rung outside the list keeps the common floor
  const plain = 'openrouter/google/gemini-2.5-flash-lite';
  assert.ok(!REASONING_MODELS.includes(plain));
  assert.equal(upstreamRequest(env, plain, { messages: [] }, 0).body.max_tokens, MIN_TOKENS);
  assert.equal(minTokensFor(plain), MIN_TOKENS);
  // a caller-supplied max_tokens higher than the rung floor still wins
  assert.equal(upstreamRequest(env, plain, { messages: [], max_tokens: 4000 }, 0).body.max_tokens, 4000);
  assert.equal(upstreamRequest(env, 'opencode-go/mimo-v2.6-flash', { messages: [], max_tokens: 4000 }, 0).body.max_tokens, 4000);

  // the #34 guard diagnostic prints the floor that ACTUALLY went upstream, per rung class
  const reasoningRung = await run(msg, { env, config: DIAG_CFG, store: memoryStore(2), fetchImpl: fakeFetch({ [short(DIAG_REASONING)]: () => ({ status: 200, content: '', finish: 'length', usage: EMPTY_USAGE }) }, []) });
  const rErr = reasoningRung.attempts.find(a => a.outcome === 'error').error;
  assert.ok(rErr.includes('max_tokens=3000'), `reasoning rung → max_tokens=3000 in the diag, got: ${rErr}`);
  const plainRung = await run({ ...msg, model: 'vision' }, {
    env, config: GOCFG, store: memoryStore(2), pinRung: plain,
    fetchImpl: fakeFetch({ 'google/gemini-2.5-flash-lite': () => ({ status: 200, content: '', finish: 'length', usage: { prompt_tokens: 9, completion_tokens: 1500 } }) }, []),
  });
  const pErr = plainRung.attempts.find(a => a.outcome === 'error').error;
  assert.ok(pErr.includes('max_tokens=1500'), `plain rung → max_tokens=1500 in the diag, got: ${pErr}`);
});

test('Go key limit → rotate to spare key, retry SAME rung', async () => {
  const calls = [];
  const beh = { [short(PAID_GO_HEAD[0])]: ({ auth }) => (auth === 'Bearer oc_a' ? { status: 429, error: 'Go usage limit exceeded' } : { status: 200, content: 'ok' }) };
  const store = memoryStore(2);
  const r = await run(msg, { env, config: PAID_FIRST, store, fetchImpl: fakeFetch(beh, calls) });
  assert.equal(r.model, PAID_GO_HEAD[0]);
  assert.deepEqual(calls.map(c => c.auth), ['Bearer oc_a', 'Bearer oc_b']);
  assert.equal(store.state.keys.active, 1, 'next call starts on the spare key');
  assert.equal(r.attempts.find(a => a.outcome === 'key-rotated').key, 0, 'attempts name the key that failed');
});

test('spare key answers a non-key failure → the call STAYS on the same Go rung; fast rejections walk the pool per rung (#311)', async () => {
  // Key A is unhealthy for this model for a reason that never matches the quota/401 patterns
  // (silent throttle looks like a flaky model) — the probe keeps the call on Go instead of paying
  // for OpenRouter and resetting the caller's prompt cache.
  const store = memoryStore(2);
  const beh = { [short(PAID_GO_HEAD[0])]: ({ auth }) => (auth === 'Bearer oc_a' ? { status: 500, error: 'boom' } : { status: 200, content: 'ok' }) };
  const calls = [];
  const r = await run(msg, { env, config: PAID_FIRST, store, fetchImpl: fakeFetch(beh, calls) });
  assert.equal(r.ok, true);
  assert.equal(r.model, PAID_GO_HEAD[0]);
  assert.deepEqual(calls.map(c => c.auth), ['Bearer oc_a', 'Bearer oc_b']);
  assert.equal(store.state.keys.active, 0, 'a probe is local — shared rotation state moves only on quota/401');
  assert.equal(r.attempts.filter(a => a.outcome === 'key-probe').length, 1);

  // FAST rejections cost milliseconds, so the probe budget is PER RUNG (#311): rung0 fails on
  // BOTH keys, and rung1 still gets its own probe — the call finds oc_b working and stays on Go
  // instead of hopping to OpenRouter because the call-wide budget was already burnt.
  const beh2 = {
    [short(PAID_GO_HEAD[0])]: () => ({ status: 500, error: 'boom' }),
    [short(PAID_GO_HEAD[1])]: ({ auth }) => (auth === 'Bearer oc_a' ? { status: 500, error: 'boom' } : { status: 200, content: 'ok' }),
  };
  const calls2 = [];
  const r2 = await run(msg, { env, config: PAID_FIRST, store: memoryStore(2), fetchImpl: fakeFetch(beh2, calls2) });
  assert.equal(r2.attempts.filter(a => a.outcome === 'key-probe').length, 2, 'one fast probe per failing rung — the budget is per rung, not per call');
  assert.equal(r2.model, PAID_GO_HEAD[1], 'rung1 gets its own probe and the ladder stays on Go');
  assert.ok(calls2.every(c => c.url.includes('opencode.ai')), 'the ladder never reaches OpenRouter');
});

test('#311: a dead active key cannot hide the healthy one — a fast 403 walks the whole usable pool', async () => {
  // The 2026-10-27 incident: active sat on the key that 403s everything, the single per-call
  // probe was spent on the NEXT dead key (monthly-capped), recordFailure fired the exponential
  // backoff — and the Go floor went dark for everyone with a healthy third key in the pool.
  const env3 = { ...env, OPENCODE_GO_API_KEYS: 'oc_a,oc_b,oc_c' };
  const store = memoryStore(3);
  const beh = {
    [short(PAID_GO_HEAD[0])]: ({ auth }) => (auth === 'Bearer oc_c' ? { status: 200, content: 'ok' } : { status: 403, error: 'Forbidden' }),
  };
  const calls = [];
  const r = await run(msg, { env: env3, config: PAID_FIRST, store, fetchImpl: fakeFetch(beh, calls) });
  assert.equal(r.ok, true, 'the healthy third key answers');
  assert.equal(r.model, PAID_GO_HEAD[0], 'on the FIRST rung — no ladder walk, no OpenRouter');
  assert.deepEqual(calls.map(c => c.auth), ['Bearer oc_a', 'Bearer oc_b', 'Bearer oc_c'], 'both dead keys are tried before the healthy one');
  assert.equal(r.attempts.filter(a => a.outcome === 'key-probe').length, 2, 'two fast probes for the two dead keys');
  // #316: a healthy sibling disproved both 403s → the refusing keys are parked (quota TTL) and
  // `active` lands on the healthy one, so the NEXT call starts there without the 403 tax.
  // Rung health stays untouched either way: a key problem never becomes a rung backoff.
  assert.ok(store.state.keys.exhausted[0] > Date.now(), 'the refusing start key is parked');
  assert.ok(store.state.keys.exhausted[1] > Date.now(), 'the second refusing key is parked');
  assert.equal(store.state.keys.active, 2, 'active moved to the healthy key');
  assert.deepEqual(store.state.health, {}, 'the rung never fails — no exponential backoff from a key problem');
});

test('#311: a SLOW failure keeps the one-probe-per-call bound — a Go outage cannot multiply timeouts', async () => {
  const env3 = { ...env, OPENCODE_GO_API_KEYS: 'oc_a,oc_b,oc_c' };
  const beh = { [short(PAID_GO_HEAD[0])]: 'throw', [short(PAID_GO_HEAD[1])]: 'throw' };
  const calls = [];
  const r = await run(msg, { env: env3, config: PAID_FIRST, store: memoryStore(3), fetchImpl: fakeFetch(beh, calls) });
  assert.equal(r.attempts.filter(a => a.outcome === 'key-probe').length, 1, 'exactly one slow probe for the WHOLE call');
  const goCalls = calls.filter(c => c.url.includes('opencode.ai'));
  assert.equal(goCalls.length, 3, 'rung0 probes once (a+b), rung1 gets an attempt but no probe');
  assert.equal(r.model, PAID_GO_HEAD[2], 'the ladder still fails over to OpenRouter');
});

test('#316: a fast 403 disproved by the spare key parks the refusing key and moves active off it', async () => {
  const store = memoryStore(2);
  const beh = {
    [short(PAID_GO_HEAD[0])]: ({ auth }) => (auth === 'Bearer oc_a' ? { status: 403, error: 'Forbidden' } : { status: 200, content: 'ok' }),
  };
  const calls = [];
  const r = await run(msg, { env, config: PAID_FIRST, store, fetchImpl: fakeFetch(beh, calls) });
  assert.equal(r.ok, true, 'the spare key serves the very same payload');
  assert.equal(r.model, PAID_GO_HEAD[0]);
  assert.deepEqual(calls.map(c => c.auth), ['Bearer oc_a', 'Bearer oc_b']);
  assert.ok(store.state.keys.exhausted[0] > Date.now(), 'the refusing key is parked for the quota TTL');
  assert.equal(store.state.keys.active, 1, 'active left the dead key — the next call starts healthy');
  assert.deepEqual(store.state.health, {}, 'a key problem never becomes rung health');
});

test('#316: a SLOW failure a spare recovers parks nothing — a timeout is not key evidence', async () => {
  const store = memoryStore(2);
  const beh = {
    [short(PAID_GO_HEAD[0])]: ({ auth }) => (auth === 'Bearer oc_a' ? 'throw' : { status: 200, content: 'ok' }),
  };
  const calls = [];
  const r = await run(msg, { env, config: PAID_FIRST, store, fetchImpl: fakeFetch(beh, calls) });
  assert.equal(r.ok, true, 'the spare answers');
  assert.equal(store.state.keys.exhausted[0], undefined, 'network flake → no park');
  assert.equal(store.state.keys.active, 0, 'shared rotation state untouched');
  assert.deepEqual(store.state.health, {});
});

test('#316: a paid rung starts OFF the monthly-capped key — /usage says cap, a cap is not a fault', async () => {
  resetGoUsageMemo(); // the module memo is shared by every test in this file
  const env3 = { ...env, OPENCODE_GO_API_KEYS: 'oc_a,oc_b,oc_c' };
  const store = memoryStore(3); // active = 0 = oc_a = the capped key
  const beh = { [short(PAID_GO_HEAD[0])]: () => ({ status: 200, content: 'ok' }) };
  const calls = [];
  const chat = fakeFetch(beh, calls);
  const fetchImpl = async (url, init) => {
    if (String(url).endsWith('/usage')) {
      const auth = init.headers.Authorization;
      const monthly = auth === 'Bearer oc_a'
        ? { status: 'rate-limited', percent: 100, resetsAt: '2026-11-02T00:00:00Z' }
        : { status: 'ok', percent: 5, resetsAt: '2026-11-03T00:00:00Z' };
      return {
        ok: true, status: 200,
        json: async () => ({ usage: {
          rolling: { status: 'ok', percent: 0, resetsAt: 'x' },
          weekly: { status: 'ok', percent: 0, resetsAt: 'y' },
          monthly,
        } }),
        text: async () => '',
      };
    }
    return chat(url, init);
  };
  const r = await run(msg, { env: env3, config: PAID_FIRST, store, fetchImpl });
  assert.equal(r.ok, true, 'the paid rung answers on the uncapped key');
  assert.deepEqual(calls.map(c => c.auth), ['Bearer oc_b'], 'no request on the capped key — straight to the first uncapped one');
  assert.deepEqual(store.state.keys.exhausted, {}, 'a cap is not a fault — nothing is parked');
  assert.equal(store.state.keys.active, 0, 'shared active untouched — free rungs still ride the capped key on purpose');
  resetGoUsageMemo(); // do not leak the capped answer into the next test
});

test('context overflow on a Go rung does NOT probe the spare key — the key cannot change it', async () => {
  resetFreeGoKeyCursor();
  const beh = { [short(GOLADDER[0])]: () => ({ status: 400, error: 'This request exceeds the context window of the model' }) };
  const calls = [];
  const r = await run(msg, { env, config: GOCFG, store: memoryStore(2), fetchImpl: fakeFetch(beh, calls) });
  assert.equal(r.attempts.filter(a => a.outcome === 'key-probe').length, 0);
  // In the new ladder: free Go (oc_a) → free OR (or_key) — the key changes when moving from Go to OR.
  // With pool oc_a, oc_b, the first attempt uses oc_a (key index 0).
  assert.equal(calls.length, 2, 'the rung is tried on the active key, then moves on');
  // The next rung is free OR, which uses a different auth header.
  assert.equal(calls[0].auth, 'Bearer oc_a', 'first attempt uses Go key');
  assert.equal(calls[1].auth, 'Bearer or_key', 'second attempt uses OR key');
  assert.equal(r.model, GOLADDER[1]);
});

test('a WEEKLY Go allowance parks the key for hours; a plain rate-limit hit keeps the15-minute TTL', () => {
  const weekly = 'HTTP 429: {"type":"error","error":{"type":"GoUsageLimitError","message":"Go usage limit exceeded"},'
    + '"metadata":{"workspace":"wrk_01KN4","limitName":"weekly"}}';
  assert.equal(keyFaultOf(weekly).ttlMs, KEY_WEEKLY_TTL_MS);
  assert.equal(keyFaultOf('HTTP 429: Go usage limit exceeded').ttlMs, KEY_QUOTA_TTL_MS);
  assert.equal(keyFaultOf('HTTP 401: invalid api key').dead, true);
});

// ── size gate: a rung whose window is smaller than the request is skipped, not failed ─────────
// Measured 2026-10-07: opencode-go takes ~98K tokens, and at ~123K answers `429 Endpoint is
// unavailable` while rotating through every key — 48 s and three keys for no answer. Refusing
// before the attempt is the whole point, and refusing must NOT poison the rung's health: the
// request is wrong for it, the model is fine (same rule as a context overflow).
test('input above a rung ceiling skips it — no attempt, no health failure', async () => {
  const calls = [];
  const store = memoryStore(2);
  // ~935K токенов по нашей оценке — за новым потолком 900K для opencode-go
  const fat = { ...msg, messages: [{ role: 'user', content: 'x'.repeat(3_400_001) }] };
  const r = await run(fat, { env, config: GOCFG, store, fetchImpl: fakeFetch({}, calls) });

  assert.equal(r.ok, true, 'the ladder still answers from a rung that fits');
  const skipped = r.attempts.filter(a => a.outcome === 'skipped' && /above the rung ceiling/.test(a.error || ''));
  // With the new order: free Go (beam-free) is first, free OR models follow, paid Go is at the end.
  // The fat input skips beam-free, then the ladder proceeds to free OR models which answer.
  assert.ok(skipped.length >= 1, `at least the opencode-go free rung is skipped, got ${JSON.stringify(r.attempts)}`);
  assert.ok(skipped.every(a => a.model.startsWith('opencode-go/')), 'only rungs with a measured ceiling are refused');
  assert.equal(calls.filter(c => c.url.includes('opencode.ai')).length, 0, 'the Go gateway is never called');
  assert.equal(store.state.health['opencode-go/beam-free'], undefined,
    'a ceiling skip is not a failure — the rung stays healthy for every other caller');
});

test('an account with no payment (no tariff) is rotated instantly — no waiting', () => {
  // The error returns immediately (no timeout), so the next key can serve the call.
  // TTL is short (same as rate-limit) so the account is retried once payment is added.
  assert.equal(keyFaultOf('HTTP 402: insufficient account funds').ttlMs, KEY_QUOTA_TTL_MS);
  assert.equal(keyFaultOf('HTTP 403: subscription required').ttlMs, KEY_QUOTA_TTL_MS);
  assert.equal(keyFaultOf('subscription required for this model').ttlMs, KEY_QUOTA_TTL_MS);
});

// ── гонка: одна и та же бесплатная Go-модель на двух разных аккаунтах ─────────────────────────
// «два запуска одной модели на разных аккаунтах го» (владелец, 2026-10-07). Четыре ключа,
// бесплатный тир без надбавки — параллельный зов стоит только времени, а один тормозящий или
// залимиченный ключ перестаёт решать исход.
test('mid-size prompt races the same free Go rung on two accounts — the answer wins, nothing is poisoned', async () => {
  const calls = [];
  // один аккаунт отдаёт 500, второй отвечает — гонка обязана выиграть вторым
  const beh = { [short(GOLADDER[0])]: ({ auth }) => (auth === 'Bearer oc_a' ? { status: 500, error: 'boom' } : { status: 200, content: 'ok' }) };
  const store = memoryStore(2);
  const mid = { ...msg, messages: [{ role: 'user', content: 'x'.repeat(9000) }] };
  const r = await run(mid, { env, config: GOCFG, store, fetchImpl: fakeFetch(beh, calls) });

  assert.equal(r.ok, true, 'the healthy account answers even though the other one failed');
  assert.equal(r.model, GOLADDER[0], 'and it is the SAME rung — не фоллбэк, а гонка внутри ранга');
  assert.equal(calls.length, 2, 'оба ключа запущены одновременно');
  assert.equal(new Set(calls.map(c => c.auth)).size, 2, 'и это два разных аккаунта');
  assert.equal(store.state.health[GOLADDER[0]], undefined, 'проигравший ключ не портит здоровье ранга');
  assert.equal(store.state.keys.active, 0, 'гонка — не ротация: общее состояние ключей не тронуто');
  assert.equal(r.attempts.filter(a => a.outcome === 'error').length, 0, 'логическая попытка одна, а не два отказа');
});

test('a small prompt does NOT race — one account, one attempt', async () => {
  const calls = [];
  const beh = { [short(GOLADDER[0])]: () => ({ status: 200, content: 'ok' }) };
  const store = memoryStore(2);
  const r = await run(msg, { env, config: GOCFG, store, fetchImpl: fakeFetch(beh, calls) });
  assert.equal(r.ok, true);
  assert.equal(calls.length, 1, '<2K → hedgePlan.count = 1, гонки нет');
  assert.equal(r.model, GOLADDER[0]);
});

// #69: the weekly limit must NOT take the free Go rungs down with the paid ones — they don't
// consume the allowance, and the incident is exactly when the free head has to keep serving.
test('every key limited → paid Go rungs parked, free Go rungs keep serving (#69)', async () => {
  const beh = {};
  for (const m of PAID_GO_HEAD) if (m.startsWith('opencode-go/') && !m.endsWith('-free')) beh[short(m)] = () => ({ status: 429, error: 'usage limit' });
  const store = memoryStore(2);
  const calls = [];
  const r = await run(msg, { env, config: PAID_FIRST, store, fetchImpl: fakeFetch(beh, calls) });
  // платный Go в голове жжёт оба ключа → паркуется,
  // yet the free Go rung right behind it answers in the SAME call
  assert.equal(r.model, 'opencode-go/beam-free', 'free Go serves while every key is limited');
  assert.ok(store.state.health['opencode-go/mimo-v2.6-flash'], 'paid Go rung is parked');
  assert.equal(store.state.health['opencode-go/beam-free'], undefined, 'free Go rung is never parked');
  assert.deepEqual(calls.filter(c => c.url.includes('opencode.ai')).map(c => c.model),
    ['mimo-v2.6-flash', 'mimo-v2.6-flash', 'beam-free'],
    'paid rung once per key, then the free rung on the last key');
  // next call: paid Go still parked → the free rung is the head that answers
  const calls2 = [];
  const r2 = await run(msg, { env, config: PAID_FIRST, store, fetchImpl: fakeFetch({}, calls2) });
  assert.equal(r2.model, 'opencode-go/beam-free');
  // окно истекло → платный Go снова в голове, без ручных шагов.
  // Чистим ОБЕ карты: с #341 лимит платного ранга живёт в `paid`, а не в `exhausted`.
  for (const m of Object.keys(store.state.health)) store.state.health[m].skipUntil = Date.now() - 1;
  for (const map of [store.state.keys.exhausted, store.state.keys.paid || {}]) {
    for (const k of Object.keys(map)) map[k] = Date.now() - 1;
  }
  const calls3 = [];
  const r3 = await run(msg, { env, config: GOCFG, store, fetchImpl: fakeFetch({}, calls3) });
  assert.equal(r3.model, GOLADDER[0]);
});

// #341: пауза ключа, поставленная ПЛАТНЫМ рангом, не должна отнимать у бесплатного обход пула.
// Инцидент 2026-10-28 23:05–23:45Z: 429 давали ТОЛЬКО mimo-v2.6-flash (75 + 2 key-rotated) и
// deepseek-v4-pro (36), у beam-free — ни одного 429, и при этом 61 его вызов отвечал. Общий
// exhausted после 429 от платного закрывал spare-обход, и бесплатному оставалась ровно одна
// попытка на уже припаркованном ключе: pool.findIndex(!exhaustedAtStart) → -1 → break.
test('платный 429 сажает ключ только для платных рангов — бесплатный обходит весь пул (#341)', async () => {
  const cfg = { ...GOCFG, ladders: { ...GOCFG.ladders, service: { build: ['opencode-go/mimo-v2.6-flash', 'opencode-go/beam-free'] } } };
  let beamCalls = 0;
  const beh = {
    // платный ранг упирается в лимит на КАЖДОМ ключе — ровно то, что было в инциденте
    [short('opencode-go/mimo-v2.6-flash')]: () => ({ status: 429, error: 'usage limit' }),
    // бесплатный отказывает на СВОЕЙ первой попытке, но здоров на запасном ключе.
    // Счётчик, а не ключ: какой именно ключ достаётся стартовым, зависит от карусели (#81).
    [short('opencode-go/beam-free')]: () => (++beamCalls === 1 ? { status: 403, error: 'forbidden' } : { status: 200, content: 'ok' }),
  };
  const store = memoryStore(2);
  const calls = [];
  const r = await run(msg, { env, config: cfg, store, fetchImpl: fakeFetch(beh, calls) });

  assert.equal(r.ok, true,
    `бесплатный ранг обязан дойти до здорового ключа, попыток: ${JSON.stringify(r.attempts)}`);
  assert.equal(r.model, 'opencode-go/beam-free', 'и отвечает та же ступень, не фоллбэк');
  assert.equal(Object.keys(store.state.keys.paid || {}).length, 2,
    'оба 429 от платного ранга уходят в ПЛАТНУЮ карту, а не в общую');
  assert.equal(store.state.health['opencode-go/mimo-v2.6-flash']?.class, 'quota',
    'сам платный ранг по-прежнему припаркован — мы не ослабляем его защиту');
  assert.equal(store.state.health['opencode-go/beam-free'], undefined,
    'бесплатный ранг здоровью не испорчен: его 403 — свойство запроса, а не ступени');
});

test('non-key failure on a Go rung probes the spare key once, but never burns shared state', async () => {
  resetFreeGoKeyCursor();
  const beh = { [short('opencode-go/beam-free')]: () => ({ status: 503, error: 'temporarily overloaded' }) };
  const store = memoryStore(2);
  const calls = [];
  const r = await run(msg, { env, config: GOCFG, store, fetchImpl: fakeFetch(beh, calls) });
  assert.equal(store.state.keys.active, 0, 'a model-level fault never moves shared key state');
  // Карусель свободного Go (#81) двигает глобальный курсор, поэтому буква активного ключа
  // зависит от порядка тестов. Инварианты: первый и второй зов — один и тот же первый ранг
  // (Go) на разных ключах (активный + проба запасного), третий зов — уже следующий ранг,
  // и он OpenRouter, поэтому ключ у него свой (or_key), не Go-ключ.
  assert.equal(calls.length, 3, 'same rung twice (active + spare), then the next rung');
  assert.notEqual(calls[0].auth, calls[1].auth, 'the probe goes to the OTHER key');
  assert.equal(calls[0].model, calls[1].model, 'both first calls are on the same rung');
  assert.equal(calls[2].auth, 'Bearer or_key', 'rung 2 is OpenRouter, not a Go key');
  assert.equal(r.model, 'openrouter/nvidia/nemotron-3-super-120b-a12b:free');
  assert.equal(r.attempts.filter(a => a.outcome === 'key-probe').length, 1);
});

test('an account with no payment (no tariff) rotates instantly to the next account', async () => {
  // Owner scenario 2026-10-27: 4 Go accounts, only 2 are paid. An unpaid account returns
  // "insufficient account funds" immediately — no timeout, no waiting. The ladder must rotate
  // to the next account instantly and succeed there.
  resetFreeGoKeyCursor();
  let key0Called = 0;
  const beh = {
    [short(GOLADDER[0])]: (ctx) => {
      // Key 0 is unpaid → instant error; key 1 is paid → serves the call
      if (ctx.auth.includes('oc_a')) { key0Called++; return { status: 402, error: 'insufficient account funds' }; }
      return { status: 200, content: '{"ok":true}' };
    },
  };
  const store = memoryStore(2);
  const calls = [];
  const r = await run({ ...msg, response_format: { type: 'json_object' } }, { env, config: GOCFG, store, fetchImpl: fakeFetch(beh, calls) });
  assert.equal(r.ok, true, 'the ladder answers from the paid account');
  assert.equal(r.model, GOLADDER[0], 'the free Go rung serves');
  assert.equal(key0Called, 1, 'the unpaid account is tried exactly once');
  assert.ok(r.attempts.some(a => a.outcome === 'key-rotated'), 'rotation to the next account is visible');
  // The rotation happened instantly — the next account is tried on the SAME rung
  const goCalls = calls.filter(c => c.url.includes('opencode.ai'));
  assert.equal(goCalls.length, 2, 'first attempt on unpaid account, second on paid account');
  assert.notEqual(goCalls[0].auth, goCalls[1].auth, 'the two attempts use different accounts');
  assert.equal(goCalls[0].model, goCalls[1].model, 'same rung, different accounts');
});

test('no Go keys → OpenRouter only; no keys → 503; unknown ladder → 404', async () => {
  const calls = [];
  const r = await run(msg, { env: { OPENROUTER_API_KEY: 'k' }, config: GOCFG, store: memoryStore(0), fetchImpl: fakeFetch({}, calls) });
  assert.equal(r.model, LADDER.find(m => m.startsWith('openrouter/')));
  assert.equal((await run(msg, { env: {}, config: GOCFG, store: memoryStore(0), fetchImpl: fakeFetch({}, []) })).status, 503);
  assert.equal((await run({ ...msg, model: 'nope' }, { env, config: GOCFG, store: memoryStore(2), fetchImpl: fakeFetch({}, []) })).status, 404);
});

test('stale skip on every rung does not black-hole the call', async () => {
  const store = memoryStore(2);
  for (const m of LADDER) store.state.health[m] = { failures: 9, firstFailureAt: Date.now(), skipUntil: Date.now() + 60000 };
  const r = await run(msg, { env, config: GOCFG, store, fetchImpl: fakeFetch({}, []) });
  assert.equal(r.ok, true);
});

test('a transient Go skip is capped at 30s after the last failure (fleet returns to Go); real limits keep their TTL', async () => {
  const old = { failures: 5, firstFailureAt: Date.now() - 60000, lastFailureAt: Date.now() - 31000, skipUntil: Date.now() + 240000 };
  const store = memoryStore(2);
  store.state.health[GOLADDER[0]] = { ...old, class: 'transient' };
  const calls = [];
  await run(msg, { env, config: GOCFG, store, fetchImpl: fakeFetch({}, calls) });
  assert.equal(calls[0].model, short(GOLADDER[0]), 'a transient skip older than 30s is ignored — Go is re-tested');

  const fresh = memoryStore(2);
  fresh.state.health[GOLADDER[0]] = { ...old, lastFailureAt: Date.now() - 10000, class: 'transient' };
  const calls2 = [];
  await run(msg, { env, config: GOCFG, store: fresh, fetchImpl: fakeFetch({}, calls2) });
  assert.equal(calls2[0].model, short(GOLADDER[1]), 'a fresh transient skip is honoured');

  const quota = memoryStore(2);
  quota.state.health[GOLADDER[0]] = { ...old, class: 'quota' };
  const calls3 = [];
  await run(msg, { env, config: GOCFG, store: quota, fetchImpl: fakeFetch({}, calls3) });
  assert.equal(calls3[0].model, short(GOLADDER[1]), 'a quota park is never capped');
});

test('totalTimeoutMs stops walking the ladder', async () => {
  const beh = {};
  for (const m of LADDER) beh[short(m)] = () => ({ status: 500, error: 'boom' });
  const slow = fakeFetch(beh, []);
  const fetchImpl = async (u, i) => { await new Promise(r => setTimeout(r, 300)); return slow(u, i); };
  const r = await run(msg, { env, config: GOCFG, store: memoryStore(2), fetchImpl, totalTimeoutMs: 700 });
  assert.equal(r.ok, false);
  assert.ok(r.attempts.filter(a => a.outcome === 'error').length < LADDER.length);
});

test('state: per-model exponential backoff restarts for every model; key rotation + snapshot heal', () => {
  assert.deepEqual([1, 2, 3, 4, 10].map(n => backoffFor(n)), [2000, 4000, 8000, 16000, 300000]);
  const st = emptyState();
  assert.deepEqual(rotateKey(st, 2, 1000, 0), { rotated: true, fromIndex: 0, toIndex: 1 });
  const r = rotateKey(st, 2, 1000, 10);
  assert.equal(r.rotated, false);
  assert.equal(r.retryAt, 1000);
  assert.equal(snapshot(st, 2, 2000).keys.active, 1, 'healed keys: active stays usable');
});

// ── пол бюджета кольца: зен не влезал в собственный бюджет ────────────────────────────────────
// Замер на здоровом окне (2026-10-07 09:00 →, 1599 успешных задач): очередь p50 = 0 с,
// сервис zen p50 = 33 с, p90 = 63 с — а бюджет был 20 с + 15 с grace = 35 с, то есть сама
// модель не влезала в своё окно, и доставлялось 41.4 %.
test('ring watchdog floors at RING_WAIT_MS — the floor only ever raises', () => {
  assert.equal(RING_WAIT_MS, 45_000, '45 с + 15 с grace = 60 с → 74.8 % доставки');
  assert.ok(RING_WAIT_MS < 90_000, 'пол остаётся ниже предела пула MAX_WAIT_MS (90 с)');
  assert.equal(ringWaitMs(20_000), 45_000, 'дефолт поднят: 20 с не покрывали p50 33 с');
  assert.equal(ringWaitMs(undefined), 45_000, 'без бюджета — тоже пол');
  assert.equal(ringWaitMs(0), 45_000, 'ноль — не «мгновенно», а пол');
  assert.equal(ringWaitMs(75_000), 75_000, 'попросил больше — получил больше');
  assert.ok(ringWaitMs(46_000) > RING_WAIT_MS, 'пол никогда не урезает явный бюджет вызывающего');
});

// ── context-class is a property of the request, never a shared exhaustion ─────────────────────
// classify.js spells it out («the next task on this rung (from any user) is very likely a
// normal-sized prompt that would work fine»), but failureClass() collapsed 'context' into
// 'transient', so every fat prompt put the head rung into SHARED health for 2 s → 4 s → … (30 s
// cap for zen). Measured: one 1.1 MB agent prompt arrived ~24×/hour, i.e. the head rung was
// dark for a large slice of every hour while the paid tail was 402 — that is what `every rung
// failed` and a hung opencode turn look like from the inside.
test('a context-class failure is recorded but never health-skips the rung for other callers', async () => {
  const store = memoryStore(2);
  let headCalls = 0;
  const beh = {
    [short(GOLADDER[0])]: () => (++headCalls === 1
      ? { status: 413, error: 'input is too long for the free tier: 1146453 bytes, limit 50000' }
      : { status: 200, content: '{"ok":true}' }),
    [short(GOLADDER[1])]: () => ({ status: 200, content: '{"ok":true}' }),
  };
  const body = { ...msg, response_format: { type: 'json_object' } };

  const r1 = await run(body, { env, config: GOCFG, store, fetchImpl: fakeFetch(beh, []) });
  assert.equal(r1.ok, true, 'the ladder walks down instead of failing the caller');
  assert.ok(r1.attempts.some(a => a.model === GOLADDER[0] && a.outcome === 'error'), 'the refusal is reported');

  const h = store.state.health[GOLADDER[0]];
  assert.ok(h, 'the failure IS recorded — it stays visible in /v1/state');
  assert.equal(h.class, 'context', 'classified as a property of the request, not the rung');
  assert.equal(h.skipUntil, 0, 'and never a shared skip (null would mean dead, a timestamp would mean dark for everyone)');

  const r2 = await run(body, { env, config: GOCFG, store, fetchImpl: fakeFetch(beh, []) });
  assert.ok(r2.attempts.some(a => a.model === GOLADDER[0]),
    'the NEXT caller still gets the head rung — a transient skip filters it out of rungs entirely');
  assert.equal(r2.model, GOLADDER[0], 'and this time it answers');
});

// ── #45 для кольца: ретрай только если вторая попытка может отличаться ─────────────────────────
// Замер 2026-10-08 (сутки): 74 отказа `zen-rings: ok (finish=tool_calls, …)` — это ответ, у
// которого кольцевой воркер НЕ приложил tool_calls (старая версия, фикс от 07.10 не доехал:
// синк кольца падает, ZEN_RING_PAYLOAD в секретах нет). Повтор дал бы то же самое: дошли до
// ответа 8 из 74 (и то другой ступенью), суммарно 35 минут стенда за сутки.
test('ringGuardRetry: отказ по tool_calls не ретраится — структурный, а не флейк', () => {
  const toolCallOnly = { kind: 'ok', ok: false, finish_reason: 'tool_calls', usage: { completion_tokens: 65 } };
  assert.equal(ringGuardRetry(toolCallOnly, {}), false, 'повтор даст тот же результат — уходим сразу');

  const flake = { kind: 'ok', ok: false, finish_reason: 'length', usage: { completion_tokens: 3000 } };
  assert.equal(ringGuardRetry(flake, {}), true, 'обычный пустой ответ остаётся под ретрай (#45)');

  assert.equal(ringGuardRetry(toolCallOnly, { stream: true }), false, 'стрим и так оплатил окно');
  assert.equal(ringGuardRetry({ kind: 'ok', ok: true, finish_reason: 'tool_calls' }, {}), false,
    'это не guard-случай вовсе — ответ принят');
  assert.equal(ringGuardRetry(null, {}), false, 'нет данных — не ретраим вслепую');
});

// ── Streaming (opencode as a client of the free ladder) ────────────────────────────────────────
const enc = new TextEncoder();
function sseBody(events, { delayFirstMs = 0, endWithoutOutput = false, signal } = {}) {
  return new ReadableStream({
    async start(c) {
      // Честная эмуляция реального fetch: пока тело не начало отдаваться, abort рвёт поток, и
      // reader.read() бросает — ровно так в проде выглядит «no first token in time». Без этого
      // сетевой заглушке всё равно, и окно первого токена в тестах невозможно проверить.
      if (delayFirstMs) {
        await new Promise((resolve, reject) => {
          const t = setTimeout(resolve, delayFirstMs);
          if (!signal) return;
          const onAbort = () => { clearTimeout(t); reject(signal.reason || new Error('aborted')); };
          if (signal.aborted) onAbort(); else signal.addEventListener('abort', onAbort, { once: true });
        });
      }
      for (const e of events) c.enqueue(enc.encode(`data: ${typeof e === 'string' ? e : JSON.stringify(e)}\n\n`));
      c.close();
    },
  });
}
const delta = (d) => ({ choices: [{ delta: d }] });
function streamFetch(behaviour, calls) {
  return async (url, init) => {
    const body = JSON.parse(init.body);
    calls.push({ url, model: body.model, body });
    const b = (behaviour[body.model] || (() => ({ events: [delta({ role: 'assistant' }), delta({ content: 'hi' }), '[DONE]'] })))({ body, signal: init.signal });
    if (b.status && b.status !== 200) return { ok: false, status: b.status, text: async () => b.error || '' };
    // honour abort (ttfb timeout)
    const bodyStream = sseBody(b.events, b);
    return { ok: true, status: 200, body: bodyStream };
  };
}
async function readAll(stream) {
  const r = stream.getReader(); const dec = new TextDecoder(); let out = '';
  for (;;) { const { value, done } = await r.read(); if (done) return out; out += dec.decode(value); }
}

test('free-headed ladder rotates Go keys round-robin across calls (#81)', async () => {
  resetFreeGoKeyCursor();
  const cfg = { ...config, ladders: { ...config.ladders, free: { build: ['opencode-go/beam-free'] } } };
  const calls = [];
  const fetchImpl = async (url, init) => {
    calls.push(init.headers.Authorization);
    return { ok: true, status: 200, json: async () => ({ choices: [{ message: { content: 'ok' } }], usage: { prompt_tokens: 1 } }) };
  };
  const store = memoryStore(2);
  for (let i = 0; i < 4; i++) await run({ model: 'free', messages: [{ role: 'user', content: 'hi' }] }, { env, config: cfg, store, fetchImpl });
  assert.deepEqual(calls, ['Bearer oc_a', 'Bearer oc_b', 'Bearer oc_a', 'Bearer oc_b'],
    'consecutive free calls land on different pool keys');
});

// Замер 2026-10-08: 21 из 28 `no first token in time` — на payload ≥ 100 КБ (медиана 978 830 байт).
// Обе ступени с большим окном (sante:free 262K, nemotron-3-ultra-550b-a55b:free 1M) уходили в
// health-skip именно на этом, и лестница отвечала `every rung failed`, хотя промпт влезал.
test('TTFB: жирный запрос дожидается первого токена, маленький убивается по умолчанию', async () => {
  const slow = ({ signal }) => ({ delayFirstMs: 150, signal, events: [delta({ role: 'assistant' }), delta({ content: 'hi' }), '[DONE]'] });
  const beh = { [short(GOFREE[0])]: slow, [short(GOFREE[1])]: slow };
  // Свежий store на каждый прогон: после первого обрыва голова health-skip'ается, и второй
  // запуск начался бы уже со второй ступени — тогда тест мерял бы не окно, а этот пропуск.
  const opts = () => ({ env, config: GOCFG, store: memoryStore(2), ttfbMs: 100, fetchImpl: streamFetch(beh, []) });

  // маленький промпт: окно 100 мс → обрыв до первого токена → уход на следующую ступень
  const small = await run({ model: 'free', stream: true, messages: [{ role: 'user', content: 'hi' }] }, opts());
  assert.notEqual(small.model, GOFREE[0], 'маленький промпт убивается в 100 мс — это его окно');

  // жирный: 240K символов ≈ 240K байт ≈ 60K токенов → фактор 2 → окно 200 мс > 150 мс
  const fat = await run({ model: 'free', stream: true, messages: [{ role: 'user', content: 'x'.repeat(240_000) }] }, opts());
  assert.equal(fat.model, GOFREE[0], 'жирный запрос дождался первого токена — окно поднялось');
  assert.ok(estimateTokens({ messages: [{ role: 'user', content: 'x'.repeat(240_000) }] }) > 32_000,
    'контроль: промпт действительно попадает в полосу с надбавкой');
});

test('free ladder: stream answered by the first rung with output, bytes replayed intact', async () => {
  const calls = [];
  const r = await run({ model: 'free', stream: true, messages: [{ role: 'user', content: 'hi' }] }, { env, config: GOCFG, store: memoryStore(2), fetchImpl: streamFetch({}, calls) });
  assert.equal(r.ok, true);
  assert.equal(r.model, GOFREE[0]);
  assert.equal(calls[0].body.stream, true);
  const text = await readAll(r.stream);
  assert.match(text, /"role":"assistant"/, 'the buffered role frame is replayed');
  assert.match(text, /"content":"hi"/);
  assert.match(text, /\[DONE\]/);
});

test('stream: a rung that ends / errors before the first token fails over; role-only frame does not commit', async () => {
  const beh = {
    [short(GOFREE[0])]: () => ({ events: [delta({ role: 'assistant' })] }),                       // ends with no output
    [short(GOFREE[1])]: () => ({ events: [{ error: { message: 'upstream overloaded' } }] }),       // in-stream error
    [short(GOFREE[2])]: () => ({ status: 503, error: 'busy' }),
  };
  const calls = [];
  const r = await run({ model: 'free', stream: true, messages: [{ role: 'user', content: 'hi' }] }, { env, config: GOCFG, store: memoryStore(2), fetchImpl: streamFetch(beh, calls) });
  assert.equal(r.model, GOFREE[3]);
  assert.deepEqual(r.attempts.map(a => a.outcome), ['key-probe', 'error', 'error', 'error', 'ok'],
    'the first Go rung also gets the one spare-key probe; the rest fail over rung by rung');
});

test('stream: tool_calls delta counts as the first token (tools passed through)', async () => {
  const tools = [{ type: 'function', function: { name: 'bash', parameters: { type: 'object' } } }];
  const beh = { [short(GOFREE[0])]: ({ body }) => { assert.deepEqual(body.tools, tools); return { events: [delta({ tool_calls: [{ index: 0, function: { name: 'bash', arguments: '{}' } }] }), '[DONE]'] }; } };
  const r = await run({ model: 'free', stream: true, tools, messages: [{ role: 'user', content: 'ls' }] }, { env, config: GOCFG, store: memoryStore(2), fetchImpl: streamFetch(beh, []) });
  assert.equal(r.model, GOFREE[0]);
  assert.match(await readAll(r.stream), /tool_calls/);
});

test('non-stream: tool_calls with empty content is a valid answer', async () => {
  const f = async (url, init) => ({ ok: true, status: 200, json: async () => ({ choices: [{ message: { content: null, tool_calls: [{ id: 't', type: 'function', function: { name: 'bash', arguments: '{}' } }] } }] }), text: async () => '' });
  const r = await run({ model: 'free', messages: [{ role: 'user', content: 'ls' }], tools: [{ type: 'function', function: { name: 'bash' } }] }, { env, config: GOCFG, store: memoryStore(2), fetchImpl: f });
  assert.equal(r.ok, true);
  assert.equal(r.model, GOFREE[0]);
});

test('provider rejects response_format (400) → same rung retried once without it', async () => {
  const seen = [];
  const f = async (url, init) => {
    const body = JSON.parse(init.body); seen.push(!!body.response_format);
    if (body.response_format) return { ok: false, status: 400, text: async () => 'response_format is not supported by this model' };
    return { ok: true, status: 200, json: async () => ({ choices: [{ message: { content: '{"ok":true}' } }] }), text: async () => '' };
  };
  const r = await run({ model: 'free', response_format: { type: 'json_object' }, messages: [{ role: 'user', content: 'json' }] }, { env, config: GOCFG, store: memoryStore(2), fetchImpl: f });
  assert.equal(r.model, GOFREE[0]);
  assert.deepEqual(seen, [true, false]);
});

test('ladder_rung pins one rung: no failover, health skip ignored, foreign rung rejected', async () => {
  const store = memoryStore(2);
  store.state.health[GOLADDER[1]] = { failures: 3, firstFailureAt: Date.now(), skipUntil: Date.now() + 60000 };
  const calls = [];
  const r = await run(msg, { env, config: GOCFG, store, fetchImpl: fakeFetch({}, calls), pinRung: GOLADDER[1] });
  assert.equal(r.model, GOLADDER[1]);
  assert.equal(calls.length, 1);
  const beh = { [short(GOLADDER[1])]: () => ({ status: 500, error: 'boom' }) };
  const r2 = await run(msg, { env, config: GOCFG, store: memoryStore(2), fetchImpl: fakeFetch(beh, []), pinRung: GOLADDER[1] });
  assert.equal(r2.ok, false);
  assert.equal(r2.attempts.length, 1);
  assert.equal((await run(msg, { env, config: GOCFG, store: memoryStore(2), fetchImpl: fakeFetch({}, []), pinRung: 'openrouter/x/y' })).status, 400);
});

test('config: doctor = free Go → free OpenRouter → paid Go (owner 2026-09-28 / 2026-10-09 / #329)', () => {
  const expected = ['opencode-go/beam-free',
    'openrouter/nvidia/nemotron-3-ultra-550b-a55b:free', 'openrouter/dots-studio/dots-3-note-preview:free',
    'opencode-go/mimo-v2.6-flash', 'opencode-go/qwen3.7-plus', 'opencode-go/deepseek-v4-pro'];
  for (const role of ['build', 'plan', 'explore', 'general', 'review']) assert.deepEqual(config.ladders.doctor[role], expected, role);
});

test('config: research is split by role — free Go → free OpenRouter → paid Go (owner 2026-09-30 / 2026-10-09 / #329)', () => {
  const reader = ['opencode-go/beam-free',
    'openrouter/nvidia/nemotron-3-ultra-550b-a55b:free', 'openrouter/dots-studio/dots-3-note-preview:free',
    'opencode-go/mimo-v2.6-flash'];
  assert.deepEqual(config.ladders.research.explore, reader);
  const thinker = ['opencode-go/beam-free',
    'openrouter/nvidia/nemotron-3-ultra-550b-a55b:free', 'openrouter/dots-studio/dots-3-note-preview:free',
    'opencode-go/mimo-v2.6-flash', 'opencode-go/deepseek-v4.1-flash'];
  assert.ok(!JSON.stringify(config.ladders.research).includes('gemini-2.5-pro'), 'no 2.5-pro in research');
  assert.ok(!JSON.stringify(config.ladders.research).includes('openrouter/google/gemini'), 'платный Gemini убран — деньги на Go (owner 2026-10-08)');
});

test('config: conversation = hh-skill writing — free Go → free OpenRouter → paid Go (owner 2026-10-01 / 2026-10-09 / #329)', () => {
  const expected = [
    'opencode-go/beam-free',
    'openrouter/nvidia/nemotron-3-ultra-550b-a55b:free',
    'openrouter/dots-studio/dots-3-note-preview:free',
    'opencode-go/mimo-v2.6-flash',
  ];
  assert.deepEqual(CONVERSATION, expected);
  assert.deepEqual(config.ladders.conversation, { build: expected }, 'conversation is the build-role ladder');
});

// #71 (owner 2026-10-02, уточнение №2): уровень = отдельная лестница, «нет эскалации — это не
// задача лестницы, задача лестницы ретраи». Лестницы-уровни: build (base free ×3 + платный хвост,
// без mimo), build advanced (mimo + платный хвост), plan/general/review = advanced-first,
// explore = big-ctx only, picture* = гемини-стек (multimodal 1M), free = только
// бесплатное ($0-потолок для тяжёлых тестов). Два конструктора: single-model профили API
// (service/conversations — как есть) и сборка из четырёх лестниц для opencode (профили
// free / master / advanced).
test('config: tier ladders — build=base, build advanced=mimo, picture gemini, free $0 (#71)', () => {
  const FG = 'opencode-go/beam-free';
  // Владелец 2026-10-08: «на OR нет денег — платный должен быть на Go». Платный OR-хвост
  // убран из всех не-фри лестниц (каждый такой вызов = 402), платный сегмент теперь только Go.
  // 2026-10-28 (#318/#329): zen-ранги (mimo head, lightning, big-pickle) и мёртвые слаги
  // (nemotron-3-ultra-free, ling-3.0-flash-sante:free) убраны из всех лестниц.

  // Порядок: free Go → free OpenRouter → paid Go. Платный Go — в самом хвосте,
  // после всех бесплатных моделей (owner 2026-10-09).
  assert.deepEqual(config.ladders.build.build, [FG, BIG, DOTS, MIMO], 'build = free Go + two big windows + mimo in tail');
  assert.deepEqual(config.ladders['build advanced'].build, [FG, BIG, DOTS, MIMO], 'build advanced = бесплатный Go + бесплатный OpenRouter + mimo в хвосте');
  for (const role of ['plan', 'general', 'review']) {
    assert.deepEqual(config.ladders[role], { build: [FG, BIG, DOTS, MIMO] }, `${role} advanced-first — роль=лестница (#71)`);
  }

  // explore: контексты замерены по OpenRouter /v1/models 2026-10-02 — mimo 1M,
  // gemini-2.5-flash-lite 1048576, xiaomi mimo 1050000; ling-3.0-flash = 262144 и не годится.
  const explore = config.ladders.explore.build;
  assert.deepEqual(explore, [
    FG,
    'openrouter/nvidia/nemotron-3-ultra-550b-a55b:free',
    'openrouter/dots-studio/dots-3-note-preview:free',
    'opencode-go/mimo-v2.6-flash',
  ], 'explore = бесплатный OpenRouter + mimo в хвосте (1M + 512K)');
  assert.ok(!JSON.stringify(explore).includes('ling-3.0-flash'), 'ling 256k must not enter explore');

  // vision (распознавание картинок): всё multimodal image+text, 1M (замер архитектуры OpenRouter 02.10)
  assert.deepEqual(config.ladders.vision.build, [
    'openrouter/google/gemini-2.5-flash-lite',
    'openrouter/google/gemini-2.5-flash',
  ], 'vision = gemini base (0.10/0.40 -> 0.30/2.50)');
  assert.deepEqual(config.ladders['vision advanced'].build, [
    'openrouter/google/gemini-2.5-flash',
    'openrouter/google/gemini-3.8-flash',
  ], 'vision advanced = gemini advanced (0.30/2.50 -> 0.75/3.75)');

  // free: потолок $0 — ни подписки, ни платного (для тестов с объёмом/повторами)
  const f100 = config.ladders.free.build;
  assert.ok(f100.every(m => m.startsWith('zen-rings/') || m.startsWith('opencode-zen/') || m.endsWith('-free') || m.endsWith(':free')), 'every rung is $0');
  assert.ok(!f100.some(m => m === 'opencode-go/mimo-v2.6-flash'), 'no subscription rung');
  assert.ok(!f100.some(m => m.startsWith('openrouter/') && !m.endsWith(':free')), 'no paid rung');

  assert.ok(!JSON.stringify(config.ladders.build).includes('opencode-zen/'), 'zen stays out (#42)');
  // #329: zen-rings вырезан из всех лестниц — анонимный free-доступ убит апстримом (429 для всех IP).
  assert.ok(!JSON.stringify(config.ladders.build).includes('zen-rings/'), 'zen-rings вырезан из build (#329)');
  // Владелец 2026-10-09: «сначала free go, далее free openrouter и только потом платные модели go».
  assert.equal(config.ladders.service.build[config.ladders.service.build.length - 1], MIMO, 'самая дешёвая платная Go — последняя в лестнице');
  assert.equal(config.ladders.service.build[0], 'opencode-go/beam-free', 'бесплатный Go открывает лестницу');
  assert.deepEqual(config.ladders.service.build.slice(1, 3),
    ['openrouter/nvidia/nemotron-3-super-120b-a12b:free', 'openrouter/nvidia/nemotron-3-ultra-550b-a55b:free'],
    'за ним сразу бесплатный OpenRouter (#329: zen вырезан)');
});

// Владелец 2026-10-09: «сначала free go, далее free openrouter и только потом платные модели go»
// (#329: сегмент free zen вырезан — апстрим убил анонимный доступ).
// Правило, а не разовый список: тест не хранит 27 копий состава, он проверяет инвариант — поэтому
// следующая появившаяся платная Go встанет в хвост сама, а чья-то забытая правка уронит тест.
test('config: во всех не-фри лестницах free go → free openrouter → paid go, без Muse и zen (owner 2026-10-09 / #329)', () => {
  const prices = JSON.parse(fs.readFileSync(new URL('../config/prices.json', import.meta.url)));
  // standard/fast are the paid-first public profiles (owner 2026-10-05): they keep their paid
// OpenRouter reserve and are excluded from this ordering invariant, exactly as in the main branch.
const SKIP = new Set(['free', 'vision', 'vision advanced', 'standard', 'fast']);
  let checked = 0;
  for (const [name, roles] of Object.entries(config.ladders)) {
    if (SKIP.has(name)) continue;
    for (const [role, rungs] of Object.entries(roles)) {
      const freeGo = rungs.filter(m => m.startsWith('opencode-go/') && m.endsWith('-free'));
      const freeOR = rungs.filter(m => m.startsWith('openrouter/') && m.endsWith(':free'));
      const paidGo = rungs.filter(m => m.startsWith('opencode-go/') && !m.endsWith('-free'));
      // free Go → free OR → paid Go (#329: zen вырезан из всех лестниц)
      const lastFreeORIdx = rungs.findLastIndex(m => m.startsWith('openrouter/') && m.endsWith(':free'));
      const firstPaidIdx = rungs.findIndex(m => m.startsWith('opencode-go/') && !m.endsWith('-free'));
      assert.ok(lastFreeORIdx < firstPaidIdx, `${name}:${role} — free OR до paid Go (OR=${lastFreeORIdx}, paid=${firstPaidIdx})`);
      assert.ok(!rungs.some(m => m.startsWith('zen-rings/')), `${name}:${role} — без zen (#329)`);
      assert.ok(paidGo.every((m, k) => rungs.indexOf(m) === firstPaidIdx + k), `${name}:${role} — платный Go-сегмент сплошной`);
      const paidNames = [...paidGo];
      assert.deepEqual(paidNames, [...paidNames].sort((a, b) => prices[a][1] - prices[b][1]),
        `${name}:${role} — платный Go-сегмент от дешёвых к дорогим`);
      assert.equal(paidNames[0], MIMO, `${name}:${role} — mimo ($0.28/1M) первая платная Go`);
      assert.ok(!rungs.includes(MUSE), `${name}:${role} — Muse нет: 400 trains on request data (Privacy settings)`);
      assert.equal(rungs.filter(m => m.startsWith('openrouter/') && !m.endsWith(':free')).length, 0,
        `${name}:${role} — платного OpenRouter нет (402, деньги на Go, owner 2026-10-08)`);
      assert.ok(rungs.includes(BIG), `${name}:${role} — есть бесплатный ранг с окном 1M (иначе жирная сессия → every rung failed)`);
      assert.ok(rungs.includes(DOTS), `${name}:${role} — есть ВТОРОЕ большое окно (512K): одно перегруженное не должно ронять лестницу`);
      assert.ok(freeGo.length > 0, `${name}:${role} — есть бесплатный Go (голова)`);
      assert.ok(freeOR.length > 0, `${name}:${role} — есть бесплатный OpenRouter`);
      const cheapest = Math.min(...paidGo.map(m => prices[m][1]));
      assert.equal(prices[MIMO][1], cheapest, `${name}:${role} — mimo и правда самая дешёвая платная Go`);
      checked++;
    }
  }
  assert.ok(checked >= 20, `проверено ${checked} не-фри лестниц`);
  assert.ok(!JSON.stringify(config.ladders.free).includes(MIMO), 'free остаётся жёстким $0');
  assert.ok(!JSON.stringify(config.ladders.vision).includes(MIMO), 'vision — multimodal, Go-модель текстовая');
});

test('ladder: conversation walks top-down — 3.1-flash-lite-preview answers, 2.5-flash only on its failure', async () => {
  const calls = [];
  const beh = { [short(GOCONVERSATION[0])]: () => ({ status: 500, error: 'boom' }) };
  const r = await run({ model: 'conversation', messages: [{ role: 'user', content: 'hi' }] },
    { env, config: GOCFG, store: memoryStore(2), fetchImpl: fakeFetch(beh, calls) });
  assert.equal(r.ok, true);
  assert.equal(r.model, GOCONVERSATION[1]);
  // upstream sees the provider prefix stripped (openrouter/ is routing, not part of the id)
  // Первый ранг бесплатный Go — на нём срабатывает проба запасного ключа (#45/#69), поэтому
  // в списке его попытка может быть дважды. Инварианты: префикс у первого срезан, и последний
  // вызов — тот, кто ответил.
  assert.equal(calls[0].model, short(GOCONVERSATION[0]), 'prefix stripped on the first rung too');
  assert.equal(calls.at(-1).model, short(GOCONVERSATION[1]), 'the answering rung is the last call');
});

test('ladder: conversation ladder_rung pins the requested model without failover (model switch / bench)', async () => {
  const calls = [];
  const r = await run({ model: 'conversation', messages: [{ role: 'user', content: 'hi' }] },
    { env, config: GOCFG, store: memoryStore(2), fetchImpl: fakeFetch({}, calls), pinRung: GOCONVERSATION[1] });
  assert.equal(r.ok, true);
  assert.equal(r.model, GOCONVERSATION[1]);
  assert.equal(calls.length, 1, 'pinned rung, no walk');
});

// Incident 2026-09-29: two concurrent calls both started on key 0; A hit the weekly limit and
// rotated to key 1, then B hit the same limit on key 0 and parked the HEALTHY key 1 for 6h —
// "every rung failed" fleet-wide while key 1 still had allowance.
test('state: a late failure on an already-rotated key parks THAT key, not the healthy active one', () => {
  const st = emptyState();
  assert.deepEqual(rotateKey(st, 2, 6 * 3600e3, 0, 0), { rotated: true, fromIndex: 0, toIndex: 1 });
  const late = rotateKey(st, 2, 6 * 3600e3, 5, 0);
  assert.deepEqual(late, { rotated: true, fromIndex: 0, toIndex: 1 });
  assert.equal(st.keys.exhausted[1], undefined, 'key 1 was never failed — must stay usable');
  assert.equal(snapshot(st, 2, 10).keys.active, 1);
});

test('ladder: concurrent weekly-limit on key 0 keeps Go serving on key 1', async () => {
  const env = { LADDER_TOKEN: 't', OPENCODE_GO_API_KEYS: 'oc_a, oc_b', OPENROUTER_API_KEY: 'or' };
  const cfg = { ladders: { service: { build: ['opencode-go/mimo', 'openrouter/x'] } } };
  const store = memoryStore(2);
  const weekly = () => new Response('{"type":"error","error":{"type":"GoUsageLimitError","message":"Go usage limit exceeded"},"metadata":{"limitName":"weekly"}}', { status: 429 });
  const ok = () => new Response(JSON.stringify({ choices: [{ message: { content: 'ok' } }] }), { status: 200 });
  let gate; const held = new Promise(r => { gate = r; });
  let firstA = true;
  const fetchImpl = async (u, init) => {
    const auth = init.headers.Authorization;
    if (String(u).includes('openrouter')) return new Response('down', { status: 500 });
    if (auth === 'Bearer oc_a') {
      if (firstA) { firstA = false; await held; } // B's key-0 failure lands AFTER A rotated
      return weekly();
    }
    return ok();
  };
  const body = { model: 'service', messages: [{ role: 'user', content: 'hi' }] };
  const pB = run(body, { env, config: cfg, store, fetchImpl });
  await new Promise(r => setTimeout(r, 10));
  const a = await run(body, { env, config: cfg, store, fetchImpl });
  gate();
  const b = await pB;
  assert.equal(a.ok, true); assert.equal(b.ok, true);
  const snap = await store.snapshot(2);
  assert.equal(snap.keys.exhausted[1], undefined);
  assert.equal(snap.keys.active, 1);
  const c = await run(body, { env, config: cfg, store, fetchImpl });
  assert.equal(c.ok, true);
  assert.equal(c.model, 'opencode-go/mimo');
});

test('state: resetKeys clears Go key parks and Go rung skips, keeps OpenRouter health', () => {
  const st = emptyState();
  st.keys = { active: 1, exhausted: { 0: 9e15, 1: 9e15 }, paid: { 0: 9e15 } };
  st.health = { 'opencode-go/mimo': { skipUntil: 9e15 }, 'openrouter/x': { skipUntil: 9e15 } };
  resetKeys(st);
  assert.deepEqual(st.keys, { active: 0, exhausted: {}, paid: {} },
    'общий парк и ПЛАТНЫЙ парк сбрасываются вместе — рычаг оператора обязан освобождать оба');
  assert.deepEqual(Object.keys(st.health), ['openrouter/x']);
});

// ── OpenRouter app attribution (#33) ───────────────────────────────────────────────────────────
test('upstreamRequest: HTTP-Referer / X-OpenRouter-Title / Visibility only on openrouter/* rungs', () => {
  const or = upstreamRequest(env, 'openrouter/deepseek/deepseek-v4-flash-0731', msg, 0, { appSlug: 'hh-messages', appTitle: 'HH Messages' });
  assert.equal(or.headers['HTTP-Referer'], `${APP_REFERER_BASE}/hh-messages`);
  assert.equal(or.headers['X-OpenRouter-Title'], 'HH Messages');
  assert.equal(or.headers['X-OpenRouter-App-Visibility'], 'hidden');

  const go = upstreamRequest(env, 'opencode-go/mimo-v2.6-flash', msg, 0, { appSlug: 'hh-messages', appTitle: 'HH Messages' });
  for (const h of ['HTTP-Referer', 'X-OpenRouter-Title', 'X-OpenRouter-App-Visibility']) {
    assert.equal(go.headers[h], undefined, `${h} must not be sent to Go`);
  }
});

// #341, п. 4: шлюз opencode-go отвечает HTTP 400 на любое неизвестное ему поле, а раньше сюда шёл
// клиентский body целиком (`upstream = { ...body }`). Список собран замером против живого шлюза;
// на OpenRouter ВСЕ эти поля проходят, поэтому фильтр применяется только к Go.
test('upstreamRequest: opencode-go получает только известные поля, OpenRouter — body целиком (#341)', () => {
  const body = {
    model: 'service',
    messages: [{ role: 'user', content: 'hi' }],
    // замерено: каждый из этих даёт 400 от гейта opencode-go
    top_k: 40, min_p: 0.05, repetition_penalty: 1.1, n: 2, logit_bias: { 1: -1 },
    custom_id: 'c', store: true, parallel_tool_calls: false, prompt_cache: true,
    // а эти проходят и должны доехать
    temperature: 0.7, top_p: 0.9, stop: ['\n'], seed: 42,
    user: 'trained-assist', metadata: { a: 'b' }, service_tier: 'auto',
  };

  const logged = [];
  const log = console.log;
  console.log = (line) => logged.push(String(line));
  let go;
  try {
    go = upstreamRequest(env, 'opencode-go/beam-free', body, 0, {});
  } finally { console.log = log; }

  for (const dropped of ['top_k', 'min_p', 'repetition_penalty', 'n', 'logit_bias',
    'custom_id', 'store', 'parallel_tool_calls', 'prompt_cache']) {
    assert.equal(dropped in go.body, false, `opencode-go не должен получить ${dropped}`);
  }
  // Отсечение обязано быть видимым: иначе клиент молча лишится работающего поля, а шлюз может
  // принимать и то, чего мы не пробовали. В лог уходят ТОЛЬКО ИМЕНА, значения не логируются.
  const whitelistLog = logged.map((l) => { try { return JSON.parse(l); } catch { return null; } })
    .find((e) => e && e.route === 'go-field-whitelist');
  assert.ok(whitelistLog, 'отброшенные поля фиксируются в логе');
  assert.deepEqual(whitelistLog.dropped.sort(),
    ['custom_id', 'logit_bias', 'min_p', 'n', 'parallel_tool_calls', 'prompt_cache', 'repetition_penalty', 'store', 'top_k'].sort(),
    'и перечислены по именам');
  assert.equal(logged.join('').includes('"hi"'), false, 'значения полей в лог не попадают');
  for (const kept of ['messages', 'temperature', 'top_p', 'stop', 'seed', 'user', 'metadata', 'service_tier']) {
    assert.ok(kept in go.body, `opencode-go обязан получить ${kept}`);
  }
  // model / stream / max_tokens выставляет сама лестница, а не клиент
  assert.equal(go.body.model, 'beam-free');
  assert.equal(go.body.stream, false);
  assert.ok(go.body.max_tokens > 0, 'потолок ответа ставит лестница');

  const or = upstreamRequest(env, 'openrouter/nvidia/nemotron-3-super-120b-a12b:free', body, 0, {});
  for (const field of ['top_k', 'n', 'store', 'min_p']) {
    assert.ok(field in or.body, `OpenRouter получает body целиком — ${field} на месте`);
  }
  assert.equal(or.body.model, 'nvidia/nemotron-3-super-120b-a12b:free', 'и префикс у OpenRouter срезается как раньше');
});

// #341: strict-схема, не выполняющая правила strict-режима (additionalProperties:false на каждом
// объекте + каждое свойство в required), отвергается шлюзом opencode-go HTTP 400. Замерено
// 2026-10-29: 400 воспроизводится 4 из 4 прогонов на strict:true без AP:false, и та же схема
// с флагом снятым проходит. Лестница повторяет ТУ ЖЕ ступень один раз — схему не трогая.
test('HTTP 400 от strict-схемы → повтор той же ступени без флага strict (#341)', async () => {
  const cfg = { ...GOCFG, ladders: { ...GOCFG.ladders, service: { build: ['opencode-go/beam-free'] } } };
  const parameters = { type: 'object', properties: { q: { type: 'string' } }, required: ['q'] };
  const strictTool = [{ type: 'function', function: { name: 'search', description: 's', strict: true, parameters } }];

  const calls = [];
  const beh = {
    // шлюз требует additionalProperties:false при strict — этой схемы нет, поэтому отказ
    [short('opencode-go/beam-free')]: ({ body }) =>
      (body.tools?.[0]?.function?.strict === true ? { status: 400, error: 'Invalid schema for strict mode' } : { status: 200, content: 'ok' }),
  };
  const store = memoryStore(2);
  const r = await run({ model: 'service', tools: strictTool, messages: [{ role: 'user', content: 'hi' }] },
    { env, config: cfg, store, fetchImpl: fakeFetch(beh, calls) });

  assert.equal(r.ok, true, `ступень обязана ответить после повтора: ${JSON.stringify(r.attempts)}`);
  assert.equal(calls.length, 2, 'ровно два запроса: с strict → 400, без → 200');
  assert.equal(calls[0].body.tools[0].function.strict, true, 'первый ушёл с флагом strict');
  assert.equal(calls[1].body.tools[0].function.strict, undefined, 'повтор — уже без него');
  assert.deepEqual(calls[1].body.tools[0].function.parameters, parameters,
    'СХЕМА не изменена — снят только флаг, контракт клиента не переписан');
  assert.equal(store.state.health['opencode-go/beam-free'], undefined,
    'повтор не ядовит здоровью ступени: успех снимает любую записанную попытку');

  // повтор ОДИН раз НА КЛЮЧ: если шлюз отвергает и без флага, ступень честно падает, а не крутится.
  // После повтора включается обычный обход ключей (#311), поэтому считаем по каждому ключу.
  const always400 = { [short('opencode-go/beam-free')]: () => ({ status: 400, error: 'nope' }) };
  const calls2 = [];
  const r2 = await run({ model: 'service', tools: strictTool, messages: [{ role: 'user', content: 'hi' }] },
    { env, config: cfg, store: memoryStore(2), fetchImpl: fakeFetch(always400, calls2) });
  assert.equal(r2.ok, false, 'и не зависает, если помогло не снятие флага');

  const perKey = {};
  for (const c of calls2) {
    const slot = (perKey[c.auth] ||= { total: 0, strict: 0 });
    slot.total++;
    if (c.body.tools?.[0]?.function?.strict === true) slot.strict++;
  }
  for (const [auth, v] of Object.entries(perKey)) {
    assert.equal(v.strict, 1, `на ключ ${auth} ровно ОДИН запрос с флагом strict, а не по кругу: ${v.strict}`);
    assert.ok(v.total <= 2, `на ключ ${auth} не больше двух запросов (strict + повтор): ${v.total}`);
  }
});

test('app attribution defaults: no slug → llm-ladder, no title → Trained Assist (openrouter only)', () => {
  const or = upstreamRequest(env, 'openrouter/x/y', msg, 0, {});
  assert.equal(or.headers['HTTP-Referer'], `${APP_REFERER_BASE}/${DEFAULT_APP_SLUG}`);
  assert.equal(or.headers['X-OpenRouter-Title'], DEFAULT_APP_TITLE);
  assert.equal(or.headers['X-OpenRouter-App-Visibility'], 'hidden');
});

test('sanitize: garbage slug falls back to llm-ladder (never a half-repaired one)', () => {
  assert.equal(sanitizeAppSlug('gtd-intent'), 'gtd-intent');
  assert.equal(sanitizeAppSlug(' HH-Messages '), 'hh-messages');
  for (const junk of ['', '  ', 'привет', 'a b', 'a/b', '../../etc/passwd', 'a'.repeat(65), 'a_b', 'a\nb']) {
    assert.equal(sanitizeAppSlug(junk), DEFAULT_APP_SLUG, JSON.stringify(junk));
  }
  assert.equal(sanitizeAppSlug(null), DEFAULT_APP_SLUG);
  assert.equal(sanitizeAppTitle(''), DEFAULT_APP_TITLE);
  assert.equal(sanitizeAppTitle(null), DEFAULT_APP_TITLE);
  assert.equal(sanitizeAppTitle('My\x00Tool\n '), 'MyTool');
});

test('run() threads appSlug/appTitle down to the OpenRouter upstream; Go stays clean', async () => {
  const beh = { [short(GOLADDER[0])]: () => ({ status: 500, error: 'boom' }), [short(GOLADDER[1])]: () => ({ status: 500, error: 'boom' }) };
  const calls = [];
  const f = async (url, init) => {
    calls.push({ url, headers: init.headers });
    const body = JSON.parse(init.body);
    return { ok: false, status: 500, text: async () => (beh[body.model] ? beh[body.model]().error : 'x') };
  };
  await run(msg, { env, config: GOCFG, store: memoryStore(2), fetchImpl: f, appSlug: 'gtd-intent', appTitle: 'GTD Intent' });
  const go = calls.filter(c => c.url.includes('opencode.ai'));
  const or = calls.filter(c => c.url.includes('openrouter.ai'));
  assert.ok(go.length >= 2 && or.length >= 1, 'walked Go rungs then OpenRouter');
  assert.equal(go[0].headers['HTTP-Referer'], undefined);
  assert.equal(or[0].headers['HTTP-Referer'], `${APP_REFERER_BASE}/gtd-intent`);
  assert.equal(or[0].headers['X-OpenRouter-Title'], 'GTD Intent');
  assert.equal(or[0].headers['X-OpenRouter-App-Visibility'], 'hidden');
});

// ── Zen free tier via relay (#36) ────────────────────────────────────────────────────────────────
const zenv = { ...env, OPENCODE_ZEN_RELAY_TOKEN: 'zr_t', OPENCODE_ZEN_BASE_URL: 'https://relay.test/zen' };

test('zen upstreamRequest: relay URL + relay token, no OpenRouter attribution, model prefix stripped', () => {
  const r = upstreamRequest(zenv, 'opencode-zen/mimo-v2.6-flash-free', { ...msg, response_format: { type: 'json_object' } }, 0, { appSlug: 'hh-messages' });
  assert.equal(r.url, 'https://relay.test/zen/chat/completions');
  assert.equal(r.headers.Authorization, 'Bearer zr_t');
  assert.equal(r.body.model, 'mimo-v2.6-flash-free');
  assert.equal(r.body.stream, false);
  for (const h of ['HTTP-Referer', 'X-OpenRouter-Title', 'X-OpenRouter-App-Visibility']) {
    assert.equal(r.headers[h], undefined, `${h} must not be sent to the zen relay`);
  }
  assert.equal(r.headers['x-opencode-session'], undefined, 'zen has its own session fingerprint');
  assert.ok(r.body.response_format, 'response_format passes through to the relay');

  const conv = upstreamRequest(zenv, 'opencode-zen/big-pickle', msg, 0, { conversation: 'conv-42' });
  assert.equal(conv.headers['x-session-id'], 'conv-42', 'conversation id goes to the relay for ses_ derivation');
});

// #42 (owner): zen left service for the free ladder TAIL, so the live service walk no longer
// contains a zen rung. The relay mechanics below are what matter here — pin them to an explicit
// config (the DIAG_CFG pattern) instead of to GOLADDER[0]/GOLADDER[1].
const ZEN_WALK = [
  'opencode-go/beam-free',
  'opencode-zen/mimo-v2.6-flash-free',
  'openrouter/nvidia/nemotron-3-super-120b-a12b:free',
];
const ZEN_CFG = { ...config, ladders: { ...config.ladders, service: { build: ZEN_WALK } } }; // canonical key
const zenGoFail = () => ({
  [short(ZEN_WALK[0])]: () => ({ status: 500, error: 'boom' }),
  [short(ZEN_WALK[1])]: () => ({ status: 500, error: 'boom' }),
});

test('zen rung without OPENCODE_ZEN_RELAY_TOKEN is skipped; with it the relay answers', async () => {
  // no token → hasKey filters every zen rung out: the walk goes Go → (zen absent) → OpenRouter
  const beh = zenGoFail();
  const noTok = [];
  const r0 = await run(msg, { env, config: ZEN_CFG, store: memoryStore(2), fetchImpl: fakeFetch(beh, noTok) });
  assert.equal(r0.model, 'openrouter/nvidia/nemotron-3-super-120b-a12b:free', 'unconfigured zen → OpenRouter free tier');
  assert.ok(noTok.every(c => !c.url.includes('sslip.io')), 'no relay calls without the token');

  // token present → the third rung (first zen) serves after both Go-free rungs fail
  const calls = [];
  const r1 = await run(msg, { env: zenv, config: ZEN_CFG, store: memoryStore(2), fetchImpl: fakeFetch(beh, calls) });
  assert.equal(r1.ok, true);
  assert.equal(r1.model, 'opencode-zen/mimo-v2.6-flash-free');
  const zenCall = calls[calls.length - 1];
  assert.equal(zenCall.url, 'https://relay.test/zen/chat/completions');
  assert.equal(zenCall.auth, 'Bearer zr_t');
  assert.equal(zenCall.body.model, 'mimo-v2.6-flash-free');
});

test('zen failures skip the rung for everyone (shared health), same as any provider', async () => {
  const beh = {
    ...zenGoFail(),
    'mimo-v2.6-flash-free': () => ({ status: 429, error: 'Rate limit exceeded. Please try again later.' }),
  };
  const store = memoryStore(2);
  const r = await run(msg, { env: zenv, config: ZEN_CFG, store, fetchImpl: fakeFetch(beh, []) });
  assert.equal(r.ok, true, 'walked past the limited zen rung');
  assert.equal(r.model, 'openrouter/nvidia/nemotron-3-super-120b-a12b:free', 'the ladder continues past the parked zen rung');
  const h = store.state.health['opencode-zen/mimo-v2.6-flash-free'];
  assert.ok(h && h.skipUntil > Date.now(), 'the limited zen rung is parked in shared health');
});

test('route: x-ladder-app / x-ladder-app-title headers are sanitised and forwarded to run()', async () => {  const ENV = { LADDER_TOKEN: 't', OPENCODE_GO_API_KEYS: 'oc_a,oc_b', OPENROUTER_API_KEY: 'or_key' };
  const seen = [];
  const f = async (url, init) => {
    seen.push({ url, headers: init.headers });
    return { ok: true, status: 200, json: async () => ({ choices: [{ message: { content: 'ok' } }] }), text: async () => '' };
  };
  const post = (headers) => handle(new Request('https://l.test/v1/chat/completions', {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: 'Bearer t', ...headers },
    body: JSON.stringify({ model: 'service', ladder_rung: 'openrouter/nvidia/nemotron-3-ultra-550b-a55b:free', messages: [{ role: 'user', content: 'hi' }] }),
  }), ENV, { store: memoryStore(0), fetchImpl: f });

  await post({ 'x-ladder-app': 'bg-Playbooks', 'x-ladder-app-title': 'Background Playbooks' });
  assert.equal(seen[0].headers['HTTP-Referer'], `${APP_REFERER_BASE}/bg-playbooks`);
  assert.equal(seen[0].headers['X-OpenRouter-Title'], 'Background Playbooks');

  await post({ 'x-ladder-app': 'GTD Intent/../../etc' });
  assert.equal(seen[1].headers['HTTP-Referer'], `${APP_REFERER_BASE}/${DEFAULT_APP_SLUG}`, 'garbage slug → default, not forwarded as-is');
  assert.equal(seen[1].headers['X-OpenRouter-Title'], DEFAULT_APP_TITLE);

  await post({});
  assert.equal(seen[2].headers['HTTP-Referer'], `${APP_REFERER_BASE}/${DEFAULT_APP_SLUG}`, 'no header → default slug');
  assert.equal(seen[2].headers['X-OpenRouter-App-Visibility'], 'hidden');
});

// #329 (2026-10-28): zen-rings вырезаны из всех лестниц — анонимный free-доступ убит апстримом
// (429 FreeUsageLimitError с любого egress: residential, GHA Azure, CF-коло), лимит ~1000/модель/
// день на чистый фиксированный IP — копейки для нашего трафика (решение владельца). Контракт
// хранит и форму возврата: вернуть кольцо = вернуть ранги конфигом и снять zen_ring:false.
test('CONFIG CONTRACT (#329): zen вырезан — ни одного zen-ранга нигде, кольцо выключено флагом', () => {
  const FREE_GO = 'opencode-go/beam-free';
  for (const [name, roles] of Object.entries(config.ladders)) {
    for (const [role, rungs] of Object.entries(roles)) {
      assert.ok(!rungs.some((m) => m.startsWith('zen-rings/') || m.startsWith('opencode-zen/')),
        `${name}/${role}: zen не должен вернуться без решения владельца (#329)`);
    }
  }
  assert.equal(config.zen_ring, false, 'zen_ring: false — операторский флаг выключает и канарейку, и механику кольца');

  // Каждая текстовая лестница открывается бесплатным Go. vision* — исключение (multimodal),
  // standard/fast — платные публичные профили (owner 2026-10-05).
  for (const [name, roles] of Object.entries(config.ladders)) {
    for (const [role, rungs] of Object.entries(roles)) {
      if (name.startsWith('vision') || name === 'standard' || name === 'fast') continue;
      assert.equal(rungs[0], FREE_GO, `${name}/${role} must open with free Go`);
    }
  }

  // Ни один платный ранг не может стоять перед бесплатным хвостом, иначе free-тир декоративный.
  for (const [name, roles] of Object.entries(config.ladders)) {
    if (name.startsWith('vision') || name === 'standard' || name === 'fast') continue;
    const firstPaid = roles.build.findIndex((m) => m.startsWith('openrouter/') && !m.endsWith(':free'));
    if (firstPaid !== -1) assert.ok(firstPaid >= 1, `${name}: a paid rung sits ahead of the free head`);
  }

  // The deepseek alias exists because stored opencode profiles still send it. It resolves to the
  // build ladder, NOT to `service` — a wrong target here would silently move historical traffic
  // onto a different rung order.
  assert.deepEqual(config.aliases, { deepseek: 'build' });
  assert.deepEqual(rungsFor(config, 'deepseek'), config.ladders.build.build);
  assert.deepEqual(rungsFor(config, 'deepseek:build'), config.ladders.build.build);
});
