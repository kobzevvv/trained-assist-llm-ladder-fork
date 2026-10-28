import { test } from 'node:test';
import assert from 'node:assert/strict';
import { makeTrace, logCall } from '../src/trace.js';
import { handle } from '../src/handler.js';
import { memoryStore } from '../src/state.js';
import { sanitizeAppSlugOrNull, DEFAULT_APP_SLUG } from '../src/ladder.js';
import ladders from '../config/ladders.json' with { type: 'json' };

function call(extraHeaders = {}) {
  return new Request('https://llm-ladder.trainedassist.store/v1/chat/completions', {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...extraHeaders },
    body: '{}',
  });
}

// Fake D1: captures every INSERT's bound params and lets us toggle a throw.
function fakeD1() {
  const rows = [];
  return {
    _rows: rows,
    _fail: false,
    prepare() {
      const d1 = this;
      return {
        bind(...values) {
          d1._pending = values;
          return this;
        },
        async run() {
          if (d1._fail) throw new Error('db down');
          rows.push(d1._pending);
          return { success: true };
        },
      };
    },
  };
}

function row(r) {
  const cols = ['ts', 'trace_id', 'run_id', 'user_id', 'chat_id', 'session_id', 'ladder', 'ok', 'model', 'ms', 'attempts', 'tokens_in', 'tokens_out', 'tokens_cached', 'tokens_cache_write', 'estimated_tokens_in', 'app'];
  return Object.fromEntries(cols.map((c, i) => [c, r[i]]));
}

test('makeTrace: extracts all five headers, capped', () => {
  assert.deepEqual(makeTrace(call({
    'x-ladder-trace': 'task-123', 'x-ladder-run': 'run-1', 'x-ladder-user': 'u7',
    'x-ladder-chat': 'c42', 'x-ladder-session': 's-99',
  })), { traceId: 'task-123', runId: 'run-1', userId: 'u7', chatId: 'c42', sessionId: 's-99', app: null });
  assert.deepEqual(makeTrace(call()), { traceId: null, runId: null, userId: null, chatId: null, sessionId: null, app: null });
  assert.equal(makeTrace(call({ 'x-ladder-trace': '  ' })).traceId, null);
  assert.equal(makeTrace(call({ 'x-ladder-trace': 'x'.repeat(500) })).traceId.length, 200);
});

test('makeTrace: optional OpenCode session/run IDs map without caller-supplied Ladder fields', () => {
  assert.deepEqual(makeTrace(call({
    'x-opencode-session-id': 'ses_opencode_123',
    'x-opencode-run-id': 'msg_user_456',
  })), {
    traceId: 'ses_opencode_123', runId: 'msg_user_456', userId: null, chatId: null,
    sessionId: 'ses_opencode_123', app: null,
  });
});

test('makeTrace: explicit Ladder IDs override native OpenCode IDs; both remain optional', () => {
  const trace = makeTrace(call({
    'x-opencode-session-id': 'ses_opencode_123',
    'x-opencode-run-id': 'msg_user_456',
    'x-ladder-trace': 'external-trace',
    'x-ladder-run': 'external-run',
    'x-ladder-session': 'external-session',
  }));
  assert.deepEqual([trace.traceId, trace.runId, trace.sessionId], [
    'external-trace', 'external-run', 'external-session',
  ]);
  assert.deepEqual(makeTrace(call()), {
    traceId: null, runId: null, userId: null, chatId: null, sessionId: null, app: null,
  });
});

test('makeTrace: native OpenCode IDs are capped to 200 characters', () => {
  const trace = makeTrace(call({
    'x-opencode-session-id': 's'.repeat(250),
    'x-opencode-run-id': 'r'.repeat(250),
  }));
  assert.equal(trace.traceId.length, 200);
  assert.equal(trace.runId.length, 200);
  assert.equal(trace.sessionId.length, 200);
});

test('makeTrace: x-ladder-app (the caller sub-task tag) is read and capped like the ids', () => {
  // #107: this is the only thing that tells apart the ~20 service tools that all post
  // model="service" — without it the cost cut cannot answer "which sub-task spends".
  assert.equal(makeTrace(call({ 'x-ladder-app': 'tg-format' })).app, 'tg-format');
  assert.equal(makeTrace(call({ 'x-ladder-app': '  ' })).app, null);
  assert.equal(makeTrace(call({ 'x-ladder-app': 'y'.repeat(500) })).app.length, 200);
});

test('logCall: no D1 binding → no-op, no throw; db error swallowed', async () => {
  const tracer = { traceId: 't1', runId: null, userId: 'u1', chatId: null, sessionId: null };
  const okRun = { ok: true, model: 'opencode-go/x', data: { usage: { prompt_tokens: 5, completion_tokens: 2 } } };
  await logCall({}, tracer, 'deepseek', okRun, Date.now());
  const d1 = fakeD1();
  d1._fail = true;
  await logCall({ LADDER_TRACE_DB: d1 }, tracer, 'deepseek', okRun, Date.now());
  assert.equal(d1._rows.length, 0);
});

test('logCall: writes a row with trace ids, ladder, attempts, and non-stream usage tokens', async () => {
  const d1 = fakeD1();
  const tracer = { traceId: 'task-777', runId: 'run-2', userId: 'user-alpha', chatId: 'chat-5', sessionId: 'ses-1' };
  const started = Date.now();
  await logCall({ LADDER_TRACE_DB: d1 }, tracer, 'deepseek', {
    ok: true, model: 'opencode-go/mimo', data: { usage: { prompt_tokens: 11, completion_tokens: 4 } },
    attempts: [{ model: 'opencode-go/mimo', outcome: 'ok' }],
  }, started);
  assert.equal(d1._rows.length, 1);
  const r = row(d1._rows[0]);
  assert.equal(r.trace_id, 'task-777');
  assert.equal(r.run_id, 'run-2');
  assert.equal(r.user_id, 'user-alpha');
  assert.equal(r.chat_id, 'chat-5');
  assert.equal(r.session_id, 'ses-1');
  assert.equal(r.ladder, 'deepseek');
  assert.equal(r.ok, 1);
  assert.equal(r.model, 'opencode-go/mimo');
  assert.equal(r.tokens_in, 11);
  assert.equal(r.tokens_out, 4);
  assert.equal(r.tokens_cached, null, 'absence of a cache counter is unknown, not zero');
  assert.equal(r.tokens_cache_write, null, 'absence of a cache-write counter is unknown, not zero');
  assert.equal(r.app, null, 'no x-ladder-app → null, the analytics cut skips the row');
  assert.ok(r.ts >= started);
  assert.equal(JSON.parse(r.attempts)[0].outcome, 'ok');
});

test('logCall normalizes OpenAI, Anthropic and DeepSeek cache usage dialects', async () => {
  const cases = [
    [{ prompt_tokens: 20, completion_tokens: 4, prompt_tokens_details: { cached_tokens: 8 } }, [20, 4, 8, null]],
    [{ input_tokens: 20, output_tokens: 4, cache_read_input_tokens: 8, cache_creation_input_tokens: 3 }, [20, 4, 8, 3]],
    [{ prompt_tokens: 20, completion_tokens: 4, prompt_cache_hit_tokens: 8, prompt_cache_miss_tokens: 3 }, [20, 4, 8, 3]],
  ];
  for (const [usage, expected] of cases) {
    const d1 = fakeD1();
    await logCall({ LADDER_TRACE_DB: d1 }, { traceId: null }, 'free', { ok: true, data: { usage } }, Date.now());
    assert.deepEqual(d1._rows[0].slice(11, 15), expected);
  }
});

test('logCall: the caller sub-task tag lands in the row (#107)', async () => {
  const d1 = fakeD1();
  // The handler overwrites the raw header with the sanitised slug it sends to OpenRouter, so
  // the D1 cut and the OpenRouter "Application" cut can never name the same call differently.
  const tracer = { traceId: null, runId: null, userId: null, chatId: null, sessionId: null, app: 'session-summary' };
  await logCall({ LADDER_TRACE_DB: d1 }, tracer, 'service', {
    ok: true, model: 'opencode-go/mimo-v2.6-flash', data: { usage: { prompt_tokens: 9, completion_tokens: 3 } }, attempts: [],
  }, Date.now());
  assert.equal(row(d1._rows[0]).app, 'session-summary');
});

test('logCall: no trace ids → row still written with nulls; stream call has no usage → null tokens', async () => {
  const d1 = fakeD1();
  const tracer = { traceId: null, runId: null, userId: null, chatId: null, sessionId: null };
  await logCall({ LADDER_TRACE_DB: d1 }, tracer, 'free', {
    ok: true, model: 'opencode-go/mimo', stream: {}, data: undefined, attempts: [],
  }, Date.now());
  assert.equal(d1._rows.length, 1);
  const r = row(d1._rows[0]);
  for (const k of ['trace_id', 'run_id', 'user_id', 'chat_id', 'session_id', 'tokens_in', 'tokens_out']) assert.equal(r[k], null, k);
  assert.equal(r.ok, 1);
});
// ── #136: the router is not an application ──────────────────────────────────
//
// Regression tests for the bug where `trace.app = appSlug` wrote sanitizeAppSlug()'s DEFAULT
// ('llm-ladder') into D1 whenever the caller sent no x-ladder-app. Nothing caught it: both
// existing app tests call logCall() with a hand-built tracer, so they never cross the handler
// line that did the damage — mutating that line to a constant kept all 206 tests green.
//
// These go through handle() so the handler's own assignment is on the path.

const ROUTER_ENV = { LADDER_TOKEN: 't', OPENCODE_GO_API_KEYS: 'oc_a,oc_b', OPENROUTER_API_KEY: 'or_key' };
// An openrouter/* rung, not a Go one: the Go lane's upstream guard rejects a fabricated lane
// outright (503, no upstream call, hence no attribution headers to assert on). This lane reaches
// the provider, so one call exercises BOTH sides of #136 at once — the D1 row and the headers.
const TEST_RUNG = ladders.ladders.service.build.find(m => m.startsWith('openrouter/'));

// One served call through the real handler + real config, with a fake D1 to read the row back.
async function postThroughHandler(headers) {
  const d1 = fakeD1();
  const sent = [];
  const fetchImpl = async (url, init) => {
    sent.push(init.headers || {});
    return { ok: true, status: 200, json: async () => ({ choices: [{ message: { content: 'ok' } }] }), text: async () => '' };
  };
  // Pin the first service rung so the walk never leaves it (no failover, no store rotation).
  // The served model does not matter here: the assertion is about what the HANDLER writes into the
  // trace row, and logCall runs on every outcome — ok or not. The lane returns 503 in this
  // sandbox because the rung's own upstream guard rejects a fabricated lane; the row is still
  // written, which is exactly the code path #136 broke.
  const r = await handle(new Request('https://l.test/v1/chat/completions', {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: 'Bearer t', ...headers },
    body: JSON.stringify({ model: 'service', ladder_rung: TEST_RUNG, messages: [{ role: 'user', content: 'hi' }] }),
  }), { ...ROUTER_ENV, LADDER_TRACE_DB: d1 }, { store: memoryStore(0), fetchImpl });
  return { d1, sent, status: r.status };
}

test('#136: no x-ladder-app → app stays NULL in D1, while OpenRouter still gets the router name', async () => {
  const { d1, sent } = await postThroughHandler({});
  assert.equal(d1._rows.length, 1, 'the call was traced');
  const r = row(d1._rows[0]);
  // The whole point: absent header must NOT become an application name. DEFAULT_APP_SLUG is our
  // own constant — no caller ever sends it — so writing it here let the router's own unattributed
  // traffic outrank every real application in the cut (264 of 466 calls in the first hour).
  assert.equal(r.app, null, 'absent header → null app, NOT the router slug');
  assert.notEqual(r.app, DEFAULT_APP_SLUG, 'the router must never file itself as an application');
  // …and the OpenRouter view must NOT change (#33 regression): upstream still needs a name, and
  // there "no app" honestly IS the generic proxy.
  assert.match(sent[0]['HTTP-Referer'], new RegExp(`${DEFAULT_APP_SLUG}$`), 'upstream still gets the router referer');
});

test('#136: a real slug reaches D1; a garbage slug reaches D1 as null but upstream as the default', async () => {
  const ok = await postThroughHandler({ 'x-ladder-app': 'tg-format' });
  assert.equal(row(ok.d1._rows[0]).app, 'tg-format', 'sanitised slug is stored');

  const junk = await postThroughHandler({ 'x-ladder-app': 'GTD Intent/../../etc' });
  assert.equal(row(junk.d1._rows[0]).app, null, 'a slug we would have to repair is not an app name');
  assert.match(junk.sent[0]['HTTP-Referer'], new RegExp(`${DEFAULT_APP_SLUG}$`), 'upstream falls back to the router');
});

test('#136: sanitizeAppSlugOrNull agrees with sanitizeAppSlug on WHAT is a slug, differing only in the fallback', async () => {
  assert.equal(sanitizeAppSlugOrNull('gtd-intent'), 'gtd-intent');
  assert.equal(sanitizeAppSlugOrNull(' HH-Messages '), 'hh-messages', 'same trim/lowercase as the OpenRouter side');
  for (const junk of ['', '  ', 'привет', 'a b', 'a/b', '../../etc/passwd', 'a'.repeat(65), 'a_b', 'a\nb', null, undefined]) {
    assert.equal(sanitizeAppSlugOrNull(junk), null, JSON.stringify(junk));
  }
});
