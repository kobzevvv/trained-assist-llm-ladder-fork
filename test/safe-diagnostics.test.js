import { test } from 'node:test';
import assert from 'node:assert/strict';
import { handle } from '../src/handler.js';
import { memoryStore } from '../src/state.js';
import { safeAttemptDiagnostics } from '../src/diagnostics.js';

test('safe attempt diagnostics preserve provider source and status without provider text', () => {
  const diagnostics = safeAttemptDiagnostics([
    { model: 'openrouter/example/model:free', outcome: 'error', key: 2,
      error: 'HTTP 503: body contains private prompt and upstream details' },
    { model: 'opencode-go/model', outcome: 'error', error: 'fetch failed: request timed out with private context' },
    { model: 'zen-rings/model', outcome: 'error', error: 'zen-rings: warming up (3s left), private detail' },
  ]);
  assert.deepEqual(diagnostics, [
    { model: 'openrouter/example/model:free', outcome: 'error', source: 'provider',
      reason_code: 'provider_http_error', http_status: 503, error: 'provider returned HTTP 503' },
    { model: 'opencode-go/model', outcome: 'error', source: 'provider',
      reason_code: 'provider_timeout', error: 'provider timed out' },
    { model: 'zen-rings/model', outcome: 'error', source: 'zen_pool',
      reason_code: 'provider_capacity', error: 'Zen worker capacity unavailable' },
  ]);
  assert.equal(JSON.stringify(diagnostics).includes('private'), false);
  assert.equal(JSON.stringify(diagnostics).includes('key'), false);
});

test('real chat handler returns a stable provider outage and stores only sanitized diagnostics', async () => {
  const traceRows = [];
  const traceDb = { prepare() {
    const statement = { bind(...values) { statement.values = values; return statement; },
      async run() { traceRows.push(statement.values); return { success: true }; } };
    return statement;
  } };
  const env = { LADDER_TOKEN: 'ladder-test', OPENROUTER_API_KEY: 'not-a-real-key', LADDER_TRACE_DB: traceDb };
  const events = [];
  const log = console.log;
  const logged = [];
  console.log = (line) => logged.push(String(line));
  try {
    const response = await handle(new Request('https://ladder.test/v1/chat/completions', {
      method: 'POST',
      headers: { authorization: 'Bearer ladder-test', 'content-type': 'application/json' },
      body: JSON.stringify({
        model: 'free',
        ladder_rung: 'openrouter/nvidia/nemotron-3-super-120b-a12b:free',
        messages: [{ role: 'user', content: 'synthetic timeout check' }],
      }),
    }), env, {
      store: memoryStore(10),
      events: { publishError: async (event) => events.push(event) },
      fetchImpl: async () => {
        throw new Error('synthetic timeout after PRIVATE_UPSTREAM_BODY and synthetic timeout check');
      },
    });
    assert.equal(response.status, 502);
    const body = await response.json();
    assert.equal(body.error.message, 'every rung failed');
    assert.equal(body.error.reason_code, 'provider_unavailable');
    assert.equal(body.error.service_status, 'operational');
    assert.equal(body.error.attempts.length, 1, 'pinned verification measures exactly one provider attempt');
    assert.deepEqual(body.error.attempts.map((attempt) => attempt.outcome), ['error']);
    for (const attempt of body.error.attempts) assert.deepEqual(attempt, {
      model: 'openrouter/nvidia/nemotron-3-super-120b-a12b:free', outcome: attempt.outcome,
      source: 'provider', reason_code: 'provider_timeout', error: 'provider timed out',
    });
    const storedAttempts = JSON.parse(traceRows[0][10]);
    assert.deepEqual(storedAttempts, body.error.attempts, 'durable call traces get only safe diagnostics');
    const serialized = JSON.stringify({ body, logged, events, storedAttempts });
    assert.equal(serialized.includes('PRIVATE_UPSTREAM_BODY'), false);
    assert.equal(serialized.includes('synthetic timeout check'), false);
    assert.equal(serialized.includes('not-a-real-key'), false);
    assert.equal(logged.length, 1, 'one sanitized terminal record is emitted');
    assert.equal(JSON.parse(logged[0]).attempts[0].reason_code, 'provider_timeout');
  } finally {
    console.log = log;
  }
});

// #339: причины, которые генерирует САМА лестница, — это не текст провайдера. До фикса они
// маскировались в `provider request failed`: в D1 за 26 ч было 2217 масок и НОЛЬ строк с
// реальной причиной, а ветки SIZE_BYTES/TOKEN_CEILING/CONTEXT в npm run diagnose не срабатывали.
test('собственные причины лестницы доезжают дословно и несут source=ladder', () => {
  const own = [
    ['time budget spent', 'ladder_time_budget'],
    ['every OpenRouter account is parked until its limit resets', 'openrouter_pool_parked'],
    ['input ~167319t above the rung ceiling', 'input_above_rung_ceiling'],
    ['zen-rings: input is too long for the free tier: 1509343 bytes, limit 400000', 'input_too_long_for_free_tier'],
    ['input is too long for the ring payload', 'input_too_long_for_ring_payload'],
  ];
  for (const [message, reason] of own) {
    const out = safeAttemptDiagnostics([{ model: 'opencode-go/beam-free', outcome: 'skipped', key: 0, error: message }]);
    assert.deepEqual(out[0], {
      model: 'opencode-go/beam-free', outcome: 'skipped',
      source: 'ladder', reason_code: reason, error: message,
    }, `причина сохраняется дословно: ${message}`);
    assert.equal(JSON.stringify(out[0]).includes('key'), false, 'ключ провайдера не течёт');
  }
});

// #339: /v1/calls и /v1/analytics прогоняют через санитайзер то, что уже лежит в D1. Без
// идемпотентности `provider returned HTTP 429` деградировало в `provider request failed`,
// http_status терялся, и /v1/analytics сводил 400/402/403/429 к двум бакетам.
test('повторный проход по уже санированным попыткам ничего не теряет', () => {
  const stored = [
    { model: 'opencode-go/mimo-v2.6-flash', outcome: 'error', source: 'provider',
      reason_code: 'provider_http_error', http_status: 429, error: 'provider returned HTTP 429' },
    { model: 'opencode-go/beam-free', outcome: 'error', source: 'provider',
      reason_code: 'provider_http_error', http_status: 400, error: 'provider returned HTTP 400' },
    { model: 'zen-rings/x', outcome: 'error', source: 'zen_pool',
      reason_code: 'provider_capacity', error: 'Zen worker capacity unavailable' },
    { model: 'opencode-go/x', outcome: 'error', source: 'provider',
      reason_code: 'provider_empty_response', error: 'provider returned no usable answer' },
    { model: 'opencode-go/x', outcome: 'error', source: 'provider',
      reason_code: 'provider_transport_error', error: 'provider connection failed' },
    { model: 'opencode-go/x', outcome: 'skipped', source: 'ladder',
      reason_code: 'input_above_rung_ceiling', error: 'input ~167319t above the rung ceiling' },
    { model: 'opencode-go/x', outcome: 'ok' },
  ];
  for (const attempt of stored) {
    const once = safeAttemptDiagnostics([attempt]);
    assert.deepEqual(once[0], attempt, `идемпотентно: ${attempt.error || attempt.outcome}`);
    assert.deepEqual(safeAttemptDiagnostics(once)[0], attempt, `и на третий раз: ${attempt.error || attempt.outcome}`);
  }
});

// Сквозная проверка: пропуск по бюджету — это отказ ЛЕСТНИЦЫ, а не провайдера. Провайдер не
// вызывался вообще, поэтому потребитель должен получить ladder_unavailable, а не
// provider_unavailable (до фикса было ровно наоборот).
test('502 от пропуска по бюджету несёт ladder_unavailable и дословную причину', async () => {
  const traceRows = [];
  const traceDb = { prepare() {
    const statement = { bind(...values) { statement.values = values; return statement; },
      async run() { traceRows.push(statement.values); return { success: true }; },
      async all() { return { results: [] }; },
      async first() { return null; } };
    return statement;
  } };
  const env = { LADDER_TOKEN: 'ladder-test', OPENROUTER_API_KEY: 'not-a-real-key', LADDER_TRACE_DB: traceDb };
  const log = console.log;
  const logged = [];
  console.log = (line) => logged.push(String(line));
  try {
    const response = await handle(new Request('https://ladder.test/v1/chat/completions', {
      method: 'POST',
      headers: { authorization: 'Bearer ladder-test', 'content-type': 'application/json' },
      // Бюджет 1 мс истекает до первой ступени → run() отдаёт `time budget spent` и не зовёт провайдера.
      body: JSON.stringify({ model: 'free', ladder_total_timeout_ms: 1,
        messages: [{ role: 'user', content: 'hi' }] }),
    }), env, { store: memoryStore(10), fetchImpl: async () => { throw new Error('провайдер не должен вызываться'); } });

    assert.equal(response.status, 502);
    const body = await response.json();
    assert.equal(body.error.message, 'every rung failed');
    assert.equal(body.error.reason_code, 'ladder_unavailable',
      'ни одна попытка не была к провайдеру → причина в лестнице, а не в провайдере');
    assert.deepEqual(body.error.attempts.map(a => a.error), ['time budget spent'], 'причина доезжает дословно');
    assert.deepEqual(body.error.attempts.map(a => a.source), ['ladder']);
    const stored = JSON.parse(traceRows[0][10]);
    assert.deepEqual(stored, body.error.attempts, 'в D1 пишется та же дословная причина');
    assert.equal(JSON.stringify(body).includes('provider request failed'), false, 'маска не попадает в ответ');
  } finally {
    console.log = log;
  }
});
