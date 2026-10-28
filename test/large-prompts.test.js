import { test } from 'node:test';
import assert from 'node:assert/strict';
import { run } from '../src/ladder.js';
import { handle } from '../src/handler.js';
import { memoryStore } from '../src/state.js';

const free = 'opencode-go/beam-free';
const paid = 'opencode-go/mimo-v2.6-flash';
const fallback = 'openrouter/example/fallback:free';
const config = { ladders: { service: { build: [free, paid, fallback] } } };
const env = { OPENCODE_GO_API_KEYS: 'test-go-key', OPENROUTER_API_KEY: 'test-router-key' };

function bodyOfSize(bytes) {
  const body = { model: 'service', messages: [
    { role: 'system', content: 'Keep the full context.' },
    { role: 'user', content: '' },
  ] };
  const size = value => new TextEncoder().encode(JSON.stringify(value)).length;
  body.messages[1].content = 'x'.repeat(bytes - size(body));
  assert.equal(size(body), bytes);
  return body;
}

function completion(content = 'done') {
  return { id: 'test-id', object: 'chat.completion', created: 1,
    choices: [{ index: 0, finish_reason: 'stop', message: { role: 'assistant', content } }] };
}

test('large requests pass unchanged through free and paid rungs', async () => {
  for (const bytes of [100_000, 350_001]) {
    const input = bodyOfSize(bytes);
    const calls = [];
    const result = await run(input, { env, config, store: memoryStore(), fetchImpl: async (_url, init) => {
      const outgoing = JSON.parse(init.body);
      calls.push(outgoing);
      return new Response(JSON.stringify(completion()), { status: calls.length === 1 ? 503 : 200 });
    } });
    assert.equal(result.ok, true);
    assert.equal(result.model, paid);
    assert.deepEqual(calls.map(call => call.model), [free, paid].map(model => model.split('/')[1]));
    for (const call of calls) assert.deepEqual(call.messages, input.messages);
    assert.equal(Object.hasOwn(result, 'compression'), false);
  }
});

// 3.7 МБ ≈ 935K токенов — за новым потолком 900K для opencode-go, поэтому оба Go-ранга
// (free и paid) пропускаются и запрос едет на OpenRouter-фолбек без изменений. Суть гейта —
// payload НЕ переписывается, а не то, что отвечает Go-ранг.
test('3.7 MB Go request rides the OpenRouter fallback rung unchanged instead of being sent to the Go rungs', async () => {
  const input = bodyOfSize(3_700_000);
  const calls = [];
  const result = await run(input, { env, config, store: memoryStore(), fetchImpl: async (_url, init) => {
    calls.push(JSON.parse(init.body));
    return new Response(JSON.stringify(completion()));
  } });
  assert.equal(result.ok, true);
  assert.equal(result.model, fallback);
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].messages, input.messages);
  assert.deepEqual(result.attempts.map(attempt => attempt.outcome), ['skipped', 'skipped', 'ok']);
});

test('HTTP endpoint drops the retired compression flag and returns no compression header', async () => {
  const input = { ...bodyOfSize(350_001), ladder_rung: free, ladder_context_compression: true };
  const calls = [];
  const response = await handle(new Request('https://ladder.example/v1/chat/completions', {
    method: 'POST', headers: { authorization: 'Bearer test-token', 'content-type': 'application/json', 'x-ladder-context-compression': 'on' },
    body: JSON.stringify(input),
  }), { ...env, LADDER_TOKEN: 'test-token' }, { store: memoryStore(), fetchImpl: async (_url, init) => {
    calls.push(JSON.parse(init.body));
    return new Response(JSON.stringify(completion()));
  } });
  assert.equal(response.status, 200);
  assert.equal(response.headers.get('x-ladder-compression'), null);
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].messages, input.messages);
  assert.equal(Object.hasOwn(calls[0], 'ladder_context_compression'), false);
});

// Pre-Ladder Context Optimization (#331): a big HISTORY + a small fresh question is split and
// summarized by the first free Go rung BEFORE the ladder tries its first step. The fetchImpl
// distinguishes map calls (the "context filter" prompt) from the final answer call.
test('oversized history is summarized before the first rung, and the reduced prompt reaches the free rung', async () => {
  const question = 'what did we decide about the cache?';
  // ~100 assistant turns of 4 KB each ≈ 400 KB ≈ 100K tokens of history.
  // Force optimizer with a small limit via env override (staging mode).
  const testEnv = { ...env, LADDER_CONTEXT_OPTIMIZER_LIMIT: '20000' };
  const history = Array.from({ length: 100 }, (_, i) => ({ role: 'assistant', content: `turn ${i}: ${'x'.repeat(4000)}` }));
  const input = { model: 'service', messages: [
    { role: 'system', content: 'be concise' },
    ...history,
    { role: 'user', content: question },
  ] };
  const seen = [];
  const result = await run(input, { env: testEnv, config, store: memoryStore(), fetchImpl: async (_url, init) => {
    const outgoing = JSON.parse(init.body);
    seen.push(outgoing);
    const firstMsg = outgoing.messages[0] && outgoing.messages[0].content;
    if (typeof firstMsg === 'string' && firstMsg.includes('context filter')) {
      return new Response(JSON.stringify(completion('SUMMARY: decided to cache by key')), { status: 200 });
    }
    return new Response(JSON.stringify(completion('final answer')), { status: 200 });
  } });

  assert.equal(result.ok, true);
  const mapCalls = seen.filter(o => typeof o.messages[0]?.content === 'string' && o.messages[0].content.includes('context filter'));
  const finalCalls = seen.filter(o => !(typeof o.messages[0]?.content === 'string' && o.messages[0].content.includes('context filter')));
  assert.ok(mapCalls.length > 1, `expected multiple map calls, got ${mapCalls.length}`);
  assert.equal(finalCalls.length, 1, 'after optimization exactly one final call reaches the ladder');
  // The final call is small and carries the summaries + the verbatim question.
  const finalMessages = finalCalls[0].messages;
  assert.ok(new TextEncoder().encode(JSON.stringify(finalMessages)).length < 80_000, 'reduced prompt is under the threshold');
  assert.equal(finalMessages.find(m => m.role === 'user').content, question);
  assert.ok(finalMessages.some(m => m.role === 'system' && m.content.includes('decided to cache by key')), 'summary survived into the final prompt');
});
