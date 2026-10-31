import { test } from 'node:test';
import assert from 'node:assert/strict';
import { run } from '../src/ladder.js';
import { handle } from '../src/handler.js';
import { memoryStore } from '../src/state.js';

const free = 'opencode-go/example-free';
const paid = 'opencode-go/example-paid';
const config = { ladders: { service: { build: [free, paid] } } };
const env = { OPENCODE_GO_API_KEYS: 'key-a,key-b', LADDER_TOKEN: 'test-token', OPENROUTER_API_KEY: 'or-key' };
const compact = [{ role: 'user', content: 'compact' }];
const masked = [{ role: 'user', content: 'masked-prefix' }];

function fakeFetch(seen, behaviour = {}) {
  return async (_url, init) => {
    const body = JSON.parse(init.body);
    seen.push({ body, key: init.headers.Authorization });
    const status = behaviour[body.model]?.(init.headers.Authorization) ?? 200;
    if (status !== 200) return new Response('provider unavailable', { status });
    if (body.stream) return new Response('data: {"choices":[{"delta":{"content":"ok"}}]}\n\ndata: [DONE]\n\n', {
      headers: { 'content-type': 'text/event-stream' },
    });
    return Response.json({ choices: [{ message: { role: 'assistant', content: 'ok' } }], usage: { prompt_tokens: 2 } });
  };
}

test('a ladder_cache_variant block is ignored and the request is served', async () => {
  const seen = [];
  const result = await run({ model: 'service', messages: compact, ladder_cache_variant: {
    rung: 'opencode-go/retired-free', cache_scope: 'invalid', messages: masked,
  } }, {
    env, config, store: memoryStore(2), fetchImpl: fakeFetch(seen),
  });
  assert.equal(result.ok, true);
  assert.deepEqual(seen.map((call) => call.body.messages), [compact]);
  assert.ok(seen.every((call) => !Object.hasOwn(call.body, 'ladder_cache_variant')));
});

test('an unusable ladder_cache_variant no longer fails the request', async () => {
  const seen = [];
  const request = new Request('https://ladder.test/v1/chat/completions', {
    method: 'POST',
    headers: { authorization: 'Bearer test-token', 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'service', messages: compact, ladder_cache_variant: {
      rung: 'openrouter/other', cache_scope: 'bad', messages: masked } }),
  });
  const response = await handle(request, env, { store: memoryStore(2), fetchImpl: fakeFetch(seen) });
  assert.equal(response.status, 200);
  assert.equal(seen.length > 0, true);
  assert.ok(seen.every((call) => !Object.hasOwn(call.body, 'ladder_cache_variant')));
  assert.deepEqual(seen[0].body.messages, compact);
});

test('HTTP strips the block upstream and reports no cache variant', async () => {
  const seen = [];
  const httpPaid = 'opencode-go/mimo-v2.6-flash';
  const request = new Request('https://ladder.test/v1/chat/completions', {
    method: 'POST',
    headers: { authorization: 'Bearer test-token', 'content-type': 'application/json', 'x-session-id': 'session-a' },
    body: JSON.stringify({ model: 'service', ladder_rung: httpPaid, stream: true, messages: compact,
      ladder_cache_variant: { rung: httpPaid, cache_scope: `v1_${'0'.repeat(32)}`, messages: masked } }),
  });
  const response = await handle(request, env, { store: memoryStore(2), fetchImpl: fakeFetch(seen) });
  assert.equal(response.status, 200);
  assert.equal(response.headers.get('x-ladder-model'), httpPaid);
  assert.equal(response.headers.get('x-ladder-cache-variant'), null);
  assert.equal(response.headers.get('x-ladder-cache-scope'), null);
  assert.deepEqual(seen[0].body.messages, compact);
  assert.equal(Object.hasOwn(seen[0].body, 'ladder_cache_variant'), false);
});
