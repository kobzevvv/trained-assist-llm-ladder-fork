import { test } from 'node:test';
import assert from 'node:assert/strict';
import { run } from '../src/ladder.js';
import { memoryStore } from '../src/state.js';

const free = 'opencode-go/beam-free';
const paid = 'opencode-go/mimo-v2.6-flash';
const config = { ladders: { service: { build: [free, paid] } } };

function completion(content) {
  return { id: 'test-id', object: 'chat.completion', created: 1,
    choices: [{ index: 0, finish_reason: 'stop', message: { role: 'assistant', content } }] };
}

// Scenario 1: Optimizer succeeds (all map calls return summaries)
test('staging: optimizer succeeds → reduced prompt reaches ladder', async () => {
  const env = { OPENCODE_GO_API_KEYS: 'key1', LADDER_CONTEXT_OPTIMIZER_LIMIT: '5000' };
  const question = 'what about caching?';
  const history = Array.from({ length: 40 }, (_, i) => ({ role: 'assistant', content: `turn ${i}: ${'x'.repeat(700)}` }));
  const input = { model: 'service', messages: [{ role: 'system', content: 'brief' }, ...history, { role: 'user', content: question }] };
  const seen = [];
  const progressEvents = [];

  const result = await run(input, {
    env, config, store: memoryStore(),
    onProgress: (e) => progressEvents.push(e),
    fetchImpl: async (_url, init) => {
      const body = JSON.parse(init.body);
      seen.push(body);
      const firstMsg = body.messages[0]?.content || '';
      if (firstMsg.includes('context filter')) {
        return new Response(JSON.stringify(completion('SUMMARY: decided on LRU cache')), { status: 200 });
      }
      return new Response(JSON.stringify(completion('final answer')), { status: 200 });
    }
  });

  assert.equal(result.ok, true);
  const mapCalls = seen.filter(b => b.messages[0]?.content?.includes('context filter'));
  const finalCalls = seen.filter(b => !b.messages[0]?.content?.includes('context filter'));
  assert.ok(mapCalls.length >= 2, `expected multiple map calls, got ${mapCalls.length}`);
  assert.equal(finalCalls.length, 1);
  assert.ok(finalCalls[0].messages.length < input.messages.length, 'final prompt is smaller');
  assert.ok(finalCalls[0].messages.some(m => m.content?.includes('SUMMARY')));
  assert.ok(finalCalls[0].messages.some(m => m.content === question), 'question preserved');

  const optEvent = progressEvents.find(e => e.type === 'context_optimized');
  assert.ok(optEvent, 'context_optimized progress event emitted');
  assert.equal(optEvent.rung, free);
  assert.ok(optEvent.tokens_before > 5000, 'tokens_before reflects original size');
  assert.ok(optEvent.tokens_after < optEvent.tokens_before, 'tokens_after is smaller');
  assert.equal(optEvent.threshold, 5000);

  // The logged input size (race band, ttfb factor, trace) must reflect the SHRUNK
  // prompt, not the original: otherwise a 2K optimized prompt would still race on
  // the fat-prompt band (the exact bug behind incident 2026-10-28 / #341).
  assert.ok(result.estimated_tokens_in < 2000,
    `estimated_tokens_in should be the reduced size, got ${result.estimated_tokens_in}`);
});

// Scenario 2: All map calls fail → fail-open to original
test('staging: all map calls fail → fail-open to original body', async () => {
  const env = { OPENCODE_GO_API_KEYS: 'key1', LADDER_CONTEXT_OPTIMIZER_LIMIT: '5000' };
  const history = Array.from({ length: 40 }, (_, i) => ({ role: 'assistant', content: `turn ${i}: ${'x'.repeat(700)}` }));
  const input = { model: 'service', messages: [{ role: 'system', content: 'brief' }, ...history, { role: 'user', content: 'q?' }] };
  const seen = [];
  const progressEvents = [];

  const result = await run(input, {
    env, config, store: memoryStore(),
    onProgress: (e) => progressEvents.push(e),
    fetchImpl: async (_url, init) => {
      const body = JSON.parse(init.body);
      seen.push(body);
      const firstMsg = body.messages[0]?.content || '';
      if (firstMsg.includes('context filter')) {
        return new Response(JSON.stringify({ error: { message: 'provider down' } }), { status: 500 });
      }
      return new Response(JSON.stringify(completion('final answer')), { status: 200 });
    }
  });

  assert.equal(result.ok, true);
  const mapCalls = seen.filter(b => b.messages[0]?.content?.includes('context filter'));
  assert.ok(mapCalls.length >= 1, 'map was attempted');
  // Final call should have ORIGINAL messages (fail-open)
  const finalCall = seen.find(b => !b.messages[0]?.content?.includes('context filter'));
  assert.ok(finalCall, 'final call happened');
  assert.ok(finalCall.messages.length >= input.messages.length - 1, 'original body preserved (or close)');
  assert.ok(!progressEvents.find(e => e.type === 'context_optimized'), 'no progress event on fail-open');
});

// Scenario 3: Some map calls fail → optimization succeeds with partial summaries
test('staging: partial map failure → optimization succeeds', async () => {
  const env = { OPENCODE_GO_API_KEYS: 'key1', LADDER_CONTEXT_OPTIMIZER_LIMIT: '5000' };
  const history = Array.from({ length: 40 }, (_, i) => ({ role: 'assistant', content: `turn ${i}: ${'x'.repeat(700)}` }));
  const input = { model: 'service', messages: [{ role: 'system', content: 'brief' }, ...history, { role: 'user', content: 'q?' }] };
  const seen = [];
  const mapCount = { n: 0 };

  const result = await run(input, {
    env, config, store: memoryStore(),
    fetchImpl: async (_url, init) => {
      const body = JSON.parse(init.body);
      seen.push(body);
      const firstMsg = body.messages[0]?.content || '';
      if (firstMsg.includes('context filter')) {
        mapCount.n++;
        if (mapCount.n === 2) return new Response(JSON.stringify({ error: { message: 'flaky' } }), { status: 503 });
        return new Response(JSON.stringify(completion('SUMMARY: relevant')), { status: 200 });
      }
      return new Response(JSON.stringify(completion('final answer')), { status: 200 });
    }
  });

  assert.equal(result.ok, true);
  const finalCall = seen.find(b => !b.messages[0]?.content?.includes('context filter'));
  assert.ok(finalCall.messages.length < input.messages.length, 'optimization succeeded despite partial failure');
  assert.ok(finalCall.messages.some(m => m.content?.includes('SUMMARY')));
});

// Scenario 4: Input under threshold → no optimization
test('staging: input under threshold → no optimizer triggered', async () => {
  const env = { OPENCODE_GO_API_KEYS: 'key1', LADDER_CONTEXT_OPTIMIZER_LIMIT: '100000' };
  const history = Array.from({ length: 3 }, (_, i) => ({ role: 'assistant', content: `turn ${i}` }));
  const input = { model: 'service', messages: [{ role: 'system', content: 'brief' }, ...history, { role: 'user', content: 'q?' }] };
  const seen = [];

  const result = await run(input, {
    env, config, store: memoryStore(),
    fetchImpl: async (_url, init) => {
      seen.push(JSON.parse(init.body));
      return new Response(JSON.stringify(completion('answer')), { status: 200 });
    }
  });

  assert.equal(result.ok, true);
  const mapCalls = seen.filter(b => b.messages[0]?.content?.includes('context filter'));
  assert.equal(mapCalls.length, 0, 'no map calls when under threshold');
  assert.ok(seen.length >= 1);
});

// Scenario 5: No free rung available → no optimization attempt
test('staging: no free Go rung in ladder → optimizer skipped', async () => {
  const env = { OPENCODE_GO_API_KEYS: 'key1', LADDER_CONTEXT_OPTIMIZER_LIMIT: '1000' };
  const noFreeConfig = { ladders: { service: { build: [paid] } } };
  const history = Array.from({ length: 30 }, (_, i) => ({ role: 'assistant', content: `turn ${i}: ${'x'.repeat(500)}` }));
  const input = { model: 'service', messages: [{ role: 'system', content: 'brief' }, ...history, { role: 'user', content: 'q?' }] };
  const seen = [];

  const result = await run(input, {
    env, config: noFreeConfig, store: memoryStore(),
    fetchImpl: async (_url, init) => {
      seen.push(JSON.parse(init.body));
      return new Response(JSON.stringify(completion('answer')), { status: 200 });
    }
  });

  assert.equal(result.ok, true);
  const mapCalls = seen.filter(b => b.messages[0]?.content?.includes('context filter'));
  assert.equal(mapCalls.length, 0, 'no map calls without free rung');
});
