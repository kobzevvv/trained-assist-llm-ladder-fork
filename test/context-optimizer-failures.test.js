import { test } from 'node:test';
import assert from 'node:assert/strict';
import { optimizeContext, chunkMessages, getLastUserQuestion } from '../src/context-optimizer.js';

// Build a body with N chunks worth of history (each chunk = ~100 tokens at limit=100).
function bodyWithChunks(numChunks) {
  const messages = [{ role: 'system', content: 'be brief' }];
  for (let c = 0; c < numChunks; c++) {
    for (let i = 0; i < 5; i++) {
      messages.push({ role: 'assistant', content: `chunk${c} turn${i}: ${'x'.repeat(120)}` });
    }
  }
  messages.push({ role: 'user', content: 'the question' });
  return { model: 'service', messages };
}

test('partial failure: 1 of 3 map calls fails → succeeds with remaining 2 summaries', async () => {
  const body = bodyWithChunks(3); // ~3 chunks at limit 100
  let callCount = 0;
  const attempt = async () => {
    callCount++;
    if (callCount === 2) return { ok: false, error: 'HTTP 500: internal' };
    return { ok: true, content: `SUMMARY ${callCount}: relevant info` };
  };
  const out = await optimizeContext(body, {}, async () => {}, 'opencode-go/beam-free', attempt, 100);
  assert.ok(out !== body, 'optimization should succeed despite partial failure');
  const summaryMsg = out.messages.find(m => m.content?.includes('Relevant context summaries'));
  assert.ok(summaryMsg, 'summaries message should exist');
  assert.ok(summaryMsg.content.includes('SUMMARY 1'), 'first summary survived');
  assert.ok(!summaryMsg.content.includes('SUMMARY 2'), 'failed summary excluded');
  assert.ok(summaryMsg.content.includes('SUMMARY 3'), 'third summary survived');
});

test('partial failure: 2 of 3 map calls fail → still succeeds with 1 summary', async () => {
  const body = bodyWithChunks(3);
  let callCount = 0;
  const attempt = async () => {
    callCount++;
    if (callCount === 1 || callCount === 3) return { ok: false, error: 'timeout' };
    return { ok: true, content: 'SUMMARY 2: the only good one' };
  };
  const out = await optimizeContext(body, {}, async () => {}, 'opencode-go/beam-free', attempt, 100);
  assert.ok(out !== body, 'optimization succeeds with 1 surviving summary');
  const summaryMsg = out.messages.find(m => m.content?.includes('Relevant context summaries'));
  assert.ok(summaryMsg.content.includes('SUMMARY 2'));
});

test('all map calls fail → fail-open to original body', async () => {
  const body = bodyWithChunks(3);
  const attempt = async () => ({ ok: false, error: 'all providers down' });
  const out = await optimizeContext(body, {}, async () => {}, 'opencode-go/beam-free', attempt, 100);
  assert.equal(out, body, 'fail-open returns original body when all map calls fail');
});

test('map call throws exception (not returns error) → treated as failure, fail-open works', async () => {
  const body = bodyWithChunks(2);
  const attempt = async () => { throw new Error('network crashed'); };
  const out = await optimizeContext(body, {}, async () => {}, 'opencode-go/beam-free', attempt, 100);
  assert.equal(out, body, 'exception in attempt → fail-open');
});

test('mixed: some throw, some return ok:false → fail-open if none succeed', async () => {
  const body = bodyWithChunks(3);
  let n = 0;
  const attempt = async () => {
    n++;
    if (n === 1) throw new Error('connection reset');
    return { ok: false, error: 'HTTP 429' };
  };
  const out = await optimizeContext(body, {}, async () => {}, 'opencode-go/beam-free', attempt, 100);
  assert.equal(out, body);
});

test('map returns ok but empty content → excluded from summaries', async () => {
  const body = bodyWithChunks(2);
  const attempt = async () => ({ ok: true, content: '' });
  const out = await optimizeContext(body, {}, async () => {}, 'opencode-go/beam-free', attempt, 100);
  assert.equal(out, body, 'empty summaries → fail-open');
});

test('map returns IRRELEVANT → excluded, but other relevant summaries keep optimization alive', async () => {
  const body = bodyWithChunks(3);
  let n = 0;
  const attempt = async () => {
    n++;
    if (n === 2) return { ok: true, content: 'IRRELEVANT' };
    return { ok: true, content: `SUMMARY ${n}: relevant` };
  };
  const out = await optimizeContext(body, {}, async () => {}, 'opencode-go/beam-free', attempt, 100);
  assert.ok(out !== body, 'optimization survives when 1 chunk is irrelevant');
  const summaryMsg = out.messages.find(m => m.content?.includes('Relevant context summaries'));
  assert.ok(summaryMsg.content.includes('SUMMARY 1'));
  assert.ok(!summaryMsg.content.includes('IRRELEVANT'), 'IRRELEVANT marker excluded');
  assert.ok(summaryMsg.content.includes('SUMMARY 3'));
});

test('attempt returns non-ok with guard flag → excluded', async () => {
  const body = bodyWithChunks(2);
  const attempt = async () => ({ ok: false, guard: true, error: 'empty answer (finish=length)' });
  const out = await optimizeContext(body, {}, async () => {}, 'opencode-go/beam-free', attempt, 100);
  assert.equal(out, body, 'guard failures count as map failures → fail-open');
});

test('timeout in attempt (returns error:timeout) → excluded, fail-open if all timeout', async () => {
  const body = bodyWithChunks(2);
  const attempt = async () => ({ ok: false, error: 'fetch failed: timeout' });
  const out = await optimizeContext(body, {}, async () => {}, 'opencode-go/beam-free', attempt, 100);
  assert.equal(out, body);
});

// Edge case: chunkMessages with exactly 1 chunk → optimizeContext short-circuits
test('single chunk (no split needed) → returns original immediately, attempt never called', async () => {
  const body = { messages: [{ role: 'user', content: 'hi' }] };
  let called = false;
  const attempt = async () => { called = true; return { ok: true, content: 'SUMMARY' }; };
  const out = await optimizeContext(body, {}, async () => {}, 'opencode-go/beam-free', attempt, 100_000);
  assert.equal(out, body);
  assert.equal(called, false, 'attempt should not be called for single chunk');
});

// Edge case: 2 chunks but BOTH return IRRELEVANT → no summaries → fail-open
test('all chunks IRRELEVANT → fail-open', async () => {
  const body = bodyWithChunks(2);
  const attempt = async () => ({ ok: true, content: 'IRRELEVANT' });
  const out = await optimizeContext(body, {}, async () => {}, 'opencode-go/beam-free', attempt, 100);
  assert.equal(out, body);
});

// Sequential execution: verify map calls happen one after another (not parallel)
test('map calls are sequential (not parallel)', async () => {
  const body = bodyWithChunks(3);
  const timeline = [];
  const attempt = async () => {
    timeline.push({ start: Date.now(), id: timeline.length });
    await new Promise(r => setTimeout(r, 30));
    timeline[timeline.length - 1].end = Date.now();
    return { ok: true, content: `SUMMARY ${timeline.length}` };
  };
  const out = await optimizeContext(body, {}, async () => {}, 'opencode-go/beam-free', attempt, 100);
  assert.ok(out !== body);
  assert.ok(timeline.length >= 2, `expected at least 2 map calls, got ${timeline.length}`);
  // Verify no overlap: each start >= previous end
  for (let i = 1; i < timeline.length; i++) {
    assert.ok(timeline[i].start >= timeline[i - 1].end,
      `call ${i} started at ${timeline[i].start} but previous ended at ${timeline[i - 1].end}`);
  }
});
