import { test } from 'node:test';
import assert from 'node:assert/strict';
import { estimateTokens, chunkMessages, getLastUserQuestion, optimizeContext } from '../src/context-optimizer.js';

function completion(content) {
  return { id: 'test-id', object: 'chat.completion', created: 1,
    choices: [{ index: 0, finish_reason: 'stop', message: { role: 'assistant', content } }] };
}

// A fake attempt that returns a summary for every chunk and records how it was called.
function summaryAttempt(records, prefix = 'SUMMARY: relevant') {
  return async (_env, _model, body, _keyIndex, _opts) => {
    records.push(body);
    return { ok: true, content: `${prefix} ${body.messages[0].content.slice(0, 40)}` };
  };
}

test('getLastUserQuestion: returns the LAST user message, not the first', () => {
  const messages = [
    { role: 'system', content: 'sys' },
    { role: 'user', content: 'first question' },
    { role: 'assistant', content: 'answer' },
    { role: 'user', content: 'second question' },
  ];
  assert.equal(getLastUserQuestion(messages), 'second question');
});

test('getLastUserQuestion: empty array → empty string', () => {
  assert.equal(getLastUserQuestion([]), '');
});

test('chunkMessages: splits into chunks under the token limit without splitting a message', () => {
  // Each message is ~100 chars → ~25 tokens. limit 100 → ~4 messages per chunk.
  const messages = Array.from({ length: 10 }, (_, i) => ({ role: 'assistant', content: `m${i}-${'x'.repeat(80)}` }));
  const chunks = chunkMessages(messages, 100);
  assert.ok(chunks.length > 1, `expected multiple chunks, got ${chunks.length}`);
  assert.equal(chunks.flat().length, 10, 'no message lost or duplicated');
  for (const chunk of chunks) {
    assert.ok(chunk.length >= 1, 'every chunk is non-empty');
  }
});

test('estimateTokens: bytes/4, and it grows with message count', () => {
  const one = { messages: [{ role: 'user', content: 'x'.repeat(400) }] };
  const two = { messages: [{ role: 'user', content: 'x'.repeat(400) }, { role: 'assistant', content: 'y'.repeat(400) }] };
  assert.equal(estimateTokens(one), Math.ceil(new TextEncoder().encode(JSON.stringify(one.messages)).length / 4));
  assert.ok(estimateTokens(two) > estimateTokens(one));
});

test('optimizeContext: small input passes through unchanged', async () => {
  const body = { messages: [{ role: 'user', content: 'hi' }] };
  const out = await optimizeContext(body, {}, async () => {}, null, summaryAttempt([]));
  assert.equal(out, body, 'single-chunk input must be returned unchanged (fail-open)');
});

test('optimizeContext: oversized history is split, summarized and rebuilt', async () => {
  const userQuestion = 'the actual question';
  const history = Array.from({ length: 40 }, (_, i) => ({ role: 'assistant', content: `turn ${i}: ${'x'.repeat(120)}` }));
  const body = { model: 'service', messages: [{ role: 'system', content: 'be brief' }, ...history, { role: 'user', content: userQuestion }] };
  const records = [];
  // Small injectable chunk limit so the test splits without building a 320 KB fixture.
  const out = await optimizeContext(body, {}, async () => {}, 'opencode-go/beam-free', summaryAttempt(records), 100);

  assert.ok(out !== body, 'optimization should return a new body');
  assert.ok(records.length > 1, `expected multiple map calls, got ${records.length}`);
  // The last user question is preserved verbatim as the only `user` message in the reduced output.
  const reducedUsers = out.messages.filter(m => m.role === 'user');
  assert.equal(reducedUsers.length, 1);
  assert.equal(reducedUsers[0].content, userQuestion);
  // Original system message survives.
  assert.ok(out.messages.some(m => m.role === 'system' && m.content === 'be brief'));
  // Summaries made it into the reduced body.
  const summaryMsg = out.messages.find(m => m.role === 'system' && m.content.includes('Relevant context summaries'));
  assert.ok(summaryMsg, 'reduce step should include a summaries system message');
  // The reduced body is smaller than the original.
  assert.ok(estimateTokens(out) < estimateTokens(body));
});

test('optimizeContext: every chunk failing → fail-open to original body', async () => {
  const history = Array.from({ length: 40 }, (_, i) => ({ role: 'assistant', content: `turn ${i}: ${'x'.repeat(120)}` }));
  const body = { messages: [...history, { role: 'user', content: 'q' }] };
  // attempt always fails.
  const failAttempt = async () => ({ ok: false, error: 'timeout' });
  const out = await optimizeContext(body, {}, async () => {}, 'opencode-go/beam-free', failAttempt, 100);
  assert.equal(out, body, 'all-failed map must return the original body');
});
