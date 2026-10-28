import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createProgressRecorder, progressEventsResponse, validCallId } from '../src/progress.js';
import { handle } from '../src/handler.js';

const ID = '4e112b12-1492-4eaa-8bb0-6ca537a4440c';
const OWNER = 'owner-progress-token-that-is-distinct-from-the-shared-bearer';

function progressDb() {
  const calls = new Map();
  const events = new Map();
  const prepare = (sql) => ({ bind: (...params) => ({
    async run() {
      if (sql.startsWith('INSERT INTO ladder_call_progress (')) {
        const [callId, ownerHash, createdAt, expiresAt] = params;
        if (calls.has(callId) || [...calls.values()].some((row) => row.owner_hash === ownerHash)) throw new Error('duplicate call id or per-call secret');
        calls.set(callId, { call_id: callId, owner_hash: ownerHash, created_at: createdAt, expires_at: expiresAt, terminal: 0 });
      } else if (sql.startsWith('INSERT INTO ladder_call_progress_events')) {
        const [callId, at, type, payload, cap] = params;
        const list = events.get(callId) || [];
        if (list.length < cap) list.push({ call_id: callId, seq: (list.at(-1)?.seq || 0) + 1, at_ms: at, event_type: type, payload });
        events.set(callId, list);
      } else if (sql.startsWith('UPDATE ladder_call_progress SET terminal')) {
        const row = calls.get(params[0]);
        if (row) row.terminal = 1;
      }
      return { success: true };
    },
    async first() {
      if (sql.startsWith('SELECT call_id, owner_hash, expires_at')) {
        const [callId, now] = params;
        const row = calls.get(callId);
        return row && row.expires_at > now ? { ...row } : null;
      }
      if (sql.startsWith('SELECT terminal')) return calls.get(params[0]) || null;
      return null;
    },
    async all() {
      if (sql.startsWith('SELECT seq, at_ms')) {
        const [callId, after] = params;
        return { results: (events.get(callId) || []).filter((event) => event.seq > after).slice(0, 50) };
      }
      return { results: [] };
    },
  }) });
  return { prepare, _calls: calls, _events: events };
}

test('call progress requires a UUID and an independent per-call secret', async () => {
  assert.equal(validCallId(ID), true);
  assert.equal(validCallId('service-session'), false);
  assert.equal(await createProgressRecorder(progressDb(), ID, 'short'), null);
  const db = progressDb();
  const recorder = await createProgressRecorder(db, ID, OWNER, 1000);
  await recorder.flush();
  const row = db._calls.get(ID);
  assert.ok(row);
  assert.notEqual(row.owner_hash, OWNER);
  assert.equal(row.expires_at, 1000 + 60 * 60 * 1000);
  const reused = await createProgressRecorder(db, '08356fa0-5004-4a8a-9560-0cb125e2472c', OWNER, 2000);
  reused.publish({ type: 'started' });
  await reused.flush();
  assert.equal(db._calls.size, 1, 'a progress secret is unique to one active call');
});

test('SSE replays after Last-Event-ID, keeps sequential ids, and redacts unapproved fields', async () => {
  const db = progressDb();
  const recorder = await createProgressRecorder(db, ID, OWNER);
  recorder.publish({ type: 'started', prompt: 'private prompt' });
  recorder.publish({ type: 'rung_selected', rung: 'free/first', attempt: 1 });
  recorder.publish({ type: 'retrying', rung: 'free/first', attempt: 2, reason: 'key_probe', raw_error: 'secret' });
  recorder.publish({ type: 'failover', from: 'free/first', rung: 'paid/second', attempt: 3 });
  recorder.publish({ type: 'completed', model: 'paid/second', attempt: 3 });
  await recorder.flush();

  const request = new Request(`https://ladder.test/v1/calls/${ID}/events`, {
    headers: { authorization: 'Bearer shared-token', 'x-ladder-progress-token': OWNER, 'last-event-id': '2' },
  });
  const response = await progressEventsResponse(request, { LADDER_TRACE_DB: db }, ID);
  assert.equal(response.status, 200);
  assert.match(response.headers.get('content-type'), /text\/event-stream/);
  const body = await response.text();
  assert.match(body, /id: 3\nevent: retrying/);
  assert.match(body, /id: 4\nevent: failover/);
  assert.match(body, /id: 5\nevent: completed/);
  assert.match(body, /"timestamp_ms":\d+/);
  assert.doesNotMatch(body, /private prompt|secret|raw_error/);
});

test('context_optimized event survives with tokens_before/after, redacts prompt', async () => {
  const db = progressDb();
  const recorder = await createProgressRecorder(db, ID, OWNER);
  recorder.publish({ type: 'context_optimized', rung: 'free/first', tokens_before: 101099, tokens_after: 86, prompt: 'private' });
  recorder.publish({ type: 'completed', model: 'free/first', attempt: 1 });
  await recorder.flush();

  const request = new Request(`https://ladder.test/v1/calls/${ID}/events`, {
    headers: { authorization: 'Bearer shared-token', 'x-ladder-progress-token': OWNER },
  });
  const response = await progressEventsResponse(request, { LADDER_TRACE_DB: db }, ID);
  const body = await response.text();
  assert.match(body, /event: context_optimized/);
  assert.match(body, /"tokens_before":101099/);
  assert.match(body, /"tokens_after":86/);
  assert.doesNotMatch(body, /"prompt"/);
});

test('shared bearer or guessed call id cannot read another caller progress', async () => {
  const db = progressDb();
  const recorder = await createProgressRecorder(db, ID, OWNER);
  recorder.publish({ type: 'started' });
  await recorder.flush();
  const response = await progressEventsResponse(new Request(`https://ladder.test/v1/calls/${ID}/events`, {
    headers: { authorization: 'Bearer shared-token', 'x-ladder-progress-token': 'another-clients-token-with-32-chars' },
  }), { LADDER_TRACE_DB: db }, ID);
  assert.equal(response.status, 404);
});

test('chat completion exposes live progress while preserving the ordinary OpenAI response', async () => {
  const db = progressDb();
  const providerCalls = [];
  const store = {
    snapshot: async () => ({ health: {}, keys: { active: 0, exhausted: {} } }),
    recordSuccess: async () => {}, recordFailure: async () => {}, rotateKey: async () => ({ rotated: false }),
    park: async () => {},
  };
  const response = await handle(new Request('https://ladder.test/v1/chat/completions', {
    method: 'POST',
    headers: {
      authorization: 'Bearer shared-token',
      'content-type': 'application/json',
      'x-ladder-call-id': ID,
      'x-ladder-progress-token': OWNER,
    },
    body: JSON.stringify({ model: 'standard', messages: [{ role: 'user', content: 'synthetic test' }] }),
  }), { LADDER_TOKEN: 'shared-token', OPENCODE_GO_API_KEYS: 'synthetic-key', LADDER_TRACE_DB: db }, {
    store,
    fetchImpl: async (_url, init) => {
      providerCalls.push(JSON.parse(init.body));
      return { ok: true, status: 200, json: async () => ({
        model: 'synthetic/model', choices: [{ message: { role: 'assistant', content: 'synthetic answer' }, finish_reason: 'stop' }],
        usage: { prompt_tokens: 3, completion_tokens: 2 },
      }) };
    },
  });
  assert.equal(response.status, 200);
  const answer = await response.json();
  assert.equal(answer.choices[0].message.content, 'synthetic answer');
  assert.equal(providerCalls.length, 1);

  const progress = await progressEventsResponse(new Request(`https://ladder.test/v1/calls/${ID}/events`, {
    headers: { 'x-ladder-progress-token': OWNER },
  }), { LADDER_TRACE_DB: db }, ID);
  const events = await progress.text();
  assert.match(events, /event: started/);
  assert.match(events, /event: rung_selected/);
  assert.match(events, /event: completed/);
  assert.doesNotMatch(events, /synthetic test|synthetic answer|synthetic-key/);

  const progressPath = `https://ladder.test/v1/calls/${ID}/events`;
  const unauthenticated = await handle(new Request(progressPath, {
    headers: { 'x-ladder-progress-token': OWNER },
  }), { LADDER_TOKEN: 'shared-token', LADDER_TRACE_DB: db }, {});
  assert.equal(unauthenticated.status, 401, 'per-call token does not replace Ladder authentication');
  const authenticated = await handle(new Request(progressPath, {
    headers: { authorization: 'Bearer shared-token', 'x-ladder-progress-token': OWNER, 'last-event-id': '1' },
  }), { LADDER_TOKEN: 'shared-token', LADDER_TRACE_DB: db }, {});
  assert.equal(authenticated.status, 200);
  assert.match(await authenticated.text(), /event: completed/);
  const otherClient = await handle(new Request(progressPath, {
    headers: { authorization: 'Bearer shared-token', 'x-ladder-progress-token': 'another-clients-token-with-32-chars' },
  }), { LADDER_TOKEN: 'shared-token', LADDER_TRACE_DB: db }, {});
  assert.equal(otherClient.status, 404, 'the shared bearer token does not grant cross-call access');

  const sameBearer = await handle(new Request('https://ladder.test/v1/chat/completions', {
    method: 'POST',
    headers: { authorization: 'Bearer shared-token', 'content-type': 'application/json',
      'x-ladder-call-id': '08356fa0-5004-4a8a-9560-0cb125e2472c', 'x-ladder-progress-token': 'shared-token' },
    body: JSON.stringify({ model: 'standard', messages: [{ role: 'user', content: 'test' }] }),
  }), { LADDER_TOKEN: 'shared-token', LADDER_TRACE_DB: db }, {});
  assert.equal(sameBearer.status, 400, 'progress secret must not be the shared API credential');
});

test('terminal failures and streamed calls publish truthful terminal progress', async () => {
  const failureId = '67fe8157-9e13-4aef-911d-3a6de9c70360';
  const failureDb = progressDb();
  const failure = await handle(new Request('https://ladder.test/v1/chat/completions', {
    method: 'POST',
    headers: { authorization: 'Bearer shared-token', 'content-type': 'application/json',
      'x-ladder-call-id': failureId, 'x-ladder-progress-token': OWNER },
    body: JSON.stringify({ model: 'standard', messages: [{ role: 'user', content: 'synthetic failure' }] }),
  }), { LADDER_TOKEN: 'shared-token', LADDER_TRACE_DB: failureDb }, { store: {
    snapshot: async () => ({ health: {}, keys: { active: 0, exhausted: {} } }),
    recordSuccess: async () => {}, recordFailure: async () => {}, rotateKey: async () => ({ rotated: false }), park: async () => {},
  } });
  assert.equal(failure.status, 503);
  const failedEvents = await progressEventsResponse(new Request(`https://ladder.test/v1/calls/${failureId}/events`, {
    headers: { 'x-ladder-progress-token': OWNER },
  }), { LADDER_TRACE_DB: failureDb }, failureId);
  assert.match(await failedEvents.text(), /event: failed/);

  const streamId = '1717fd3b-d46e-45ab-a0cb-0752b12d441d';
  const streamDb = progressDb();
  const streamStore = {
    snapshot: async () => ({ health: {}, keys: { active: 0, exhausted: {} } }),
    recordSuccess: async () => {}, recordFailure: async () => {}, rotateKey: async () => ({ rotated: false }), park: async () => {},
  };
  const streamResponse = await handle(new Request('https://ladder.test/v1/chat/completions', {
    method: 'POST',
    headers: { authorization: 'Bearer shared-token', 'content-type': 'application/json',
      'x-ladder-call-id': streamId, 'x-ladder-progress-token': OWNER },
    body: JSON.stringify({ model: 'standard', stream: true, messages: [{ role: 'user', content: 'synthetic stream' }] }),
  }), { LADDER_TOKEN: 'shared-token', OPENCODE_GO_API_KEYS: 'synthetic-key', LADDER_TRACE_DB: streamDb }, {
    store: streamStore,
    fetchImpl: async () => ({ ok: true, status: 200, body: new ReadableStream({ start(controller) {
      controller.enqueue(new TextEncoder().encode('data: {"choices":[{"delta":{"content":"ok"}}]}\n\n'));
      controller.enqueue(new TextEncoder().encode('data: [DONE]\n\n'));
      controller.close();
    } }) }),
  });
  assert.equal(streamResponse.status, 200);
  assert.match(await streamResponse.text(), /\[DONE\]/);
  const streamEvents = await progressEventsResponse(new Request(`https://ladder.test/v1/calls/${streamId}/events`, {
    headers: { 'x-ladder-progress-token': OWNER },
  }), { LADDER_TRACE_DB: streamDb }, streamId);
  assert.match(await streamEvents.text(), /event: completed/);
});
