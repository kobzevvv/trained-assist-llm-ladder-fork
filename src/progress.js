// Short-lived, per-call progress events. Access is scoped by a caller-generated secret distinct
// from the call id; the shared Ladder bearer token alone cannot read another caller's events.
const CALL_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const MAX_EVENTS = 128;
const TTL_MS = 60 * 60 * 1000;
const EVENT_TYPES = new Set(['started', 'rung_selected', 'retrying', 'failover', 'context_optimized', 'completed', 'failed']);

async function sha256(value) {
  const bytes = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value));
  return [...new Uint8Array(bytes)].map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

function sameHash(a, b) {
  if (typeof a !== 'string' || typeof b !== 'string' || a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

export function validCallId(value) {
  return typeof value === 'string' && CALL_ID.test(value);
}

function safeEvent(event) {
  if (!event || !EVENT_TYPES.has(event.type)) return null;
  const safe = { type: event.type };
  if (typeof event.ladder === 'string') safe.ladder = event.ladder.slice(0, 180);
  if (typeof event.rung === 'string') safe.rung = event.rung.slice(0, 180);
  if (typeof event.from === 'string') safe.from = event.from.slice(0, 180);
  if (Number.isSafeInteger(event.attempt) && event.attempt > 0) safe.attempt = event.attempt;
  if (typeof event.reason === 'string' && /^[a-z_]{1,32}$/.test(event.reason)) safe.reason = event.reason;
  if (typeof event.model === 'string') safe.model = event.model.slice(0, 180);
  if (Number.isSafeInteger(event.retry_after_ms) && event.retry_after_ms >= 0) safe.retry_after_ms = event.retry_after_ms;
  // #331 — context optimization telemetry: how much the pre-ladder split shrank the input.
  if (Number.isSafeInteger(event.tokens_before) && event.tokens_before > 0) safe.tokens_before = event.tokens_before;
  if (Number.isSafeInteger(event.tokens_after) && event.tokens_after >= 0) safe.tokens_after = event.tokens_after;
  return safe;
}

// Writes are serialized in the background. Provider selection and its deadline never wait for
// telemetry; the returned flush promise is attached to the Worker execution context by the caller.
export async function createProgressRecorder(db, callId, accessToken, now = Date.now()) {
  if (!db || !validCallId(callId) || typeof accessToken !== 'string' || accessToken.length < 32 || accessToken.length > 256) {
    return null;
  }
  const ownerHash = await sha256(accessToken);
  // Cleanup precedes INSERT so an expired token cannot block a new call through the unique owner
  // index. Progress failures are intentionally swallowed and never prevent the model call.
  let initialized = false;
  let queue = db.prepare(
    'DELETE FROM ladder_call_progress_events WHERE call_id IN (SELECT call_id FROM ladder_call_progress WHERE expires_at <= ?1)',
  ).bind(now).run().then(() => db.prepare('DELETE FROM ladder_call_progress WHERE expires_at <= ?1').bind(now).run())
    .then(() => db.prepare(
      'INSERT INTO ladder_call_progress (call_id, owner_hash, created_at, expires_at) VALUES (?1, ?2, ?3, ?4)',
    ).bind(callId, ownerHash, now, now + TTL_MS).run()).then((result) => {
      initialized = result?.success !== false;
    }).catch(() => { initialized = false; });

  return {
    publish(event) {
      const value = safeEvent(event);
      if (!value) return;
      const at = Date.now();
      const isTerminal = value.type === 'completed' || value.type === 'failed';
      const eventCap = isTerminal ? MAX_EVENTS : MAX_EVENTS - 1; // reserve the final slot for terminal state
      queue = queue.then(() => {
        if (!initialized) return undefined;
        return db.prepare(
        `INSERT INTO ladder_call_progress_events (call_id, seq, at_ms, event_type, payload)
         SELECT ?1, COALESCE(MAX(seq), 0) + 1, ?2, ?3, ?4
         FROM ladder_call_progress_events WHERE call_id = ?1 HAVING COUNT(*) < ?5`,
        ).bind(callId, at, value.type, JSON.stringify({ ...value, timestamp_ms: at }), eventCap).run();
      })
        .then(() => {
          if (isTerminal) {
            return db.prepare('UPDATE ladder_call_progress SET terminal = 1 WHERE call_id = ?1').bind(callId).run();
          }
          return undefined;
        })
        .catch(() => {});
    },
    flush() { return queue; },
  };
}

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Long-poll D1 from a pull-driven stream. Disconnected clients stop polling in cancel(); the
// ladder request itself has a separate execution and never waits for this listener.
export async function progressEventsResponse(request, env, callId) {
  const db = env.LADDER_TRACE_DB;
  const accessToken = request.headers.get('x-ladder-progress-token') || '';
  const lastId = request.headers.get('last-event-id') || '0';
  if (!db || !validCallId(callId) || accessToken.length < 32 || accessToken.length > 256 || !/^\d{1,12}$/.test(lastId)) {
    return new Response(null, { status: 404 });
  }
  const ownerHash = await sha256(accessToken);
  const lastSeq = Number(lastId);
  if (!Number.isSafeInteger(lastSeq)) return new Response(null, { status: 400 });
  let owner = null;
  try {
    // A caller can open SSE immediately after starting the POST; allow the background INSERT a
    // short window to reach D1 before treating a not-yet-visible call as unknown.
    for (let attempt = 0; attempt < 20 && !owner; attempt++) {
      owner = await db.prepare(
        'SELECT call_id, owner_hash, expires_at FROM ladder_call_progress WHERE call_id = ?1 AND expires_at > ?2',
      ).bind(callId, Date.now()).first();
      if (!owner && attempt < 19) await wait(100);
    }
  } catch {
    return new Response(null, { status: 503 });
  }
  if (!owner || !sameHash(owner.owner_hash, ownerHash)) return new Response(null, { status: 404 });

  const encoder = new TextEncoder();
  let cursor = lastSeq;
  let canceled = false;
  const stream = new ReadableStream({
    async pull(controller) {
      const deadline = Date.now() + 30_000;
      while (!canceled && Date.now() < deadline) {
        if (Date.now() >= owner.expires_at) {
          controller.close();
          return;
        }
        let rows;
        try {
          const result = await db.prepare(
            'SELECT seq, at_ms, event_type, payload FROM ladder_call_progress_events WHERE call_id = ?1 AND seq > ?2 ORDER BY seq LIMIT 50',
          ).bind(callId, cursor).all();
          rows = result?.results || [];
        } catch {
          controller.enqueue(encoder.encode('event: unavailable\ndata: {}\n\n'));
          controller.close();
          return;
        }
        if (rows.length) {
          for (const row of rows) {
            cursor = Number(row.seq);
            controller.enqueue(encoder.encode(`id: ${cursor}\nevent: ${row.event_type}\ndata: ${row.payload}\n\n`));
          }
          if (rows.some((row) => row.event_type === 'completed' || row.event_type === 'failed')) controller.close();
          return;
        }
        try {
          const state = await db.prepare('SELECT terminal FROM ladder_call_progress WHERE call_id = ?1').bind(callId).first();
          if (!state || state.terminal) {
            controller.close();
            return;
          }
        } catch {
          controller.close();
          return;
        }
        await wait(500);
      }
      if (!canceled) controller.enqueue(encoder.encode(': keep-alive\n\n'));
    },
    cancel() { canceled = true; },
  });
  return new Response(stream, { status: 200, headers: {
    'content-type': 'text/event-stream; charset=utf-8',
    'cache-control': 'no-cache, no-transform',
    connection: 'keep-alive',
    'x-accel-buffering': 'no',
  } });
}
