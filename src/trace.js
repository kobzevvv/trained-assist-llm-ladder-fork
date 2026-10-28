// Trace plumbing: optional caller ids plus native OpenCode session/run ids → one D1 row per call.
// Kept in its own module so tests can import it without dragging in config/ladders.json.
// options.events: optional error-event publisher (createErrorPublisher from src/events.js).
//   Called fire-and-forget for failed calls, never throws.

// All correlation IDs are optional and length-capped. OpenCode owns its own opaque
// session/run IDs; explicit x-ladder-* values remain a compatibility override for other
// integrations. Missing/blank metadata never prevents a request or a trace row.
export function makeTrace(request) {
  const h = request.headers;
  const get = (key) => {
    const v = h.get(key);
    if (!v) return null;
    const s = String(v).trim();
    return s ? s.slice(0, 200) : null;
  };
  const openCodeSessionId = get('x-opencode-session-id');
  return {
    traceId: get('x-ladder-trace') ?? openCodeSessionId,
    runId: get('x-ladder-run') ?? get('x-opencode-run-id'),
    userId: get('x-ladder-user'),
    chatId: get('x-ladder-chat'),
    sessionId: get('x-ladder-session') ?? openCodeSessionId,
    // Which concrete sub-task made this call (the caller's `source:` / tool name). Raw here —
    // the handler overwrites it with the sanitised slug it already sends to OpenRouter, so the
    // row and the OpenRouter "Application" cut can never disagree (#107).
    app: get('x-ladder-app'),
  };
}

const TRACE_SQL = `INSERT INTO ladder_calls
  (ts, trace_id, run_id, user_id, chat_id, session_id, ladder, ok, model, ms, attempts, tokens_in, tokens_out, tokens_cached, tokens_cache_write, estimated_tokens_in, app, key_hash)
  VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)`;

const SETTLE_STREAM_SQL = `UPDATE ladder_calls SET ok = ?1, ms = ?2, attempts = ?3,
  tokens_in = ?4, tokens_out = ?5, tokens_cached = ?6, tokens_cache_write = ?7, estimated_tokens_in = ?8 WHERE rowid = ?9`;

// estimated_tokens_in is the ladder's own size estimate (size-policy.estimateTokens), written
// for EVERY call — successful or not. The provider's tokens_in only exists on a successful
// response, so without this column the trace could not answer "how big were the requests that
// failed with HTTP 400?" — the question behind the context pre-filter. It is an estimate, so
// it is a separate column and never overwrites the provider's own count.
function usageValues(r) {
  const usage = r.data?.usage;
  if (!usage || typeof usage !== 'object') return [null, null, null, null, null];
  const details = usage.prompt_tokens_details || usage.input_tokens_details || {};
  const first = (...values) => values.find((value) => Number.isFinite(value) && value >= 0) ?? null;
  return [
    first(usage.prompt_tokens, usage.input_tokens),
    first(usage.completion_tokens, usage.output_tokens),
    first(details.cached_tokens, usage.cache_read_input_tokens, usage.prompt_cache_hit_tokens),
    first(details.cache_write_tokens, usage.cache_creation_input_tokens, usage.prompt_cache_miss_tokens),
    Number.isFinite(r.estimated_tokens_in) ? r.estimated_tokens_in : null,
  ];
}

// Commit a visible pending row before returning stream headers. Cloudflare can end an
// invocation on client disconnect without calling the response body's cancel hook; a
// settlement-only INSERT then loses the call entirely. The rowid belongs to this INSERT,
// so concurrent calls sharing a caller-supplied trace cannot update one another.
export async function beginStreamCall(env, trace, ladder, r, started) {
  const db = env.LADDER_TRACE_DB;
  if (!db) return null;
  const attempts = (r.attempts || []).map((attempt, index) =>
    index === r.attempts.length - 1 && attempt.outcome === 'ok'
      ? { ...attempt, outcome: 'stream_pending' }
      : attempt);
  try {
    const result = await db.prepare(TRACE_SQL).bind(
      Date.now(), trace.traceId ?? null, trace.runId ?? null, trace.userId ?? null, trace.chatId ?? null,
      trace.sessionId ?? null, ladder, 0, r.model || null, Date.now() - started,
      JSON.stringify(attempts), null, null, null, null, null, trace.app ?? null, trace.keyHash ?? null,
    ).run();
    const rowId = result?.meta?.last_row_id;
    return result?.success && Number.isSafeInteger(rowId) && rowId > 0 ? rowId : null;
  } catch (e) {
    console.error('trace stream pending insert failed:', e.message);
    return null;
  }
}

export async function settleStreamCall(env, rowId, trace, ladder, r, started,
  { events, retryable = false } = {}) {
  if (rowId == null) {
    await logCall(env, trace, ladder, r, started, { events, retryable });
    return;
  }
  try {
    await env.LADDER_TRACE_DB.prepare(SETTLE_STREAM_SQL).bind(
      r.ok ? 1 : 0, Date.now() - started, JSON.stringify(r.attempts || []),
      ...usageValues(r), rowId,
    ).run();
  } catch (e) {
    console.error('trace stream settlement update failed:', e.message);
  }
  if (events && !r.ok) {
    const { buildErrorEvent } = await import('./events.js');
    events.publishError(buildErrorEvent({ trace, ladder,
      error: r.error || 'stream incomplete', outcome: 'failed', retryable }));
  }
}

// Best-effort D1 append: never fails the call, never throws. Non-stream usage and
// observed terminal-frame stream usage both count toward tokens_in/out.
// tokens_cached / tokens_cache_write normalize cache-read/cache-creation dialects when present;
// absent counters stay NULL (unknown), never zero. tokens_cached is a subset of tokens_in.
// app (#107) = the caller's sub-task tag, so the cost cut can be per sub-task and not only
// per ladder — `service` covers ~20 different tools, which is too coarse to prioritise.
export async function logCall(env, trace, ladder, r, started, { events, retryable = true } = {}) {
  const db = env.LADDER_TRACE_DB;
  if (!db && !events) return;
  try {
    if (db) {
      await db.prepare(TRACE_SQL).bind(
        Date.now(),
        trace.traceId ?? null, trace.runId ?? null, trace.userId ?? null, trace.chatId ?? null, trace.sessionId ?? null,
        ladder, r.ok ? 1 : 0, r.model || null, Date.now() - started,
        JSON.stringify(r.attempts || []),
        ...usageValues(r),
        trace.app ?? null, trace.keyHash ?? null,
      ).run();
    }
  } catch (e) {
    console.error('trace d1 insert failed:', e.message);
  }
  if (events && !r.ok) {
    const { buildErrorEvent } = await import('./events.js');
    const event = buildErrorEvent({
      trace,
      ladder,
      error: r.error || 'every rung failed',
      outcome: 'failed',
      retryable,
    });
    events.publishError(event);
  }
}
