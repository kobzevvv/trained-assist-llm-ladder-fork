// Free-model inventory (issue #111): collect the free catalogs of every provider the ladder
// already talks to, probe availability lightly, and keep the history in one D1 table.
//
// The module is deliberately free of the Workerd runtime (same rule as src/handler.js) so
// `node --test` can drive it with a fake fetch and a fake D1, and the worker can import it
// for the /v1/free-models routes.
//
// Providers are keyed by the ladder rung prefix they own — config/ladders.json only ever uses
// these three, and test/free-models.test.js asserts that (a 4th prefix must get a collector
// before it can be silently missed, which is the "did we skip a service?" check from the issue).
//
// model_id is the FULL ladder rung id (e.g. `openrouter/xiaomi/mimo-v2.6-flash:free`), not the
// bare upstream id: that is the identity the rest of the repo already uses (config/prices.json,
// config/contexts.json, categoryOf()), so the inventory joins to them without a mapping table.
//
// `available` means "present in the latest catalog" — the issue's reconciliation rule. The probe
// is a separate, lighter signal (probe_status) and never decides availability: a 429 from a
// free tier is a limit, not a disappearance.

import { readPool, APP_REFERER_BASE, DEFAULT_APP_SLUG, DEFAULT_APP_TITLE } from './ladder.js';
import { isEmptyAnswer } from './answer-guard.js';
import { resolveProviderPool } from './provider-credentials.js';

export const GO_BENCH_TIERS = [
  { name: 'small', bytes: 1_024 },
  { name: 'medium', bytes: 32_768 },
  { name: 'large', bytes: 131_072 },
];

// The token budget of one bench completion. A model that needs most of it to answer is a
// reasoning model — it is measured, but it is never pushed into the ladder (its reasoning
// floor is unknown there, issue #38).
export const BENCH_MAX_TOKENS = 1500;

// The bench asks the model to answer "ready" — a model that follows the instruction is counted
// in `correct`. It is the quality signal of the ladder's fast lane (the tie-break when two
// models answer within the same margin).
export const BENCH_CORRECT_RE = /\bready\b/i;

export function goBenchEligible(benchmark, tier) {
  const b = typeof benchmark === 'string' ? JSON.parse(benchmark) : benchmark;
  const s = b?.tiers?.[tier];
  return !!s && s.attempts >= 2 && s.successes >= 2 && Number.isFinite(s.p50_ms);
}

export function goBenchOrder(rows, tier) {
  return [...rows]
    .filter((r) => r.available && r.bench_status === 'ready' && goBenchEligible(r.benchmark, tier))
    .sort((a, b) => a.benchmark.tiers[tier].p50_ms - b.benchmark.tiers[tier].p50_ms || a.model_id.localeCompare(b.model_id))
    .map((r) => r.model_id);
}

function goBenchMessages(bytes) {
  const unit = 'Review this project request and identify the key requirement. ';
  return [
    { role: 'system', content: 'Answer the final user request in one short sentence.' },
    { role: 'user', content: unit.repeat(Math.max(1, Math.floor(bytes / unit.length))) },
    { role: 'user', content: 'Reply with exactly: ready' },
  ];
}

export async function benchmarkGoModel(row, env, { fetchImpl = fetch, timeoutMs = 45_000, now = Date.now() } = {}) {
  if (row.provider !== 'opencode-go' || !String(row.model_id).endsWith('-free')) return null;
  const req = probeRequest(row.provider, row.model_id, env);
  if (req.skip) return { model_id: row.model_id, status: 'skipped', reason: req.skip, benchmark: null };
  const tiers = {};
  for (const tier of GO_BENCH_TIERS) {
    const times = [];
    let successes = 0;
    let correct = 0;
    let outTokens = null;
    let truncated = false;
    let lastError = null;
    for (let i = 0; i < 2; i++) {
      const started = Date.now();
      try {
        const body = { ...req.body, max_tokens: BENCH_MAX_TOKENS, messages: goBenchMessages(tier.bytes) };
        const res = await fetchImpl(req.url, { method: 'POST', headers: req.headers, body: JSON.stringify(body), signal: AbortSignal.timeout(timeoutMs) });
        const text = await res.text().catch(() => '');
        if (res.body) await res.body.cancel().catch(() => {});
        let payload = null;
        try { payload = JSON.parse(text); } catch { /* counted as a failed response */ }
        const content = String(payload?.choices?.[0]?.message?.content ?? '').trim();
        const finish = payload?.choices?.[0]?.finish_reason ?? null;
        const usageOut = payload?.usage?.completion_tokens;
        if (Number.isFinite(usageOut)) outTokens = usageOut;
        if (res.ok && !isEmptyAnswer(content)) {
          successes++;
          times.push(Date.now() - started);
          if (BENCH_CORRECT_RE.test(content)) correct++;
        } else {
          lastError = `http_${res.status}${isEmptyAnswer(content) ? ': empty answer' : ''}`;
          if (finish === 'length') truncated = true;
        }
      } catch (e) { lastError = String((e && e.message) || e).slice(0, 120); }
    }
    times.sort((a, b) => a - b);
    const p50 = !times.length ? null : times.length === 1 ? times[0] : (times[0] + times[1]) / 2;
    tiers[tier.name] = { attempts: 2, successes, correct, p50_ms: p50, out_tokens: outTokens, truncated, last_error: lastError };
  }
  const eligible = GO_BENCH_TIERS.some((t) => goBenchEligible({ tiers }, t.name));
  return { model_id: row.model_id, status: eligible ? 'ready' : 'failed', benchmark: { version: 1, measured_at: now, tiers } };
}

// ── OpenRouter benchmarks ─────────────────────────────────────────────────────────
// The same measurement as for Go, against the OpenRouter free models. The caller hands over the
// credential POOL: the free tier rate-limits per key, so one key would report "the model is
// broken" when in fact the key is throttled (429) or out of credits (402). A key-level fault
// therefore rotates to the next key of the pool; only the answer itself is the model's verdict.
// The recorded `correct` count and `out_tokens` are the signals the ladder's fast lane uses
// (speed first, quality as the tie-break).

// A response that says nothing about the model — the key is throttled, unauthorized or out of
// credits. The next key of the pool is tried instead of counting a failed attempt.
const KEY_LEVEL_FAULTS = new Set([401, 402, 429]);

export async function benchmarkOpenrouterModel(row, env, { fetchImpl = fetch, timeoutMs = 45_000, now = Date.now(), keys = [], key = null } = {}) {
  if (row.provider !== 'openrouter') return null;
  const modelId = String(row.model_id || '');
  if (!modelId.endsWith(':free')) return null;
  const pool = (key ? [key] : (Array.isArray(keys) ? keys : [])).filter(Boolean);
  if (!pool.length) return { model_id: modelId, status: 'skipped', reason: 'no key', benchmark: null };
  const bare = modelId.slice(modelId.indexOf('/') + 1);
  const base = env.OPENROUTER_BASE_URL || 'https://openrouter.ai/api/v1';
  const attribution = {
    // The same attribution the ladder sends, so the bench shows up in the OpenRouter dashboard
    // as this application instead of as an anonymous script.
    'HTTP-Referer': `${APP_REFERER_BASE}/${DEFAULT_APP_SLUG}`,
    'X-OpenRouter-Title': DEFAULT_APP_TITLE,
    'X-OpenRouter-App-Visibility': 'hidden',
  };
  const tiers = {};
  let keyIndex = 0;
  for (const tier of GO_BENCH_TIERS) {
    const times = [];
    let successes = 0;
    let correct = 0;
    let outTokens = null;
    let truncated = false;
    let lastError = null;
    for (let i = 0; i < 2; i++) {
      // Every attempt may walk the pool: a key-level fault is not the model's fault.
      for (let attempt = 0; attempt < pool.length; attempt++) {
        const usedKey = pool[(keyIndex + attempt) % pool.length];
        const headers = { 'content-type': 'application/json', Authorization: `Bearer ${usedKey}`, ...attribution };
        const started = Date.now();
        try {
          const body = { model: bare, max_tokens: BENCH_MAX_TOKENS, messages: goBenchMessages(tier.bytes) };
          const res = await fetchImpl(`${base}/chat/completions`, { method: 'POST', headers, body: JSON.stringify(body), signal: AbortSignal.timeout(timeoutMs) });
          const text = await res.text().catch(() => '');
          if (res.body) await res.body.cancel().catch(() => {});
          let payload = null;
          try { payload = JSON.parse(text); } catch { /* counted as a failed response */ }
          if (KEY_LEVEL_FAULTS.has(res.status)) {
            lastError = `http_${res.status}`;
            continue; // the next key of the pool
          }
          keyIndex = (keyIndex + attempt) % pool.length; // the key that answered stays in use
          const content = String(payload?.choices?.[0]?.message?.content ?? '').trim();
          const finish = payload?.choices?.[0]?.finish_reason ?? null;
          const usageOut = payload?.usage?.completion_tokens;
          if (Number.isFinite(usageOut)) outTokens = usageOut;
          if (res.ok && !isEmptyAnswer(content)) {
            successes++;
            times.push(Date.now() - started);
            if (BENCH_CORRECT_RE.test(content)) correct++;
          } else {
            lastError = `http_${res.status}${isEmptyAnswer(content) ? ': empty answer' : ''}`;
            if (finish === 'length') truncated = true;
          }
          break;
        } catch (e) { lastError = String((e && e.message) || e).slice(0, 120); break; }
      }
    }
    times.sort((a, b) => a - b);
    const p50 = !times.length ? null : times.length === 1 ? times[0] : (times[0] + times[1]) / 2;
    tiers[tier.name] = { attempts: 2, successes, correct, p50_ms: p50, out_tokens: outTokens, truncated, last_error: lastError };
  }
  const eligible = GO_BENCH_TIERS.some((t) => goBenchEligible({ tiers }, t.name));
  return { model_id: modelId, status: eligible ? 'ready' : 'failed', benchmark: { version: 1, measured_at: now, tiers } };
}

// The OpenRouter credential pool (env secret + admin-managed D1 tokens). The free tier limits
// per key, so the bench rotates over the pool instead of hammering one key.
export async function openrouterBenchKeys(env, db) {
  if (db && env?.PROVIDER_CREDS_KEY) {
    try {
      const pool = await resolveProviderPool(env, db);
      if (pool.openrouter.length) return pool.openrouter;
    } catch { /* fall back to the env key */ }
  }
  return env?.OPENROUTER_API_KEY ? [env.OPENROUTER_API_KEY] : [];
}

export async function readGoBenchmarks(db) {
  return readBenchmarks(db, 'opencode-go');
}

// Bench rows of one provider. The join keeps the row only while the model is in the inventory —
// a benchmark without a catalog entry cannot be routed to.
export async function readBenchmarks(db, provider = 'opencode-go') {
  const { results = [] } = await db.prepare(
    'SELECT b.model_id, b.bench_status, b.benchmark, b.measured_at, f.available FROM free_model_benchmarks b '
      + 'JOIN free_models f ON f.provider = b.provider AND f.model_id = b.model_id '
      + 'WHERE b.provider = ?1'
  ).bind(provider).all();
  return results.map((r) => {
    let benchmark = r.benchmark;
    if (typeof benchmark === 'string') {
      try { benchmark = JSON.parse(benchmark); } catch { benchmark = null; }
    }
    return { ...r, benchmark, available: !!r.available };
  });
}

export const UPSERT_BENCH_SQL = `INSERT INTO free_model_benchmarks
  (provider, model_id, bench_status, benchmark, measured_at)
  VALUES (?1, ?2, ?3, ?4, ?5)
  ON CONFLICT(provider, model_id) DO UPDATE SET
    bench_status = excluded.bench_status, benchmark = excluded.benchmark, measured_at = excluded.measured_at`;

export async function writeBenchmark(db, provider, result) {
  await db.prepare(UPSERT_BENCH_SQL).bind(provider, result.model_id, result.status,
    result.benchmark ? JSON.stringify(result.benchmark) : null, result.benchmark?.measured_at ?? Date.now()).run();
}

export async function writeGoBenchmark(db, result) {
  await writeBenchmark(db, 'opencode-go', result);
}

export const OPENROUTER_CATALOG_URL = 'https://openrouter.ai/api/v1/models';
// The zen catalog is fetched DIRECTLY from opencode.ai, not through the GCP relay: the relay
// (scripts/zen-relay.mjs) only proxies POST /chat/completions, so /v1/models would 404 there.
export const ZEN_CATALOG_URL = 'https://opencode.ai/zen/v1/models';
export const GO_CATALOG_URL = 'https://opencode.ai/zen/go/v1/models';

// Providers are keyed by the ladder rung prefix they own — config/ladders.json only ever uses
// these three, and test/free-models.test.js asserts that (a new prefix must get a collector).
export const PROVIDERS = ['openrouter', 'opencode-zen', 'opencode-go'];

// The zen free tier is anonymous but validates the opencode-client fingerprint (issue #106 —
// captured live via mitmproxy, see scripts/zen-limit-probe.mjs for the full matrix). A probe
// without it gets a 403 and every zen model would look unavailable. The request/session header
// SHAPES are part of the validation: msg_<12 hex><12 alnum> and ses_<6 hex><14 alnum — the same
// ones scripts/zen-relay.mjs sends.
const ZEN_UA = 'opencode/1.18.31 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14';
const ZEN_SHELL_TOOL = { type: 'function', function: { name: 'shell', parameters: { type: 'object', properties: { cmd: { type: 'string' } }, required: ['cmd'] } } };
const ZEN_READ_TOOL = { type: 'function', function: { name: 'read', parameters: { type: 'object', properties: { path: { type: 'string' } }, required: ['path'] } } };
const ZEN_ALNUM = 'abcdefghijklmnopqrstuvwxyz0123456789';
// getRandomValues, not node:crypto — this module runs in the worker too, where the global crypto
// is the Web Crypto API.
const randBytes = (n) => {
  const b = new Uint8Array(n);
  crypto.getRandomValues(b);
  return b;
};
const randHex = (n) => [...randBytes(n)].map((b) => b.toString(16).padStart(2, '0')).join('');
const randAlnum = (n) => {
  const b = randBytes(n);
  let s = '';
  for (let i = 0; i < n; i++) s += ZEN_ALNUM[b[i] % ZEN_ALNUM.length];
  return s;
};

// Prices are stored PER 1M TOKENS, matching config/prices.json (costUsd divides by 1e6).
const PER_MILLION = 1e6;
const num = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};
const perMillion = (v) => {
  const n = num(v);
  return n === null ? null : n * PER_MILLION;
};

// ── Catalog collection ──────────────────────────────────────────────────────────────

async function getJson(url, { fetchImpl, headers = {}, timeoutMs }) {
  const res = await fetchImpl(url, { headers, signal: AbortSignal.timeout(timeoutMs) });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`HTTP ${res.status}: ${text.slice(0, 160)}`);
  }
  return res.json();
}

function entriesOf(payload) {
  if (Array.isArray(payload)) return payload;
  return Array.isArray(payload?.data) ? payload.data : [];
}

// OpenRouter: free = `:free` suffix, or both prices exactly 0 (the issue's two signals are
// equivalent there; a model with a paid output is NOT free even if the prompt is free).
function openRouterFree(e) {
  const inP = num(e?.pricing?.prompt);
  const outP = num(e?.pricing?.completion);
  return String(e?.id || '').endsWith(':free') || (inP === 0 && outP === 0);
}

function zenFree(e) {
  const id = String(e?.id || '');
  if (id.endsWith('-free')) return true;
  // `big-pickle` is a free-tier zen model without the suffix (it is a rung in the `free` ladder).
  if (id === 'big-pickle') return true;
  // A catalog that publishes pricing: free only when every published price is 0.
  const prices = Object.values(e?.pricing || {}).map(num).filter((v) => v !== null);
  return prices.length > 0 && prices.every((v) => v === 0);
}

function goFree(e) {
  return String(e?.id || '').endsWith('-free');
}

function normalize(provider, e, { inLadder }) {
  const id = String(e?.id || '');
  if (!id) return null;
  const rung = `${provider}/${id}`;
  const row = {
    provider,
    model_id: rung,
    name: String(e?.name || e?.id || ''),
    context: num(e?.context_length),
    price_in: 0,
    price_out: 0,
    price_cached: 0,
    owned_by: e?.architecture?.backend || e?.owned_by || null,
    description: typeof e?.description === 'string' ? e.description.slice(0, 500) : null,
    in_ladder: inLadder.has(rung) ? 1 : 0,
  };
  if (provider === 'openrouter') {
    row.price_in = perMillion(e?.pricing?.prompt) ?? 0;
    row.price_out = perMillion(e?.pricing?.completion) ?? 0;
    row.price_cached = perMillion(e?.pricing?.input_cache_read) ?? 0;
  }
  // zen and Go free tiers are subscription/unlimited — no per-token price is published.
  return row;
}

function ladderRungs(config) {
  const rungs = new Set();
  for (const roles of Object.values(config?.ladders || {})) {
    for (const list of Object.values(roles || {})) {
      for (const rung of list || []) {
        const id = String(rung);
        rungs.add(id);
        // `zen-rings/<model>` and `opencode-zen/<model>` are the same weights over different
        // transports (the SHARED mapping in test/free-models.test.js), but only `opencode-zen/*`
        // rows are ever emitted — the collector's prefix. A model served through the ring must
        // still read as in-ladder, or the inventory reports a model nobody uses while a rung
        // points straight at it.
        if (id.startsWith('zen-rings/')) rungs.add(`opencode-zen/${id.slice('zen-rings/'.length)}`);
      }
    }
  }
  return rungs;
}

// One provider → its free rows. A thrown error becomes { ok:false, error } so one dead provider
// cannot take the whole run down (and cannot mark its models gone — see reconcileMissing).
async function collectProvider(provider, env, { fetchImpl, timeoutMs, inLadder }) {
  const one = async () => {
    if (provider === 'openrouter') {
      const payload = await getJson(OPENROUTER_CATALOG_URL, { fetchImpl, timeoutMs });
      return entriesOf(payload).filter(openRouterFree).map((e) => normalize('openrouter', e, { inLadder }));
    }
    if (provider === 'opencode-zen') {
      const payload = await getJson(ZEN_CATALOG_URL, { fetchImpl, timeoutMs });
      return entriesOf(payload).filter(zenFree).map((e) => normalize('opencode-zen', e, { inLadder }));
    }
    if (provider === 'opencode-go') {
      // GET /models is PUBLIC (answers 200 with no key) — try that first, so a missing pool key
      // degrades nothing. If Go ever locks the catalog behind auth, fall back to the pool in order.
      const attempts = [{}, ...readPool(env).map((key) => ({ Authorization: `Bearer ${key}` }))];
      let lastErr = null;
      for (const headers of attempts) {
        try {
          const payload = await getJson(GO_CATALOG_URL, { fetchImpl, timeoutMs, headers });
          return entriesOf(payload).filter(goFree).map((e) => normalize('opencode-go', e, { inLadder }));
        } catch (e) { lastErr = e; }
      }
      throw lastErr || new Error('no Go pool key configured');
    }
    throw new Error(`unknown provider ${provider}`);
  };
  try {
    const models = (await one()).filter(Boolean);
    return { provider, ok: true, count: models.length, models };
  } catch (e) {
    return { provider, ok: false, error: String((e && e.message) || e), count: 0, models: [] };
  }
}

// All providers, in parallel. Returns { providers, rows } — rows is the flat free inventory.
// Rows are ordered by PROVIDERS order, then model_id: the inventory is a report, and a stable
// order keeps the diff readable (and the tests honest) regardless of which provider answered first.
export async function collectCatalogs(env, { fetchImpl = fetch, timeoutMs = 10_000, config } = {}) {
  const inLadder = ladderRungs(config);
  const providers = await Promise.all(PROVIDERS.map((p) => collectProvider(p, env, { fetchImpl, timeoutMs, inLadder })));
  const rank = new Map(PROVIDERS.map((p, i) => [p, i]));
  const rows = providers
    .flatMap((p) => p.models)
    .sort((a, b) => (rank.get(a.provider) - rank.get(b.provider)) || a.model_id.localeCompare(b.model_id));
  return { providers, rows };
}

// ── Availability probe ─────────────────────────────────────────────────────────────

// Which rows to probe this run: least-recently-probed first (NULLS FIRST), so a budget smaller
// than the inventory still rotates coverage instead of always hitting the same head.
export function planProbes(rows, { limit = 12 } = {}) {
  return [...rows]
    .sort((a, b) => (a.probed_at ?? 0) - (b.probed_at ?? 0))
    .slice(0, Math.max(0, limit));
}

function probeRequest(provider, modelId, env) {
  const bare = modelId.slice(modelId.indexOf('/') + 1);
  const base = provider === 'openrouter'
    ? 'https://openrouter.ai/api/v1'
    : provider === 'opencode-zen'
      ? ZEN_CATALOG_URL.replace('/models', '')
      : GO_CATALOG_URL.replace('/models', '');
  const headers = { 'content-type': 'application/json' };
  const body = { model: bare, max_tokens: 8, messages: [{ role: 'user', content: 'ping' }] };
  if (provider === 'openrouter') {
    if (!env.OPENROUTER_API_KEY) return { skip: 'no key' };
    headers.Authorization = `Bearer ${env.OPENROUTER_API_KEY}`;
  } else if (provider === 'opencode-zen') {
    // zen 403s without the fingerprint and without stream:true (issue #106). The status is the
    // whole answer we need, so the body is never consumed — the stream is cancelled on arrival.
    headers.Authorization = 'Bearer public';
    headers['user-agent'] = ZEN_UA;
    headers['x-opencode-client'] = 'cli';
    headers['x-opencode-project'] = 'global';
    headers['x-opencode-request'] = `msg_${randHex(12)}${randAlnum(12)}`;
    headers['x-opencode-session'] = `ses_${randHex(6)}${randAlnum(14)}`;
    body.stream = true;
    body.tools = [ZEN_SHELL_TOOL, ZEN_READ_TOOL];
  } else {
    const pool = readPool(env);
    if (!pool.length) return { skip: 'no key' };
    headers.Authorization = `Bearer ${pool[0]}`;
    // Go 400s a chat call without a session id (same rule the ladder applies in upstreamRequest).
    headers['x-opencode-session'] = `probe_${crypto.randomUUID().slice(0, 12)}`;
  }
  return { url: `${base}/chat/completions`, headers, body };
}

// A 200 means the model exists and answers. 429 is a free-tier limit (not a disappearance),
// 404/"model not found" is the only real "gone" signal, anything else is an error to look at.
function probeStatusOf(status, text) {
  if (status === 200) return 'ok';
  if (status === 429) return 'limited';
  if (status === 404 || /not found|unknown model|no such model/i.test(text)) return 'not_found';
  return `http_${status || 0}`;
}

// Light ping: one request per model, bounded by planProbes, never more often than the schedule.
// The key never travels further than the fetch headers — nothing here logs a request.
export async function probeRows(rows, env, { fetchImpl = fetch, timeoutMs = 15_000, concurrency = 4 } = {}) {
  const out = new Map();
  const queue = [...rows];
  const workers = Array.from({ length: Math.min(Math.max(1, concurrency), queue.length) }, async () => {
    for (;;) {
      const row = queue.shift();
      if (!row) return;
      const started = Date.now();
      const req = probeRequest(row.provider, row.model_id, env);
      if (req.skip) { out.set(row.model_id, { probe_status: 'skipped', probe_reason: req.skip, probed_at: started }); continue; }
      let status = 0, text = '';
      try {
        const res = await fetchImpl(req.url, { method: 'POST', headers: req.headers, body: JSON.stringify(req.body), signal: AbortSignal.timeout(timeoutMs) });
        status = res.status;
        text = await res.text().catch(() => '');
        if (res.body) await res.body.cancel().catch(() => {});
      } catch (e) {
        out.set(row.model_id, { probe_status: 'error', probe_reason: String((e && e.message) || e).slice(0, 120), probed_at: started });
        continue;
      }
      out.set(row.model_id, { probe_status: probeStatusOf(status, text), probed_at: started });
    }
  });
  await Promise.all(workers);
  return out;
}

// ── Diff ───────────────────────────────────────────────────────────────────────────

// prev/next are maps of model_id → row. `changed` covers the fields the issue names (context,
// price) plus name/owned_by, which are the other things a benchmark would care about.
// okProviders (optional): the providers whose fetch SUCCEEDED this run. A model whose provider
// did not answer is UNKNOWN, not gone — reporting it as gone would turn every provider outage
// into a false "model left" in the diff. Without the option every provider is assumed ok.
export function diffRows(prev, next, { okProviders } = {}) {
  const appeared = [], gone = [], changed = [];
  for (const [id, row] of next) {
    const before = prev.get(id);
    if (!before) { appeared.push(row); continue; }
    const fields = [];
    for (const f of ['context', 'price_in', 'price_out', 'price_cached', 'name', 'owned_by']) {
      if (before[f] !== row[f]) fields.push({ field: f, from: before[f] ?? null, to: row[f] ?? null });
    }
    if (fields.length) changed.push({ model_id: id, provider: row.provider, fields });
  }
  for (const [id, row] of prev) {
    if (next.has(id)) continue;
    if (okProviders && !okProviders.has(row.provider)) continue;
    gone.push(row);
  }
  const byId = (r) => r.model_id;
  return {
    appeared: appeared.sort((a, b) => byId(a).localeCompare(byId(b))),
    gone: gone.sort((a, b) => byId(a).localeCompare(byId(b))),
    changed: changed.sort((a, b) => a.model_id.localeCompare(b.model_id)),
  };
}

// ── D1 ─────────────────────────────────────────────────────────────────────────────

export const FREE_MODELS_TABLE = 'free_models';

export const FREE_MODEL_PROBE_HISTORY_TABLE = 'free_model_probe_history';

export const INSERT_PROBE_HISTORY_SQL = `INSERT INTO ${FREE_MODEL_PROBE_HISTORY_TABLE}
  (provider, model_id, checked_at, status) VALUES (?1, ?2, ?3, ?4)`;
export const PRUNE_PROBE_HISTORY_SQL = `DELETE FROM ${FREE_MODEL_PROBE_HISTORY_TABLE} WHERE checked_at < ?1`;

export function probeHistoryStatements(probes) {
  return Object.entries(probes || {})
    .filter(([, p]) => p?.probe_status && p.probe_status !== 'skipped')
    .map(([modelId, p]) => ({
      sql: INSERT_PROBE_HISTORY_SQL,
      params: [String(modelId).split('/')[0],
        String(modelId), Number(p.probed_at) || Date.now(), String(p.probe_status).slice(0, 32)],
    }));
}

export async function readFreeModelAvailability(db, { since = Date.now() - 7 * 86_400_000 } = {}) {
  const { results = [] } = await db.prepare(
    `SELECT provider, model_id, status, COUNT(*) AS observations, MAX(checked_at) AS last_checked `
      + `FROM ${FREE_MODEL_PROBE_HISTORY_TABLE} WHERE checked_at >= ?1 `
      + 'GROUP BY provider, model_id, status ORDER BY provider, model_id, status',
  ).bind(since).all();
  const providers = {};
  const models = new Map();
  for (const row of results) {
    const count = Number(row.observations) || 0;
    const provider = providers[row.provider] ||= { observations: 0, ok: 0, limited: 0, not_found: 0, http_errors: 0, errors: 0, skipped: 0 };
    const model = models.get(row.model_id) || {
      provider: row.provider, model_id: row.model_id, observations: 0,
      ok: 0, limited: 0, not_found: 0, http_errors: 0, errors: 0, skipped: 0, last_checked: 0,
    };
    const status = String(row.status || 'error');
    const bucket = status === 'ok' || status === 'limited' || status === 'not_found' || status === 'skipped'
      ? status : /^http_\d+$/.test(status) ? 'http_errors' : 'errors';
    model[bucket] += count;
    provider[bucket] += count;
    if (bucket !== 'skipped') { model.observations += count; provider.observations += count; }
    model.last_checked = Math.max(model.last_checked, Number(row.last_checked) || 0);
    models.set(row.model_id, model);
  }
  const rate = (n, d) => d ? Math.round((n / d) * 10_000) / 100 : null;
  for (const p of Object.values(providers)) {
    p.responding_percent = rate(p.ok + p.limited + p.not_found + p.http_errors, p.observations);
    p.serving_percent = rate(p.ok, p.observations);
  }
  const modelRows = [...models.values()].sort((a, b) => a.provider.localeCompare(b.provider) || a.model_id.localeCompare(b.model_id));
  for (const m of modelRows) {
    m.responding_percent = rate(m.ok + m.limited + m.not_found + m.http_errors, m.observations);
    m.serving_percent = rate(m.ok, m.observations);
  }
  return { since, providers, models: modelRows };
}

export const UPSERT_SQL = `INSERT INTO ${FREE_MODELS_TABLE}
  (provider, model_id, name, context, price_in, price_out, price_cached, owned_by, description,
   in_ladder, first_seen, last_seen, available, probe_status, probed_at)
  VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 1, ?13, ?14)
  ON CONFLICT(provider, model_id) DO UPDATE SET
    name = excluded.name, context = excluded.context,
    price_in = excluded.price_in, price_out = excluded.price_out, price_cached = excluded.price_cached,
    owned_by = excluded.owned_by, description = excluded.description, in_ladder = excluded.in_ladder,
    last_seen = excluded.last_seen, available = 1,
    probe_status = excluded.probe_status, probed_at = excluded.probed_at`;

// A row that left the catalog. `in_ladder` is recomputed against the CURRENT config here too
// (#318): a model that left the catalog AND left the ladders must not stay flagged in_ladder=1
// forever — the catalog upsert is the only other writer, and a gone model gets no upsert. A gone
// model that a ladder still names keeps in_ladder=1 (it IS still in the ladder; the removal PR is
// what has not happened yet).
export const GONE_SQL = `UPDATE ${FREE_MODELS_TABLE} SET available = 0, last_seen = ?1, in_ladder = ?4 WHERE provider = ?2 AND model_id = ?3`;

export const READ_SQL =
  'SELECT provider, model_id, name, context, price_in, price_out, price_cached, owned_by, description, '
  + 'in_ladder, first_seen, last_seen, available, probe_status, probed_at '
  + `FROM ${FREE_MODELS_TABLE} ORDER BY provider, model_id`;

export function upsertStatement(row, now) {
  return {
    sql: UPSERT_SQL,
    params: [
      row.provider, row.model_id, row.name, row.context,
      row.price_in, row.price_out, row.price_cached, row.owned_by, row.description, row.in_ladder,
      row.first_seen ?? now, now, row.probe_status ?? null, row.probed_at ?? null,
    ],
  };
}

export function goneStatement(row, now, inLadder = false) {
  return { sql: GONE_SQL, params: [now, row.provider, row.model_id, inLadder ? 1 : 0] };
}

// D1 has no multi-row VALUES insert; batch in chunks (db.batch is one round trip per chunk).
// A fake D1 without .batch falls back to sequential .run() so tests can assert per statement.
export async function runStatements(db, statements, { chunk = 50 } = {}) {
  if (!statements.length) return;
  if (typeof db.batch === 'function') {
    for (let i = 0; i < statements.length; i += chunk) {
      await db.batch(statements.slice(i, i + chunk).map((s) => db.prepare(s.sql).bind(...s.params)));
    }
    return;
  }
  for (const s of statements) await db.prepare(s.sql).bind(...s.params).run();
}

export async function readFreeModels(db) {
  const { results = [] } = await db.prepare(READ_SQL).all();
  return results;
}

// ── The whole run ──────────────────────────────────────────────────────────────────

// One collection pass: catalogs → probe → upsert → reconcile → diff. `prev` is the table as it
// was BEFORE this pass (read by the caller, so the read and the write share one transaction
// boundary in the handler). Returns everything the report needs.
export async function collectFreeModels(env, db, {
  fetchImpl = fetch, timeoutMs = 10_000, probe = true, probeLimit = 12, probeConcurrency = 4,
  config, write = true, benchmarkGo = false, benchmarkOpenrouter = benchmarkGo, benchLimit = 3, benchConcurrency = 3,
  now = Date.now(),
} = {}) {
  const prevRows = await readFreeModels(db);
  const prev = new Map(prevRows.map((r) => [r.model_id, r]));

  const { providers, rows } = await collectCatalogs(env, { fetchImpl, timeoutMs, config });
  const next = new Map(rows.map((r) => [r.model_id, r]));

  // Probe only what is in the catalog now — a model that left is not worth a request.
  const probeTargets = probe ? planProbes(rows, { limit: probeLimit }) : [];
  const probes = await probeRows(probeTargets, env, { fetchImpl, timeoutMs: 15_000, concurrency: probeConcurrency });
  for (const row of rows) {
    const p = probes.get(row.model_id);
    if (p) { row.probe_status = p.probe_status; row.probe_reason = p.probe_reason ?? null; row.probed_at = p.probed_at; }
  }

  const benchmarks = [];
  if (benchmarkGo && write) {
    const known = new Map((await readGoBenchmarks(db)).map((r) => [r.model_id, r]));
    const targets = rows.filter((r) => {
      if (r.provider !== 'opencode-go' || !r.model_id.endsWith('-free')) return false;
      const old = known.get(r.model_id);
      return !old || old.bench_status !== 'ready' || now - Number(old.measured_at || 0) > 86_400_000;
    }).slice(0, 3);
    for (const row of targets) {
      const result = await benchmarkGoModel(row, env, { fetchImpl, now });
      if (!result) continue;
      await writeGoBenchmark(db, result);
      benchmarks.push(result);
    }
  }

  // The OpenRouter free models are measured the same way, so the ladder's fast lane can order
  // them by the REAL answer time (the free tier is slow and rate-limited per key, so the models
  // are measured in parallel and the keys are rotated over the pool).
  if (benchmarkOpenrouter && write) {
    const keys = await openrouterBenchKeys(env, db);
    const known = new Map((await readBenchmarks(db, 'openrouter')).map((r) => [r.model_id, r]));
    const targets = rows.filter((r) => {
      if (r.provider !== 'openrouter' || !r.model_id.endsWith(':free')) return false;
      const old = known.get(r.model_id);
      return !old || old.bench_status !== 'ready' || now - Number(old.measured_at || 0) > 86_400_000;
    }).slice(0, benchLimit);
    let cursor = 0;
    const workers = Array.from({ length: Math.min(benchConcurrency, targets.length) }, async () => {
      for (;;) {
        const index = cursor++;
        if (index >= targets.length) return;
        const row = targets[index];
        const result = await benchmarkOpenrouterModel(row, env, {
          fetchImpl, now, timeoutMs: 25_000, keys,
        });
        if (!result) continue;
        if (result.status !== 'skipped') await writeBenchmark(db, 'openrouter', result);
        benchmarks.push(result);
      }
    });
    await Promise.all(workers);
  }

  const statements = rows.map((r) => upsertStatement({ ...r, first_seen: prev.get(r.model_id)?.first_seen ?? now }, now));

  // Reconcile ONLY the providers whose fetch succeeded: a dead provider must not mark its whole
  // catalog gone (that would be a false "model left" in every diff until it recovers).
  const okProviders = new Set(providers.filter((p) => p.ok).map((p) => p.provider));
  const seen = new Set(rows.map((r) => r.model_id));
  const rungsNow = ladderRungs(config);
  for (const row of prevRows) {
    if (!seen.has(row.model_id) && okProviders.has(row.provider)) {
      statements.push(goneStatement(row, now, rungsNow.has(row.model_id)));
    }
  }

  const written = write ? statements.length : 0;
  const probeEvents = probeHistoryStatements(Object.fromEntries(probes));
  if (write) {
    await runStatements(db, [
      ...statements,
      ...probeEvents,
      { sql: PRUNE_PROBE_HISTORY_SQL, params: [now - 180 * 86_400_000] },
    ]);
  }

  const diff = diffRows(prev, next, { okProviders });

  // Issue #335: запись изменений каталога в journal для истории и алертинга.
  // Автоматический запуск бенчмарка для новых Go моделей уже выполняется
  // в блоке benchmarkGo выше (строки 656-669), который бенчмарчит все
  // неизвестные/устаревшие модели.
  const catalogChanges = [];
  if (write) {
    // Определяем, какие модели были забенчмарчены в этом запуске
    const benchmarkedModels = new Set(benchmarks.map(b => b.model_id));

    for (const row of diff.appeared) {
      catalogChanges.push({
        provider: row.provider,
        change_type: 'appeared',
        model_id: row.model_id,
        details: JSON.stringify({ context: row.context, name: row.name }),
        benchmark_triggered: benchmarkedModels.has(row.model_id) ? 1 : 0,
      });
    }
    for (const row of diff.gone) {
      catalogChanges.push({
        provider: row.provider,
        change_type: 'gone',
        model_id: row.model_id,
        details: JSON.stringify({ name: row.name }),
        benchmark_triggered: 0,
      });
    }
    for (const change of diff.changed) {
      catalogChanges.push({
        provider: change.provider,
        change_type: 'changed',
        model_id: change.model_id,
        details: JSON.stringify({ fields: change.fields }),
        benchmark_triggered: 0,
      });
    }

    // Записываем изменения в catalog_changes
    if (catalogChanges.length > 0) {
      const changeStatements = catalogChanges.map(c => ({
        sql: `INSERT INTO catalog_changes (detected_at, provider, change_type, model_id, details, benchmark_triggered)
              VALUES (?1, ?2, ?3, ?4, ?5, ?6)`,
        params: [now, c.provider, c.change_type, c.model_id, c.details, c.benchmark_triggered],
      }));
      await runStatements(db, changeStatements);
    }
  }

  return {
    started_at: new Date(now).toISOString(),
    finished_at: new Date().toISOString(),
    now,
    providers,
    collected: rows.length,
    probed: probeTargets.length,
    probes: Object.fromEntries([...probes.entries()].map(([k, v]) => [k, v])),
    benchmarks,
    written,
    probe_events_written: write ? probeEvents.length : 0,
    diff,
    rows,
    catalog_changes: catalogChanges.length,
  };
}

// ── Report ─────────────────────────────────────────────────────────────────────────

const fmtPrice = (v) => (v === null || v === undefined ? '—' : `$${(v / PER_MILLION).toFixed(4)}`);
const fmtCtx = (v) => (v === null || v === undefined ? '—' : v >= 1e6 ? `${(v / 1e6).toFixed(1)}M` : `${Math.round(v / 1000)}k`);
const ago = (ms, now) => {
  if (!ms) return 'never';
  const s = Math.max(0, Math.round((now - ms) / 1000));
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.round(s / 60)}m ago`;
  if (s < 86400) return `${Math.round(s / 3600)}h ago`;
  return `${Math.round(s / 86400)}d ago`;
};

export function summarizeRun(run) {
  const byProvider = {};
  for (const p of run.providers) {
    byProvider[p.provider] = { ok: p.ok, count: p.count, error: p.error ?? null };
  }
  const probeCounts = {};
  const probesByProvider = {};
  for (const [modelId, v] of Object.entries(run.probes)) {
    const status = String(v.probe_status || 'unknown');
    probeCounts[status] = (probeCounts[status] || 0) + 1;
    const provider = String(modelId).split('/')[0];
    const counts = probesByProvider[provider] ||= {};
    counts[status] = (counts[status] || 0) + 1;
  }
  return {
    collected: run.collected,
    by_provider: byProvider,
    probed: run.probed,
    probe_events_written: run.probe_events_written ?? 0,
    benchmarked: run.benchmarks?.length || 0,
    probe_counts: probeCounts,
    probe_counts_by_provider: probesByProvider,
    written: run.written,
    appeared: run.diff.appeared.length,
    gone: run.diff.gone.length,
    changed: run.diff.changed.length,
  };
}

// Markdown for the Actions step summary / the CLI. Kept in the module so the endpoint, the
// script and the tests all render the same report.
export function markdownReport(run) {
  const s = summarizeRun(run);
  const L = [];
  L.push(`## Free-model inventory — ${run.started_at}`);
  L.push('');
  const prov = run.providers.map((p) => `${p.ok ? '' : '⚠ '}${p.provider} ${p.count}${p.ok ? '' : ` (${p.error})`}`).join(' · ');
  L.push(`**${s.collected} free models** — ${prov}`);
  L.push(`probed ${s.probed} (${s.probe_events_written} history observations) · free-model benchmarks ${s.benchmarked} · written ${s.written} rows · appeared ${s.appeared} · gone ${s.gone} · changed ${s.changed}`);
  if (run.benchmarks?.length) {
    L.push('', '### Free-model benchmarks', '');
    for (const b of run.benchmarks) {
      const tiers = Object.entries(b.benchmark?.tiers || {}).map(([name, value]) =>
        `${name}: ${value.successes}/${value.attempts}, ${value.p50_ms === null ? '—' : `${Math.round(value.p50_ms)}ms`}${value.correct ? `, correct ${value.correct}` : ''}`);
      L.push(`- \`${b.model_id}\` — **${b.status}**${tiers.length ? ` · ${tiers.join(' · ')}` : ''}`);
    }
  }
  const pc = Object.entries(s.probe_counts).map(([k, v]) => `${k} ${v}`).join(', ');
  if (pc) L.push(`probe: ${pc}`);
  for (const [provider, counts] of Object.entries(s.probe_counts_by_provider)) {
    L.push(`probe ${provider}: ${Object.entries(counts).map(([status, count]) => `${status} ${count}`).join(', ')}`);
  }
  if (s.appeared) {
    L.push('', `### New (${s.appeared})`, '');
    for (const r of run.diff.appeared) {
      L.push(`- \`${r.model_id}\` — ${r.name} · ctx ${fmtCtx(r.context)} · ${fmtPrice(r.price_in)}/${fmtPrice(r.price_out)} per 1M${r.in_ladder ? '' : ' · **not in a ladder**'}`);
    }
  }
  if (s.gone) {
    L.push('', `### Gone (${s.gone})`, '');
    for (const r of run.diff.gone) {
      L.push(`- \`${r.model_id}\` — last seen ${ago(r.last_seen, run.now)}`);
    }
  }
  if (s.changed) {
    L.push('', `### Changed (${s.changed})`, '');
    for (const c of run.diff.changed) {
      const parts = c.fields.map((f) => `${f.field} ${f.from ?? '—'} → ${f.to ?? '—'}`);
      L.push(`- \`${c.model_id}\` — ${parts.join(', ')}`);
    }
  }
  if (!s.appeared && !s.gone && !s.changed) L.push('', '_No changes since the previous run._');
  return L.join('\n');
}
export class FreeModelRefresher {
  constructor(options = {}) {
    this.env = options.env;
    this.intervalMs = options.intervalMs || 4 * 60 * 60 * 1000; // 4 hours default
    this.isRunning = false;
    this.lastRun = 0;
  }
  
  async run(force = false) {
    if (this.isRunning) {
      console.log('Collection already in progress, skipping...');
      return;
    }
    
    if (!force && Date.now() - this.lastRun < this.intervalMs) {
      console.log(`Collection not due yet. Next run in ${Math.max(0, (this.lastRun + this.intervalMs - Date.now()) / 1000)} seconds`);
      return;
    }
    
    this.isRunning = true;
    this.lastRun = Date.now();
    
    try {
      console.log(`Starting free model collection run at ${new Date().toISOString()}...`);
      await this.collectAndBenchmark();
      console.log('Collection run completed successfully');
    } catch (error) {
      console.error('Collection run failed:', error);
    } finally {
      this.isRunning = false;
    }
  }
  
  async collectAndBenchmark() {
    const config = { ladders: { free: { build: [] } } };
    const { providers, rows } = await collectCatalogs(this.env, { config });

    const run = await collectFreeModels(this.env, this.env.LADDER_TRACE_DB, {
      config,
      probe: true,
      probeLimit: 20,
      probeConcurrency: 4,
      benchmarkGo: false,
      benchmarkOpenRouter: true,
      now: Date.now()
    });

    if (run.written > 0) {
      console.log(`Collection: ${run.collected} free models collected, ${run.probed} probed, ${run.written} rows written`);
    }

    const selectedModels = this.selectTopModelsForBenchmark(run.rows);
    return selectedModels;
  }
  
  selectTopModelsForBenchmark(rows) {
    // Sort by speed first (p50_ms), then by model quality
    return rows
      .filter(row => row.provider === 'openrouter' && row.available && row.model_id.endsWith(':free'))
      .sort((a, b) => {
        // Try to get benchmark info if available
        const aScore = this.getModelScore(a);
        const bScore = this.getModelScore(b);
        
        if (aScore !== null && bScore !== null) {
          return aScore - bScore; // Better performance first
        }
        
        // Fallback to quality-based sorting
        const aQuality = this.getModelQuality(a.model_id);
        const bQuality = this.getModelQuality(b.model_id);
        
        if (aQuality !== bQuality) {
          return bQuality.localeCompare(aQuality); // Higher quality first
        }
        
        // Final fallback to model ID
        return a.model_id.localeCompare(b.model_id);
      })
      .slice(0, 3) // Take top 3 models
      .map(row => row.model_id);
  }
  
  getModelScore(row) {
    // Try to find existing benchmark data
    if (row.benchmark && row.bench_status === 'ready') {
      const tiers = typeof row.benchmark === 'string' ? 
        JSON.parse(row.benchmark).tiers : row.benchmark.tiers;
      const small = tiers.small;
      if (small && small.p50_ms !== null) {
        return small.p50_ms;
      }
    }
    return null;
  }
  
  getModelQuality(modelId) {
    const qualityMap = {
      'nemotron-3-super-120b-a12b:free': 'high',
      'nemotron-3-ultra-550b-a55b:free': 'high',
      'ling-3.0-flash-sante:free': 'high',
      'cohere/north-mini-code:free': 'medium',
      'dots-studio/dots-3-note-preview:free': 'medium',
      'nemotron-3-nano-omni-30b-a3b-reasoning:free': 'medium'
    };
    
    const modelName = modelId.split('/').pop().replace(':free', '');
    return qualityMap[modelName] || 'unknown';
  }
  
  scheduleRun() {
    return setInterval(() => {
      this.run().catch(error => {
        console.error('Scheduled collection failed:', error);
      });
    }, this.intervalMs);
  }
  
  scheduleBenchmark() {
    setInterval(() => {
      this.benchmarkTopModels().catch(error => {
        console.error('Benchmark run failed:', error);
      });
    }, 6 * 60 * 60 * 1000);
  }
  
  async benchmarkTopModels() {
    const topModels = await this.getTopModelsForBenchmark();
    if (!topModels || topModels.length === 0) {
      console.log('No models to benchmark');
      return;
    }
    
    console.log(`Starting benchmark for top ${topModels.length} OpenRouter free models...`);
    
    for (const modelId of topModels) {
      console.log(`Would benchmark: ${modelId}`);
    }
    
    return topModels;
  }
  
  async getTopModelsForBenchmark() {
    const db = this.env.LADDER_TRACE_DB;
    if (!db) return [];
    
    const { results = [] } = await db.prepare(
      'SELECT model_id FROM free_models ' +
      "WHERE provider = 'openrouter' AND available = 1 AND model_id LIKE '%:free' " +
      'ORDER BY last_seen DESC, model_id LIMIT 20'
    ).all();
    
    return results.map(r => r.model_id);
  }
}

