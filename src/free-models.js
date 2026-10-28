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

import { readPool } from './ladder.js';
import { isEmptyAnswer } from './answer-guard.js';

const PER_MILLION = 1_000_000;
const PROVIDERS = ['openrouter', 'opencode-zen', 'opencode-go'];
const ZEN_CATALOG_URL = 'https://api.opencode.ai/zen/v1/models';
const GO_CATALOG_URL = 'https://api.opencode.ai/go/v1/models';
const ZEN_UA = 'opencode/1.0.0 (opencode.ai)';
const ZEN_SHELL_TOOL = { name: 'bash', description: 'Run a bash command', parameters: { type: 'object', properties: { command: { type: 'string' } } } };
const ZEN_READ_TOOL = { name: 'read', description: 'Read a file', parameters: { type: 'object', properties: { path: { type: 'string' } } } };

const num = (v) => (typeof v === 'number' && Number.isFinite(v) ? v : null);
const perMillion = (v) => (typeof v === 'number' && Number.isFinite(v) ? v * PER_MILLION : null);
const randHex = (n) => Array.from({ length: n }, () => '0123456789abcdef'[Math.floor(Math.random() * 16)]).join('');
const randAlnum = (n) => Array.from({ length: n }, () => 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'[Math.floor(Math.random() * 62)]).join('');

export const GO_BENCH_TIERS = [
  { name: 'small', bytes: 1_024 },
  { name: 'medium', bytes: 32_768 },
  { name: 'large', bytes: 131_072 },
];

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
    let lastError = null;
    for (let i = 0; i < 2; i++) {
      const started = Date.now();
      try {
        const body = { ...req.body, max_tokens: 1500, messages: goBenchMessages(tier.bytes) };
        const res = await fetchImpl(req.url, { method: 'POST', headers: req.headers, body: JSON.stringify(body), signal: AbortSignal.timeout(timeoutMs) });
        const text = await res.text().catch(() => '');
        if (res.body) await res.body.cancel().catch(() => {});
        let payload = null;
        try { payload = JSON.parse(text); } catch { /* counted as a failed response */ }
        const content = String(payload?.choices?.[0]?.message?.content ?? '').trim();
        if (res.ok && !isEmptyAnswer(content)) { successes++; times.push(Date.now() - started); }
        else lastError = `http_${res.status}${isEmptyAnswer(content) ? ': empty answer' : ''}`;
      } catch (e) { lastError = String((e && e.message) || e).slice(0, 120); }
    }
    times.sort((a, b) => a - b);
    const p50 = !times.length ? null : times.length === 1 ? times[0] : (times[0] + times[1]) / 2;
    tiers[tier.name] = { attempts: 2, successes, p50_ms: p50, last_error: lastError };
  }
  const eligible = GO_BENCH_TIERS.some((t) => goBenchEligible({ tiers }, t.name));
  return { model_id: row.model_id, status: eligible ? 'ready' : 'failed', benchmark: { version: 1, measured_at: now, tiers } };
}

export async function readGoBenchmarks(db) {
  const { results = [] } = await db.prepare(
    'SELECT b.model_id, b.bench_status, b.benchmark, b.measured_at, f.available FROM free_model_benchmarks b '
      + 'JOIN free_models f ON f.provider = b.provider AND f.model_id = b.model_id '
      + "WHERE b.provider = 'opencode-go'",
  ).all();
  return results.map((r) => ({ ...r, benchmark: typeof r.benchmark === 'string' ? JSON.parse(r.benchmark) : r.benchmark, available: !!r.available }));
}

export const UPSERT_GO_BENCH_SQL = `INSERT INTO free_model_benchmarks
  (provider, model_id, bench_status, benchmark, measured_at)
  VALUES ('opencode-go', ?1, ?2, ?3, ?4)
  ON CONFLICT(provider, model_id) DO UPDATE SET
    bench_status = excluded.bench_status, benchmark = excluded.benchmark, measured_at = excluded.measured_at`;

export async function writeGoBenchmark(db, result) {
  await db.prepare(UPSERT_GO_BENCH_SQL).bind(result.model_id, result.status,
    result.benchmark ? JSON.stringify(result.benchmark) : null, result.benchmark?.measured_at ?? Date.now()).run();
}

export const OPENROUTER_CATALOG_URL = 'https://openrouter.ai/api/v1/models';
export const OPENROUTER_BENCH_TIERS = [
  { name: 'small', bytes: 512 },
  { name: 'medium', bytes: 4096 },
  { name: 'large', bytes: 16384 },
];

export function openRouterBenchTier(tokens) {
  return tokens < 1024 ? 'small' : tokens < 8192 ? 'medium' : 'large';
}

function benchTierResult(row, tier) {
  try {
    const b = typeof row.benchmark === 'string' ? JSON.parse(row.benchmark) : row.benchmark;
    const s = b?.tiers?.[tier];
    return row.bench_status === 'ready' && row.available && 
           s?.attempts >= 2 && s?.successes >= 2 && 
           Number.isFinite(s.p50_ms) ? s : null;
  } catch {
    return null;
  }
}

export async function benchmarkOpenRouterModel(row, env, { 
  fetchImpl = fetch, 
  timeoutMs = 60_000, 
  now = Date.now() 
} = {}) {
  if (!env.OPENROUTER_API_KEY) return null;
  
  const req = probeRequest(row.provider, row.model_id, env);
  if (req.skip) return { model_id: row.model_id, status: 'skipped', reason: req.skip, benchmark: null };
  
  const tiers = {};
  for (const tier of OPENROUTER_BENCH_TIERS) {
    const times = [];
    let successes = 0;
    let lastError = null;
    
    for (let i = 0; i < 2; i++) {
      const started = Date.now();
      try {
        const body = {
          ...req.body, 
          max_tokens: 2048,
          messages: [{ role: 'system', content: 'Respond with exactly: benchmark' }],
          model: row.model_id.split('/').pop().replace(':free', '')
        };
        const res = await fetchImpl(req.url, {
          method: 'POST', 
          headers: req.headers, 
          body: JSON.stringify(body), 
          signal: AbortSignal.timeout(timeoutMs)
        });
        
        const text = await res.text().catch(() => '');
        if (res.body) await res.body.cancel().catch(() => {});
        
        if (res.ok) {
          successes++;
          times.push(Date.now() - started);
        } else {
          lastError = `HTTP ${res.status}`;
        }
      } catch (e) {
        lastError = String((e && e.message) || e).slice(0, 120);
      }
    }
    
    times.sort((a, b) => a - b);
    const p50 = !times.length ? null : times.length === 1 ? times[0] : (times[0] + times[1]) / 2;
    tiers[tier.name] = { attempts: 2, successes, p50_ms: p50, last_error: lastError };
  }
  
  const eligible = OPENROUTER_BENCH_TIERS.some(t => 
    benchTierResult({ model_id: row.model_id, bench_status: 'ready', benchmark: { tiers } }, t.name)
  );
  
  return {
    model_id: row.model_id,
    status: eligible ? 'ready' : 'failed',
    benchmark: { version: 2, measured_at: now, tiers }
  };
}

export async function readOpenRouterBenchmarks(db) {
  const { results = [] } = await db.prepare(
    'SELECT b.model_id, b.bench_status, b.benchmark, f.available FROM free_model_benchmarks b ' +
    'JOIN free_models f ON f.provider = b.provider AND f.model_id = b.model_id ' +
    "WHERE b.provider = 'openrouter' AND f.available = 1"
  ).all();
  
  return results.map(r => ({
    ...r,
    benchmark: typeof r.benchmark === 'string' ? JSON.parse(r.benchmark) : r.benchmark,
    available: !!r.available
  }));
}

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

function openRouterFree(e) {
  const inP = num(e?.pricing?.prompt);
  const outP = num(e?.pricing?.completion);
  return String(e?.id || '').endsWith(':free') || (inP === 0 && outP === 0);
}

function zenFree(e) {
  const id = String(e?.id || '');
  if (id.endsWith('-free')) return true;
  if (id === 'big-pickle') return true;
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
  return row;
}

function ladderRungs(config) {
  const rungs = new Set();
  for (const roles of Object.values(config?.ladders || {})) {
    for (const list of Object.values(roles || {})) {
      for (const rung of list || []) {
        const id = String(rung);
        rungs.add(id);
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
export async function collectCatalogs(env, { fetchImpl = fetch, timeoutMs = 10_000, config } = {}) {
  const inLadder = ladderRungs(config);
  const providers = await Promise.all(PROVIDERS.map((p) => collectProvider(p, env, { fetchImpl, timeoutMs, inLadder })));
  const rank = new Map(PROVIDERS.map((p, i) => [p, i]));
  const rows = providers
    .flatMap((p) => p.models)
    .sort((a, b) => (rank.get(a.provider) - rank.get(b.provider)) || a.model_id.localeCompare(b.model_id));
  return { providers, rows };
}

// ── Availability probe ───────────────────────────────────────────────────────────

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
    headers['x-opencode-session'] = `probe_${crypto.randomUUID().slice(0, 12)}`;
  }
  return { url: `${base}/chat/completions`, headers, body };
}

function probeStatusOf(status, text) {
  if (status === 200) return 'ok';
  if (status === 429) return 'limited';
  if (status === 404 || /not found|unknown model|no such model/i.test(text)) return 'not_found';
  return `http_${status || 0}`;
}

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

export const GONE_SQL = `UPDATE ${FREE_MODELS_TABLE} SET available = 0, last_seen = ?1 WHERE provider = ?2 AND model_id = ?3`;

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

export function goneStatement(row, now) {
  return { sql: GONE_SQL, params: [now, row.provider, row.model_id] };
}

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

export async function collectFreeModels(env, db, {
  fetchImpl = fetch, timeoutMs = 10_000, probe = true, probeLimit = 12, probeConcurrency = 4,
  config, write = true, benchmarkGo = false, benchmarkOpenRouter = true, now = Date.now(),
} = {}) {
  const prevRows = await readFreeModels(db);
  const prev = new Map(prevRows.map((r) => [r.model_id, r]));

  const { providers, rows } = await collectCatalogs(env, { fetchImpl, timeoutMs, config });
  const next = new Map(rows.map((r) => [r.model_id, r]));

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

  if (benchmarkOpenRouter && write) {
    const known = new Map((await readOpenRouterBenchmarks(db)).map((r) => [r.model_id, r]));
    const targets = rows.filter((r) => {
      if (r.provider !== 'openrouter' || !r.model_id.endsWith(':free')) return false;
      const old = known.get(r.model_id);
      return !old || old.bench_status !== 'ready' || now - Number(old.measured_at || 0) > 86_400_000;
    }).slice(0, 3);
    for (const row of targets) {
      const result = await benchmarkOpenRouterModel(row, env, { fetchImpl, now });
      if (!result) continue;
      await writeGoBenchmark(db, result);
      benchmarks.push(result);
    }
  }

  const statements = rows.map((r) => upsertStatement({ ...r, first_seen: prev.get(r.model_id)?.first_seen ?? now }, now));

  const okProviders = new Set(providers.filter((p) => p.ok).map((p) => p.provider));
  const seen = new Set(rows.map((r) => r.model_id));
  for (const row of prevRows) {
    if (!seen.has(row.model_id) && okProviders.has(row.provider)) statements.push(goneStatement(row, now));
  }

  const written = write ? statements.length : 0;
  if (write) await runStatements(db, statements);

  const diff = diffRows(prev, next, { okProviders });
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
    diff,
    rows,
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
  for (const v of Object.values(run.probes)) probeCounts[v.probe_status] = (probeCounts[v.probe_status] || 0) + 1;
  return {
    collected: run.collected,
    by_provider: byProvider,
    probed: run.probed,
    benchmarked: run.benchmarks?.length || 0,
    probe_counts: probeCounts,
    written: run.written,
    appeared: run.diff.appeared.length,
    gone: run.diff.gone.length,
    changed: run.diff.changed.length,
  };
}

export function markdownReport(run) {
  const s = summarizeRun(run);
  const L = [];
  L.push(`## Free-model inventory — ${run.started_at}`);
  L.push('');
  const prov = run.providers.map((p) => `${p.ok ? '' : '⚠ '}${p.provider} ${p.count}${p.ok ? '' : ` (${p.error})`}`).join(' · ');
  L.push(`**${s.collected} free models** — ${prov}`);
  L.push(`probed ${s.probed} · Go benchmarks ${s.benchmarked} · written ${s.written} rows · appeared ${s.appeared} · gone ${s.gone} · changed ${s.changed}`);
  if (run.benchmarks?.length) {
    L.push('', '### Go free-model benchmarks', '');
    for (const b of run.benchmarks) {
      const tiers = Object.entries(b.benchmark?.tiers || {}).map(([name, value]) =>
        `${name}: ${value.successes}/${value.attempts}, ${value.p50_ms === null ? '—' : `${Math.round(value.p50_ms)}ms`}`);
      L.push(`- \`${b.model_id}\` — **${b.status}**${tiers.length ? ` · ${tiers.join(' · ')}` : ''}`);
    }
  }
  const pc = Object.entries(s.probe_counts).map(([k, v]) => `${k} ${v}`).join(', ');
  if (pc) L.push(`probe: ${pc}`);
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

