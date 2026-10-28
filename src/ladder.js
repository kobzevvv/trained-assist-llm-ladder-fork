import { orderBenchmarkedGoRungs, orderBenchmarkedRungs, readGoBenchRows, readBenchRows, nextFreeGoKeyIndex, nextOpenrouterKeyIndex, nextUsableOpenrouterIndex } from './go-routing.js';
export { orderBenchmarkedGoRungs, orderBenchmarkedRungs, readGoBenchRows, readBenchRows, nextFreeGoKeyIndex, nextOpenrouterKeyIndex, nextUsableOpenrouterIndex, resetFreeGoKeyCursor, resetOpenrouterKeyCursor } from './go-routing.js';
// last. Non-streaming, for small service calls (buttons, formatting, classifiers, summaries)
// where reliability beats everything else.
//
//   * model health — a flaky / limited rung is skipped for everyone (shared state in `store`);
//   * Go key pool — a key-level fault rotates to the spare key and the rung is retried; a rung
//     that fails for a NON-key reason also gets ONE spare-key probe per call before the ladder
//     leaves Go for paid OpenRouter (a silently throttled key looks like a slow model); once
//     every key is parked, the PAID Go rungs are skipped until the earliest key heals — free
//     Go rungs (`*-free`) keep serving, they do not eat the allowance (#69, #36);
//   * guard — empty content, or non-JSON when JSON was requested, fails the rung.
//
// Pure apart from `fetchImpl` and `store`, so it runs the same in the Worker (store = Durable
// Object) and in node:test (store = in-memory).

import { classifyError } from './classify.js';
import { deadRungs } from './dead-rungs.js';
import { orAccountLimit, orAccountLimitOf, orParkUntil } from './or-limits.js';
import { estimateTokens, estimateInputTokens, hedgePlan, ttfbFactor, GO_CONTEXT, canFitTokens, contextOf } from './size-policy.js';
import { ringInvoke, ringWaitForTask, ringBoot, ringCooldown, abandonQueuedTasks } from './zen-ring.js';
import { isEmptyAnswer } from './answer-guard.js';
import { resolveProviderPool } from './provider-credentials.js';
import { optimizeContext } from './context-optimizer.js';

// Go models reason before answering and max_tokens covers the reasoning too — a tight budget
// (e.g. 5 tokens for YES/NO) would otherwise come back empty.
export const MIN_TOKENS = 1500;
// #38 (owner decision, variant 2): 1500 is not enough for rungs whose chain-of-thought eats the
// whole floor — prod showed `empty answer (finish=length, out=1500, reasoning=1500, prompt=97,
// max_tokens=1500)`, i.e. the reasoning consumed every token, content came back empty, the guard
// failed the rung (chronic 1–25 empty answers/hour). Reasoning rungs get a higher floor instead
// of raising it for everyone.
//
// REASONING_MODELS is empirical, not guessed from names (issue #38): every unique rung of
// config/ladders.json was pinned through the live worker (ladder_rung) and read
// `usage.completion_tokens_details.reasoning_tokens` — nonzero → reasoning. Two Go rungs report
// no details (deepseek-v4-flash) or 0 (glm-5.3-flash) while returning a long
// `message.reasoning_content` (855ch / 517ch against 2ch of content) — reasoning that the usage
// counter does not count, so they are in. Excluded: openrouter/google/gemini-2.5-flash-lite
// (reasoning_tokens=0 twice, works in visible content). ling-3.0-flash-fin:free had no data
// either and is gone from the ladder (dead 404, removed 2026-09-30). See the PR for the full per-rung table.
//
// Added after #39 reordered deepseek: opencode-go/space-bunny-free measured reasoning_tokens=32
// (+ reasoning_content 100ch) through the live worker. The four opencode-zen/* rungs (now the tail
// of the free ladder, #43) were measured 30.09 through the live worker once the relay was up —
// mimo-v2.6-flash-free 17, mimo-v2.5-free 15, nemotron-3.5-lightning-free 255, big-pickle 43 —
// and are added (#42).
export const REASONING_MIN_TOKENS = 3000;
export const REASONING_MODELS = [
  'opencode-go/space-bunny-free',
  'opencode-go/mimo-v2.6-flash',
  'opencode-go/deepseek-v4.1-flash',
  'openrouter/nvidia/nemotron-3-super-120b-a12b:free',
  'openrouter/inclusionai/ling-3.0-flash-sante:free',
  'openrouter/inclusionai/ling-3.0-flash',
  'openrouter/xiaomi/mimo-v2.6-flash',
  'opencode-go/deepseek-v4-flash',
  'opencode-go/beam-free',
  'opencode-go/qwen3.8-flash',
  'opencode-go/deepseek-flash',
  'opencode-go/glm-5.3-flash',
  'openrouter/nvidia/nemotron-3-ultra-550b-a55b:free',
  'openrouter/cohere/north-mini-code:free',
  'openrouter/nvidia/nemotron-3-nano-omni-30b-a3b-reasoning:free',
  'openrouter/poolside/laguna-xs-2.1:free',
  'openrouter/dots-studio/dots-3-note-preview:free',
  'opencode-go/qwen3.7-plus',
  'opencode-go/deepseek-v4-pro',
  'opencode-zen/mimo-v2.6-flash-free',
  'opencode-zen/mimo-v2.5-free',
  'opencode-zen/nemotron-3.5-lightning-free',
  'opencode-zen/big-pickle',
];
const REASONING = new Set(REASONING_MODELS);

// The floor for this rung — what upstreamRequest clamps to and what the #34 guard diagnostic
// prints, so the diag always shows the max_tokens that actually went out.
export function minTokensFor(model) {
  return REASONING.has(model) ? REASONING_MIN_TOKENS : MIN_TOKENS;
}

// OpenRouter context windows, read from the free-model inventory. Fail-open: a D1 outage
// must not turn every OpenRouter rung into a pre-filtered skip — an empty catalog means
// "unknown window", which is exactly what contextOf() returns for openrouter/* anyway.
export async function readOrContextCatalog(env) {
  const db = env?.LADDER_TRACE_DB;
  if (!db) return {};
  try {
    const { results = [] } = await db.prepare(
      'SELECT model_id, context FROM free_models WHERE provider = ? AND available = 1 AND context IS NOT NULL',
    ).bind('openrouter').all();
    const out = {};
    for (const r of results) out[r.model_id] = Number(r.context);
    return out;
  } catch { return {}; }
}

export const DEFAULT_LADDER = 'service'; // legacy alias 'deepseek' resolves here (issue #49)
const DEFAULT_ROLE = 'build';
// OpenRouter app attribution (#33): HTTP-Referer URL *is* the application identity in the
// OpenRouter dashboard ("Application" cut), Title is its display name, hidden keeps our internal
// tools out of public rankings while keeping the analytics.
export const DEFAULT_APP_SLUG = 'llm-ladder';
export const DEFAULT_APP_TITLE = 'Trained Assist';
export const APP_REFERER_BASE = 'https://recruiter-assistant.ru/app';
const SLUG_RE = /^[a-z0-9-]{1,64}$/;

// Caller-supplied slug → a clean URL segment, or the generic default. Anything that is not a
// slug already (Cyrillic, spaces, path traversal, overlong) falls back to llm-ladder instead of
// being repaired — a half-sanitised slug would silently become a DIFFERENT application upstream.
export function sanitizeAppSlug(raw) {
  const s = String(raw ?? '').trim().toLowerCase();
  return SLUG_RE.test(s) ? s : DEFAULT_APP_SLUG;
}

// The same rule, but "no app" stays a VALUE instead of becoming a name (#136).
//
// sanitizeAppSlug's default is right for the OpenRouter headers (#33): HTTP-Referer must name
// something, so a caller with no `x-ladder-app` is honestly the generic proxy — 'llm-ladder' IS
// what that request is upstream. It is the wrong value for the D1 attribution column: there
// DEFAULT_APP_SLUG is written as if a tool had claimed it, and no caller ever does (the slug comes
// from our own constant), so the apps cut cannot tell "unknown caller" from a real app and the
// router's own traffic outranks every real one — 264 of 466 attributed calls in the first hour
// after the column landed (2026-10-05).
//
// Same regex on purpose: the two must agree on WHICH strings are slugs, so a D1 row and the
// OpenRouter view of one call can never name it differently. They differ only in what happens
// when the answer is "not a slug".
export function sanitizeAppSlugOrNull(raw) {
  const s = String(raw ?? '').trim().toLowerCase();
  return SLUG_RE.test(s) ? s : null;
}

// Display name for X-OpenRouter-Title: strip control characters (header safety), cap the length,
// blank → the default title.
export function sanitizeAppTitle(raw) {
  const s = String(raw ?? '').replace(/[\x00-\x1f\x7f]/g, '').trim().slice(0, 64);
  return s || DEFAULT_APP_TITLE;
}
// Only KEY-level signals rotate a Go key; a 503 / Bad Request is about one model.
const KEY_QUOTA_RE = /usage limit|quota[^.]{0,20}exceeded|rate[_\s-]{0,5}limit|too many requests|\b429\b|more credits?/i;
const DEAD_KEY_RE = /invalid credential|invalid api key|\b401\b|unauthorized/i;
// An account with no payment / no tariff returns an immediate error (typically "insufficient
// account funds" or "subscription required"). This is a per-ACCOUNT problem, not a per-model
// one — the next key in the pool can serve the same call. Unlike 429 (temporary rate limit),
// this error comes back instantly, so rotating to the next key costs nothing but the round trip.
const NO_PAYMENT_RE = /subscription required|insufficient account funds|no.*payment.*method|no.*tariff/i;
export const KEY_QUOTA_TTL_MS = 15 * 60 * 1000;
export const KEY_DEAD_TTL_MS = 60 * 60 * 1000;
// A WEEKLY Go allowance (the 429 body carries `"limitName":"weekly"`, seen live 2026-09-29) does
// not heal in 15 minutes: parking it for the rate-limit TTL means the next rotation immediately
// bounces back onto a key that is still limited for days. Park it for hours instead.
export const KEY_WEEKLY_TTL_MS = 6 * 60 * 60 * 1000;
const WEEKLY_LIMIT_RE = /limitName["'\s:]{0,6}weekly/i;

export function readPool(env) {
  return String(env.OPENCODE_GO_API_KEYS || env.OPENCODE_GO_API_KEY || '').split(/[\s,]+/).map(s => s.trim()).filter(Boolean);
}

// OpenRouter pool index — wraps around so any starting cursor stays in range.
// With one key this is always 0 (the previous single-key behaviour).
export function openrouterKeyIndex(poolSize, cursor) {
  if (poolSize <= 0) return 0;
  const c = Number.isInteger(cursor) ? cursor : 0;
  return ((c % poolSize) + poolSize) % poolSize;
}

// Remaining Go allowance per pool key: GET /zen/go/v1/usage returns ONE unified percent per
// window — rolling (~5h) / weekly / monthly — not a per-model breakdown (owner observation
// 2026-10-03, issue #91). The raw key never leaves this function: callers see only the pool index.
export async function fetchGoUsage(env, { fetchImpl = fetch, timeoutMs = 8000 } = {}) {
  let pool = readPool(env);
  // The admin-managed credentials live in D1 next to the env pool — the operator sees both.
  if (env?.LADDER_TRACE_DB && env.PROVIDER_CREDS_KEY) {
    try { pool = (await resolveProviderPool(env, env.LADDER_TRACE_DB)).go; } catch { /* keep env pool */ }
  }
  const base = env.OPENCODE_GO_BASE_URL || 'https://opencode.ai/zen/go/v1';
  const one = async (key, keyIndex) => {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      const res = await fetchImpl(`${base}/usage`, { headers: { Authorization: `Bearer ${key}` }, signal: ctrl.signal });
      if (!res.ok) return { keyIndex, error: `HTTP ${res.status}` };
      const u = (await res.json()).usage || {};
      const win = (w) => (w ? { status: w.status ?? null, percent: w.percent ?? null, resetsAt: w.resetsAt ?? null } : null);
      return { keyIndex, rolling: win(u.rolling), weekly: win(u.weekly), monthly: win(u.monthly) };
    } catch (e) {
      return { keyIndex, error: e && e.name === 'AbortError' ? 'timeout' : String((e && e.message) || e) };
    } finally {
      clearTimeout(timer);
    }
  };
  return Promise.all(pool.map(one));
}

// ── Go usage snapshot: which keys are capped, memoized ───────────────────────────────────────
// A PAID model on a monthly-capped key does not answer fast — the gateway holds the request until
// the rung budget runs out (live 2026-10-27: 16–31 s per call on `standard`, issue #316). The
// free tier keeps working on those keys (Go docs: «If you reach the usage limit, you can continue
// using the free models»), so the cap is NOT a park reason — it is a routing hint for PAID rungs
// only. `fetchGoUsage` already knows every key's window; this memo makes asking it cheap:
// one parallel usage burst at most once per TTL per isolate, and a failed look returns `null`
// (unknown → behave exactly as before, never worse).
let goUsageMemo = { at: 0, ttl: 0, capped: null };
export function resetGoUsageMemo() { goUsageMemo = { at: 0, ttl: 0, capped: null }; }
export async function goMonthlyCappedKeys(env, { fetchImpl = fetch, now = Date.now() } = {}) {
  if (goUsageMemo.capped && now - goUsageMemo.at < goUsageMemo.ttl) return goUsageMemo.capped;
  try {
    const rows = await fetchGoUsage(env, { fetchImpl, timeoutMs: 3000 });
    const okRows = (rows || []).filter((x) => x && !x.error);
    if (!okRows.length) {
      // Every key errored — the usage endpoint is unavailable. Unknown beats a wrong answer;
      // a short TTL retries it soon instead of pinning the whole pool as healthy.
      goUsageMemo = { at: now, ttl: 30_000, capped: null };
      return null;
    }
    const capped = new Set(okRows
      .filter((x) => x.monthly && x.monthly.status === 'rate-limited')
      .map((x) => x.keyIndex));
    goUsageMemo = { at: now, ttl: 120_000, capped };
    return capped;
  } catch {
    return null;
  }
}

// OpenRouter: сколько ключу разрешено и сколько осталось на аккаунте — зеркало /v1/go-usage.
//
// Зачем: платный хвост умирает на `402 insufficient credits` (с 2026-10-04, 2833 отказа в
// трассе), а бесплатные `:free`-модели едут на ОТДЕЛЬНОМ дневном разрешении — и до сих пор
// ни то, ни другое не было видно без консоли OpenRouter. Замер 2026-10-08: за сутки лестница
// отдала 348 ответов через `:free`-ранги, дневной потолок по замеру из #86 — около 1000.
//
// ПУЛ — ЭТО НЕ ОДИН АККАУНТ. Каждый ключ пула принадлежит своему аккаунту OpenRouter, и
// дневной счётчик `free_model_daily_requests` у каждого СВОЙ (замерено 2026-10-22: у env-аккаунта
// 924/1000, а у четырёх аккаунтов из D1 — 12–57 при лимите 50). Потолок аккаунта выбирается по
// тому, сколько на нём куплено кредитов за всё время: меньше $10 — 50 запросов в сутки, от $10 —
// 1000. Поэтому лимит считается ПО ВСЕМ аккаунтам пула: один аккаунт на нуле — это не конец
// бесплатного хвоста (остальные ещё живы), конец — когда пул пуст.
//
// Два запроса на ключ, потому что они отвечают на разные вопросы:
//   GET /api/v1/auth/key  — лимит/расход ИМЕННО этого ключа (limit, usage, is_free_tier);
//   GET /api/v1/credits   — баланс аккаунта (total_credits, total_usage) — на него смотрит
//                           платный хвост, ключ может быть вообще без своего лимита.
// Сырые объекты возвращаются как есть: поля OpenRouter — это ответ API, а не наш контракт.
// Ключ в ответ не попадает никогда (как и в /v1/go-usage).
export async function fetchOrUsage(env, { fetchImpl = fetch, timeoutMs = 8000 } = {}) {
  let keys = env.OPENROUTER_API_KEY ? [env.OPENROUTER_API_KEY] : [];
  let labels = env.OPENROUTER_API_KEY ? ['env'] : [];
  if (env?.LADDER_TRACE_DB && env.PROVIDER_CREDS_KEY) {
    try {
      const resolved = await resolveProviderPool(env, env.LADDER_TRACE_DB);
      if (resolved.openrouter.length) {
        keys = resolved.openrouter;
        labels = resolved.openrouterLabels || keys.map((_, i) => `key-${i}`);
      }
    } catch { /* keep env pool */ }
  }
  if (!keys.length) return { key: null, credits: null, accounts: [], pool: null, error: 'OPENROUTER_API_KEY not configured' };
  const base = env.OPENROUTER_BASE_URL || 'https://openrouter.ai/api/v1';
  const one = async (pathname, token) => {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      const res = await fetchImpl(`${base}${pathname}`, { headers: { Authorization: `Bearer ${token}` }, signal: ctrl.signal });
      const text = await res.text();
      if (!res.ok) return { error: `HTTP ${res.status}: ${text.slice(0, 200)}` };
      try { return JSON.parse(text); } catch { return { error: `non-JSON: ${text.slice(0, 120)}` }; }
    } catch (e) {
      return { error: e && e.name === 'AbortError' ? 'timeout' : String((e && e.message) || e) };
    } finally {
      clearTimeout(timer);
    }
  };
  const raw = await Promise.all(keys.map(async (token) => {
    const [key, credits] = await Promise.all([one('/auth/key', token), one('/credits', token)]);
    return { key, credits };
  }));
  const accounts = raw.map(({ key, credits }, index) => {
    const data = (key && key.data) || {};
    const daily = data.free_model_daily_requests || null;
    const used = daily ? Number(daily.used) || 0 : null;
    const limit = daily ? Number(daily.limit) || 0 : null;
    const cd = (credits && credits.data) || null;
    return {
      index,
      label: labels[index] || `key-${index}`,
      used,
      limit,
      // `remaining` у провайдера бывает отрицательным (отклонённые попытки в счётчик идут),
      // а плану нужен честный «сколько ещё можно» — ноль, а не минус.
      remaining: daily ? Math.max(0, limit - used) : null,
      is_free_tier: !!data.is_free_tier,
      credits: cd ? { total_credits: Number(cd.total_credits) || 0, total_usage: Number(cd.total_usage) || 0 } : null,
      error: (key && key.error) || (credits && credits.error) || null,
    };
  });
  const counted = accounts.filter((a) => a.limit != null);
  const totals = {
    accounts: accounts.length,
    counted: counted.length,
    used: counted.reduce((s, a) => s + (a.used || 0), 0),
    limit: counted.reduce((s, a) => s + (a.limit || 0), 0),
    remaining: counted.reduce((s, a) => s + (a.remaining || 0), 0),
    exhausted: counted.filter((a) => (a.remaining || 0) <= 0).length,
    credit_total: accounts.reduce((s, a) => s + (a.credits ? a.credits.total_credits : 0), 0),
    credit_used: accounts.reduce((s, a) => s + (a.credits ? a.credits.total_usage : 0), 0),
  };
  // `key`/`credits` — сырые ответы ПЕРВОГО ключа пула: старый контракт /v1/or-usage.
  return { key: raw[0]?.key ?? null, credits: raw[0]?.credits ?? null, accounts, pool: totals };
}

// "service" (legacy alias "deepseek"), "service:review" or an alias ("free-ladder") → rungs, null if unknown.
// Aliases resolve on READ only (never written back), so stored state keeps working after a rename.
export function rungsFor(config, name) {
  const [ladderName, role] = String(name || DEFAULT_LADDER).split(':');
  const resolved = (config.aliases && config.aliases[ladderName]) || ladderName;
  const l = config.ladders && config.ladders[resolved];
  if (!l) return null;
  return l[role || DEFAULT_ROLE] || l[DEFAULT_ROLE] || null;
}

function stripFences(s) {
  return String(s || '').replace(/^\s*```(?:json)?\s*/i, '').replace(/\s*```\s*$/, '').trim();
}

// Fenced or prose-wrapped JSON is common from small models. undefined = not JSON.
export function parseJson(content) {
  const raw = stripFences(content);
  try { return JSON.parse(raw); } catch { /* fall through */ }
  const m = raw.match(/\{[\s\S]*\}/);
  if (m) { try { return JSON.parse(m[0]); } catch { /* not JSON */ } }
  return undefined;
}

// #34 diagnostics ride INSIDE the error string (attempts → console.log / D1). Classification
// must not see them: a token count of 401/429/503 would otherwise rotate/park a healthy Go key
// or class the rung as quota — instrumentation must not move the ladder.
const GUARD_DIAG_RE = /\s*\((?:finish|out|reasoning|prompt|max_tokens)=[^()]*\)$/;
function classSignal(errorText) {
  return String(errorText || '').replace(GUARD_DIAG_RE, '');
}

// classify + strip in one step — every error-class decision must go through here so the #34
// diagnostic can never reach the CLASSIFIERS.
function errorClass(errorText) {
  return classifyError(classSignal(errorText));
}

// The account-level OpenRouter limit of a failed response (#297). Only OpenRouter rungs have
// per-account allowances — a Go / zen failure is never an account signal.
function orLimitOf(model, status, body) {
  if (!String(model || '').startsWith('openrouter/')) return null;
  return orAccountLimit(status, body);
}

// Почему ключ отказал — это решает, КОМУ отказ указ (#341). `scope: 'account'` — свойство самого
// аккаунта (мёртвый ключ, нет оплаты): сажаем в общий exhausted, читают все. `scope: 'rate_limit'`
// — временный лимит: сюда можно не доверять для другого тира, бесплатный не расходует платное
// allowance (#69) и его этот отказ не касается.
export function keyFaultOf(errorText) {
  const t = classSignal(errorText);
  if (DEAD_KEY_RE.test(t)) return { dead: true, scope: 'account', ttlMs: KEY_DEAD_TTL_MS };
  if (KEY_QUOTA_RE.test(t)) return { dead: false, scope: 'rate_limit', ttlMs: WEEKLY_LIMIT_RE.test(t) ? KEY_WEEKLY_TTL_MS : KEY_QUOTA_TTL_MS };
  // No payment = account has no active tariff. Instant error, instant rotation to the next key.
  // Use a short TTL (same as rate-limit) so the account is retried once payment is added.
  // Тариф — свойство АККАУНТА, поэтому scope 'account', несмотря на короткий TTL.
  if (NO_PAYMENT_RE.test(t)) return { dead: false, scope: 'account', ttlMs: KEY_QUOTA_TTL_MS };
  return null;
}

function failureClass(errorText) {
  const v = errorClass(errorText);
  if (v && v.class === 'quota') return { cls: 'quota', retryAfterMs: v.ttlMs };
  if (v && v.class === 'config') return { cls: 'config' };
  // The request did not fit this rung's window. classify.js is explicit that this must NOT become
  // a shared exhaustion — «the next task on this rung (from any user) is very likely a normal-sized
  // prompt that would work fine» — but collapsing it to 'transient' did exactly that: every fat
  // prompt put the rung into shared health for 2 s → 4 s → … (capped at 30 s for zen), and one
  // 1.1 MB agent prompt was disabling the head rung for EVERY other caller ~24×/hour. The request
  // is wrong, the rung is fine.
  if (v && v.class === 'context') return { cls: 'context' };
  return { cls: 'transient' };
}

// Progress telemetry is deliberately fire-and-forget: a slow or unavailable event sink must not
// consume the request's provider timeout budget or alter rung selection.
function reportProgress(onProgress, event) {
  if (typeof onProgress !== 'function') return;
  try {
    const result = onProgress(event);
    if (result && typeof result.catch === 'function') result.catch(() => {});
  } catch { /* progress is observational only */ }
}

// Streaming: a rung is chosen BEFORE the first token — if it doesn't produce one within this
// window (or errors), the next rung is tried; after the first token there is no failover.
export const TTFB_TIMEOUT_MS = 15000;

// Сколько лестница готова ждать КОЛЬЦО, если вызывающий не попросил больше. Пол, а не замена:
// `Math.max(timeoutMs, RING_WAIT_MS)` в attemptRing. Выведено из замера на здоровом окне
// (2026-10-07 09:00 →, 1599 успешных задач): сервис zen p50 = 33 с, p90 = 63 с, при том что
// бюджет был 20 с + 15 с grace = 35 с, то есть САМА МОДЕЛЬ не влезала в свой бюджет.
//
//   бюджет 35 с от постановки → доставлено 41.4 %  (было)
//   бюджет 60 с               → доставлено 74.8 %
//   бюджет 90 с               → доставлено 92.4 %
//
// 45 с + grace 15 с = 60 с — это 74.8 % при максимально допустимом для интерактива ожидании.
// Дальше выгода падает (60→90 с даёт +18 п.п., а ожидание полторы минуты), а «опенкод зависает»
// было главной жалобой.
export const RING_WAIT_MS = 45_000;

// Правило — отдельно и явно, чтобы его можно было проверить без 45-секундного ожидания:
// пол только поднимает, никогда не опускает. Вызывающий, попросивший больше, больше и получит;
// попросивший меньше всё равно не получит меньше того, что модели нужно, чтобы ответить.
export function ringWaitMs(timeoutMs) {
  return Math.max(Number(timeoutMs) || 0, RING_WAIT_MS);
}
const RF_400_RE = /structured[-_ ]outputs?|response[_ ]?format|json_object|stream_options/i;

// Поля, которые шлюз opencode-go ПРИНИМАЕТ (#341, п. 4).
//
// Список собран замером против живого шлюза синтетическими пин-вызовами (одна ступень, без
// failover). Проверены и прошли (200 OK): messages, tools с обычной схемой, tool_choice
// (auto/required), response_format (json_object И json_schema strict), temperature, top_p, seed,
// stop, user, metadata, service_tier, presence_penalty, frequency_penalty, stream, max_tokens.
// Проверены и дали HTTP 400: top_k, min_p, repetition_penalty, n, logit_bias, custom_id, store,
// parallel_tool_calls, prompt_cache. На OpenRouter ВСЕ они проходят — строгость свойство
// именно этого гейта, поэтому фильтр применяется только к opencode-go.
//
// Симптом в проде: доля 400 среди вызовов beam-free — 57 % у product-owner против 2 % у
// анонимного сканера (его payload минимален) и 0 % на лестницах free/plan/general. Первый ранг
// упирался в 400 и жёг хоп 1–12 с перед переходом на dots-3.
//
// Известный остаток, НЕ закрытый здесь: tools со strict:true + $defs в схеме тоже даёт 400.
// Санитайзация схемы меняет семантику structured outputs у клиента, поэтому решение принимается
// отдельно и описано в #341.
const GO_ALLOWED_FIELDS = new Set([
  'messages', 'tools', 'tool_choice', 'response_format',
  'temperature', 'top_p', 'presence_penalty', 'frequency_penalty',
  'stop', 'seed', 'user', 'metadata', 'service_tier',
]);

// Поля, которые лестница выставляет сама или удаляет. Их клиентское значение подменяется, а не
// теряется, поэтому в лог «отброшено» они не идут — иначе каждая нормальная заявка выглядела бы
// как потеря.
const LADDER_MANAGED_FIELDS = new Set(['model', 'stream', 'max_tokens', 'stream_options', 'ladder_cache_variant']);

// Только известные поля — и ничего сверх. model/stream/max_tokens выставляет сама лестница ниже.
//
// Отсечение обязано быть НАБЛЮДАЕМЫМ: список собран замером, но шлюз может принимать и то, чего мы
// не пробовали, и тогда клиент молча лишится работающего поля. Имена полей — не секреты, поэтому
// они идут в лог, а значения не логируются вовсе. Увидели событие go-field-whitelist с именем —
// добавили его в список и переверили.

// STRICT-РЕЖИМ. Замерено против живого шлюза opencode-go пин-вызовами 2026-10-29:
//   без strict                                  → 200
//   strict:true БЕЗ additionalProperties:false  → 400
//   strict:true + AP:false, все свойства в req  → 200
//   strict:true + AP:false, но схема неполная   → 400
// То есть шлюз применяет правила strict-режима OpenAI (additionalProperties:false на каждом объекте
// + каждое свойство в required), и отказ — это НЕ его причуда, а невалидный запрос клиента.
//
// Что делаем: снимаем ТОЛЬКО флаг strict и повторяем ту же ступень один раз. Схему не трогаем.
// Молча дописывать в схему клиента additionalProperties:false и|required было бы изменением его
// контракта, а флаг снять безопасно: без него та же схема работает как обычная, и клиент получает
// ответ вместо мёртвого хопа. Повтор срабатывает только тогда, когда запрос всё равно падал, поэтому
// валидная strict-схема не замечает ничего.
export function hasStrictTools(body) {
  const tools = body?.tools;
  return Array.isArray(tools) && tools.some((t) => t?.function?.strict === true);
}

function withoutStrictFlag(body) {
  if (!hasStrictTools(body)) return body;
  return {
    ...body,
    tools: body.tools.map((t) => {
      if (t?.function?.strict !== true) return t;
      const fn = { ...t.function };
      delete fn.strict;
      return { ...t, function: fn };
    }),
  };
}
function pickGoFields(body) {
  const out = {};
  const dropped = [];
  for (const field of Object.keys(body || {})) {
    if (GO_ALLOWED_FIELDS.has(field)) out[field] = body[field];
    else if (!LADDER_MANAGED_FIELDS.has(field)) dropped.push(field);
  }
  if (dropped.length) console.log(JSON.stringify({ route: 'go-field-whitelist', dropped }));
  return out;
}

export function upstreamRequest(env, model, body, keyIndex, { stream = false, stripRf = false, stripStrict = false, conversation = null, appSlug = null, appTitle = null, pool = null, orIndex = null } = {}) {
  const isGo = model.startsWith('opencode-go/');
  const isZen = model.startsWith('opencode-zen/');
  // The pool is resolved ONCE per call (env secrets + admin-managed D1 credentials) and threaded
  // through every attempt, so all attempts of one call see the same credentials.
  const resolvedPool = pool || { go: readPool(env), openrouter: env.OPENROUTER_API_KEY ? [env.OPENROUTER_API_KEY] : [] };
  // Zen (#36): single shared relay token — no key pool, no rotation. Missing token → null →
  // the rung is treated as keyless and skipped, so an unconfigured worker just skips zen.
  const key = isGo ? resolvedPool.go[keyIndex]
    : isZen ? env.OPENCODE_ZEN_RELAY_TOKEN
    : resolvedPool.openrouter[openrouterKeyIndex(resolvedPool.openrouter.length, orIndex ?? keyIndex)];
  if (!key) return null;
  const headers = { 'Authorization': `Bearer ${key}`, 'Content-Type': 'application/json' };
  // Keyed calls carry a stable per-conversation id so the provider-side prompt cache survives
  // turn after turn (epic #17, S7). Unkeyed calls keep the random session (Go 400s without it).
  // The zen relay turns x-session-id into its ses_/msg_ fingerprint (deterministic per conv).
  if (isGo) headers['x-opencode-session'] = `ladder-${conversation || crypto.randomUUID()}`;
  else if (conversation) headers['x-session-id'] = conversation;
  // App attribution (#33): only OpenRouter understands these — Go and zen rungs must not get them.
  if (!isGo && !isZen) {
    headers['HTTP-Referer'] = `${APP_REFERER_BASE}/${sanitizeAppSlug(appSlug)}`;
    headers['X-OpenRouter-Title'] = sanitizeAppTitle(appTitle);
    headers['X-OpenRouter-App-Visibility'] = 'hidden';
  }
  // Повтор после HTTP 400 от strict-схемы (#341): тот же запрос, но без флага strict.
  const source = stripStrict ? withoutStrictFlag(body) : body;
  const upstream = {
    // Шлюз opencode-go принимает только известные ему поля и отвечает HTTP 400 на остальное,
    // а раньше сюда forwardingался клиентский body целиком (#341, п. 4). Список собран замером
    // синтетическими пин-вызовами: OpenRouter на том же теле отвечает 200, то есть строгость —
    // свойство именно этого гейта. Из-за лишнего поля ~54 % вызовов beam-free упирались в 400
    // на ПЕРВОЙ ступени и жгли хоп (1–12 с) перед уходом на dots-3.
    ...(isGo ? pickGoFields(source) : source),
    model: model.replace(/^opencode-go\/|^opencode-zen\/|^openrouter\//, ''),
    stream,
    max_tokens: Math.max(Number(body.max_tokens) || 0, minTokensFor(model)),
  };
  delete upstream.ladder_cache_variant;
  delete upstream.stream_options;
  if (stream && !isGo && !isZen) upstream.stream_options = { include_usage: true };
  if (stripRf) { delete upstream.response_format; delete upstream.stream_options; }
  const base = isGo
    ? (env.OPENCODE_GO_BASE_URL || 'https://opencode.ai/zen/go/v1')
    : isZen
      ? (env.OPENCODE_ZEN_BASE_URL || 'https://136-65-7-197.sslip.io/zen')
      : (env.OPENROUTER_BASE_URL || 'https://openrouter.ai/api/v1');
  return { url: `${base}/chat/completions`, headers, body: upstream };
}

async function post(fetchImpl, req, signal) {
  try {
    return { res: await fetchImpl(req.url, { method: 'POST', headers: req.headers, body: JSON.stringify(req.body), signal }) };
  } catch (e) {
    return { error: `fetch failed: ${e.message}` };
  }
}

// #34: guard failures say WHY the answer is empty — finish_reason + usage (did reasoning eat the
// whole floor?) travel inside the error string itself → attempts → console.log and D1
// ladder_calls.attempts, no new columns. Missing parts are simply omitted.
function guardDiag(data, req) {
  const parts = [];
  // flat shape too: a pool/ring answer carries finish_reason at the top level, not under
  // choices[0], so the same sentence explains an empty body from either transport.
  const finish = data?.choices?.[0]?.finish_reason ?? data?.finish_reason;
  if (finish) parts.push(`finish=${finish}`);
  const usage = data?.usage || {};
  if (usage.completion_tokens != null) parts.push(`out=${usage.completion_tokens}`);
  const reasoning = usage.completion_tokens_details?.reasoning_tokens;
  if (reasoning != null) parts.push(`reasoning=${reasoning}`);
  if (usage.prompt_tokens != null) parts.push(`prompt=${usage.prompt_tokens}`);
  if (req?.body?.max_tokens != null) parts.push(`max_tokens=${req.body.max_tokens}`);
  return parts.length ? ` (${parts.join(', ')})` : '';
}

// #45 (повтор для кольца): ретрай имеет смысл, только если вторая попытка МОЖЕТ отличаться от
// первой. Ответ с `finish=tool_calls` и без ни текста, ни самого tool_calls — структурный отказ:
// модель захотела вызвать инструмент, а кольцевой воркер (старая версия, без фикса от 07.10)
// записал задачу неудачной, не приложив tool_calls. Повтор по тому же запросу даёт тот же
// результат — замер за сутки: 74 таких отказа, дошёл до ответа 8 (и то другой ступенью),
// суммарно 35 минут стенда. Лестница уходит на следующую ступень сразу.
//
// Возвращается в отдельной функции, чтобы правило жило рядом с guardDiag и проверялось само.
export function ringGuardRetry(data, body) {
  if (body?.stream) return false;                       // стрим уже оплатил всё окно
  if (!(data?.kind === 'ok' && !data?.ok)) return false; // это вообще не guard-случай
  const finish = data?.choices?.[0]?.finish_reason ?? data?.finish_reason;
  return finish !== 'tool_calls';
}

// Non-streaming attempt. A tool-call answer with no text is a valid answer.
async function attemptJson(env, model, body, keyIndex, { timeoutMs, wantJson, fetchImpl, conversation = null, appSlug = null, appTitle = null, pool = null, orIndex = null }) {
  let stripRf = false;
  let stripStrict = false;
  for (;;) {
    const req = upstreamRequest(env, model, body, keyIndex, { stripRf, stripStrict, conversation, appSlug, appTitle, pool, orIndex });
    if (!req) return { ok: false, skip: true, error: 'no key' };
    const { res, error } = await post(fetchImpl, req, AbortSignal.timeout(timeoutMs));
    if (error) return { ok: false, error };
    if (!res.ok) {
      const errText = String(await res.text().catch(() => ''));
      // strict-схема, не выполняющая правила strict-режима, отвергается шлюзом opencode-go (#341):
      // повторяем ту же ступень один раз без флага strict, схему не трогая. Проверка идёт ДО
      // response_format, потому что причина отказа в этом случае детерминирована и не зависит от
      // текста тела (замер 2026-10-29: 400 воспроизводится 4 из 4 прогонов).
      if (res.status === 400 && !stripStrict && model.startsWith('opencode-go/') && hasStrictTools(body)) { stripStrict = true; continue; }
      // Provider rejects response_format → retry the SAME rung once without it (prompt-only JSON).
      if (res.status === 400 && !stripRf && body.response_format && RF_400_RE.test(errText)) { stripRf = true; continue; }
      // The account-level OpenRouter limit is read from the FULL body (#297): `limit_source` and the
      // reset stamp sit at the end of it, and the error string below is cut to 300 characters.
      return { ok: false, error: `HTTP ${res.status}: ${errText.slice(0, 300)}`, limit: orLimitOf(model, res.status, errText) };
    }
    const data = await res.json().catch(() => null);
    const provider = model.slice(0, model.indexOf('/'));
    const actualModel = typeof data?.model === 'string' && data.model
      ? (data.model.startsWith(`${provider}/`) ? data.model : `${provider}/${data.model}`) : model;
    const message = data?.choices?.[0]?.message || {};
    const content = String(message.content || '').trim();
    const hasTools = Array.isArray(message.tool_calls) && message.tool_calls.length > 0;
    // #45 (owner): a COMPLETED answer that fails the guard is flagged, not fatal — run() gives
    // the rung ONE same-rung retry before moving down (guardTried there bounds it).
    if (isEmptyAnswer(content) && !hasTools) return { ok: false, guard: true, error: `empty answer${guardDiag(data, req)}` };
    if (wantJson && !hasTools && parseJson(content) === undefined) return { ok: false, guard: true, error: `invalid JSON${guardDiag(data, req)}` };
    return { ok: true, data, content, winner: { model: actualModel, keyIndex } };
  }
}

// True for an SSE event that carries real output (text, reasoning or a tool call) — not a bare
// role/keep-alive frame that a stalled provider can emit before failing.
function isOutputEvent(line) {
  if (!line.startsWith('data:')) return false;
  const payload = line.slice(5).trim();
  if (!payload || payload === '[DONE]') return false;
  try {
    const d = JSON.parse(payload);
    if (d.error) return false;
    const delta = d.choices?.[0]?.delta || {};
    // An empty/whitespace-only or reasoning-only frame is not real output: committing on
    // reasoning alone lets a rung that never produces content pass as a successful stream.
    const hasContent = typeof delta.content === 'string' && delta.content.trim().length > 0;
    const hasTools = Array.isArray(delta.tool_calls) && delta.tool_calls.length > 0;
    return !!(hasContent || hasTools);
  } catch { return false; }
}

// Streaming attempt: resolves once the first output event arrived (→ committed stream that
// replays the buffered bytes and pipes the rest), or fails before it (→ caller tries next rung).
async function attemptStream(env, model, body, keyIndex, { ttfbMs, fetchImpl, conversation = null, appSlug = null, appTitle = null, pool = null, orIndex = null }) {
  const req = upstreamRequest(env, model, body, keyIndex, { stream: true, stripRf: !!body._stripRf, conversation, appSlug, appTitle, pool, orIndex });
  if (!req) return { ok: false, skip: true, error: 'no key' };
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(new Error('no first token in time')), ttfbMs);
  const { res, error } = await post(fetchImpl, req, ctrl.signal);
  if (error) { clearTimeout(timer); return { ok: false, error }; }
  if (!res.ok) {
    clearTimeout(timer);
    const errText = String(await res.text().catch(() => ''));
    if (res.status === 400 && !body._stripRf && body.response_format && RF_400_RE.test(errText)) {
      return attemptStream(env, model, { ...body, _stripRf: true }, keyIndex, { ttfbMs, fetchImpl, conversation, appSlug, appTitle, pool, orIndex });
    }
    return { ok: false, error: `HTTP ${res.status}: ${errText.slice(0, 300)}`, limit: orLimitOf(model, res.status, errText) };
  }
  const reader = res.body.getReader();
  const dec = new TextDecoder();
  const buffered = [];
  let text = '';
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) { clearTimeout(timer); return { ok: false, error: 'stream ended before first token' }; }
      buffered.push(value);
      text += dec.decode(value, { stream: true });
      const lines = text.split('\n');
      text = lines.pop();
      const errLine = lines.find(l => l.startsWith('data:') && /"error"/.test(l));
      if (errLine && !lines.some(isOutputEvent)) { clearTimeout(timer); reader.cancel().catch(() => {}); return { ok: false, error: `stream error: ${errLine.slice(5, 300)}` }; }
      if (lines.some(isOutputEvent)) break;
    }
  } catch (e) {
    clearTimeout(timer);
    return { ok: false, error: `stream failed before first token: ${e.message}` };
  }
  clearTimeout(timer);
  const stream = new ReadableStream({
    start(controller) { for (const chunk of buffered) controller.enqueue(chunk); },
    async pull(controller) {
      try {
        const { value, done } = await reader.read();
        if (done) controller.close(); else controller.enqueue(value);
      } catch (e) { controller.error(e); }
    },
    cancel(reason) { return reader.cancel(reason); },
  });
  return { ok: true, stream };
}

function attempt(env, model, body, keyIndex, opts) {
  // Zen Ring rung — in-process, no HTTP and no token: the pool lives in this same worker, so the
  // ladder calls its core directly. A cold ring boots a runner (~10-13 s) and the caller's own
  // watchdog covers it; a warm one answers in ~3 s.
  if (model.startsWith('zen-rings/')) return attemptRing(env, model, body, opts);
  return body.stream ? attemptStream(env, model, body, keyIndex, opts) : attemptJson(env, model, body, keyIndex, opts);
}

// The owner's race (2026-10-07): «два запуска одной модели на разных аккаунтах го» — the SAME
// model fired on DIFFERENT keys at once, first answer wins. Free Go has four keys, no allowance
// to protect and no per-token cost, so the race buys tail latency and immunity to one throttled
// or slow key for the price of wall clock alone.
//
// The primary key (the one the round-robin picked) is the one whose failure the caller sees, so
// key rotation and the spare-key probe below behave exactly as they do for a single attempt —
// a race that silently returned some other key's error would make #45/#69 reason about the wrong
// account. Every raced key is recorded in `tried` so the probe cannot land on one already burnt.
async function raceGoKeys(model, primaryKey, count, poolSize, tried, attemptKey) {
  const n = Math.max(2, Math.min(count, poolSize));
  const keys = [...new Set(Array.from({ length: n }, (_, i) => (primaryKey + i) % poolSize))];
  for (const k of keys) tried.add(k);
  if (keys.length < 2) return attemptKey(primaryKey);
  return new Promise((resolve) => {
    let pending = keys.length;
    let primaryResult = null;
    let done = false;
    const settle = (r) => { if (!done) { done = true; resolve(r); } };
    const lose = (r, isPrimary) => {
      if (isPrimary && !primaryResult) primaryResult = r;
      if (--pending === 0) settle(primaryResult || r);
    };
    for (const k of keys) {
      const isPrimary = k === primaryKey;
      attemptKey(k).then(
        (r) => (r.ok ? settle({ ...r, winner: { model: r.winner?.model || model, keyIndex: k } }) : lose(r, isPrimary)),
        (e) => lose({ ok: false, error: String(e?.message || e) }, isPrimary),
      );
    }
  });
}

// The pool's answer is one blob from a long-lived GitHub Actions job, so a streaming call gets a
// synthesised SSE stream — the ladder's TTFB path only needs the first output event, and the
// client sees a normal stream.
// One pool call: invoke, and if the caller's watchdog fired while the job is still working, wait a
// short grace for the answer that is already in flight rather than starting a duplicate task. The
// grace is bounded — the task already had the caller's whole rung budget, so a longer wait would
// only delay the failover.
export async function ringCall(env, payload, fetchImpl, graceMs = 15_000) {
  let r = await ringInvoke(env, payload, fetchImpl);
  if (r.status === 504 && r.data?.task_id) {
    const taskIds = r.data.task_ids || [r.data.task_id];
    const w = await ringWaitForTask(env, taskIds, { deadlineMs: graceMs });
    if (w.ok) return { status: 200, data: w.data };
    // The caller is about to fail over. Release any task a worker has not taken yet; claimed
    // work may finish, but it cannot be cancelled safely. Never submit this timed-out call again.
    let cleanup = { abandoned: 0, still_claimed: 0 };
    let taskRows = [];
    try {
      cleanup = await abandonQueuedTasks(env, taskIds, 'ladder caller failed over after watchdog');
      taskRows = await Promise.all(taskIds.map((id) => env.ZEN_DB.prepare(
        'SELECT id, state FROM zen_pool_tasks WHERE id = ?1'
      ).bind(id).first()));
    } catch (error) {
      // Cleanup failure must not turn a provider timeout into a thrown handler error. The
      // initial wait_returned_at marker and stale-task reaper remain as recovery paths.
      console.error('zen pool watchdog cleanup failed:', error?.message || error);
    }
    const completedTasks = taskRows.some((row) => row && ['done', 'failed'].includes(row.state));
    r = { ...r, data: {
      ...r.data,
      abandoned_tasks: cleanup.abandoned,
      claimed_tasks: cleanup.still_claimed,
      hint: cleanup.still_claimed
        ? 'queued work was abandoned; an already-claimed provider request may still finish; task state is available from GET /zen/pool/result/{task_id}'
        : cleanup.abandoned
          ? 'queued work was abandoned after the watchdog; no worker will spend quota on it'
          : completedTasks
            ? 'the task finished during the grace period; its result is recorded and will not be submitted again'
            : 'no queued work remains after the watchdog; task state is available from GET /zen/pool/result/{task_id}',
      task_states: taskRows.filter(Boolean).map((row) => ({ task_id: row.id, state: row.state })),
    } };
  }
  return r;
}

async function attemptRing(env, model, body, { fetchImpl, timeoutMs }) {
  const rung = model.replace(/^zen-rings\//, '');
  // Right after a cold start the runner is still settling — skip the ring for a cooldown window
  // so a call doesn't land on a half-warmed worker.
  const cooldown = await ringCooldown(env);
  if (cooldown > 0) {
    return { ok: false, error: `zen-rings: warming up (${Math.ceil(cooldown / 1000)}s left), try the next rung` };
  }
  // Cold ring: boot a worker in the background and fail over NOW. Waiting here would burn the
  // caller's whole rung budget on a one-time ~10-13 s boot; the ring is warm for the next call.
  const boot = await ringBoot(env, { fetchImpl });
  if (boot.booted) {
    return { ok: false, error: `zen-rings: cold ring — booting (${boot.reason}), try the next rung` };
  }
  const payload = {
    model: rung,
    messages: body.messages,
    tools: body.tools,
    max_tokens: body.max_tokens,
    response_format: body.response_format,
    tool_choice: body.tool_choice,
    // The caller's per-rung budget IS the pool watchdog (clamped to the pool's [1s, 90s]) — но для
    // ЗЕНА этого бюджета не хватало. Замер на здоровом окне (с 2026-10-07 09:00, 1599 задач):
    // очередь p50 = 0 с, сервис zen p50 = 33 с, p90 = 63 с, а старые 20 с + 15 с grace доставляли
    // лишь 41 % ответов — бюджет был меньше самой модели. Окно в 45 с (+15 с grace = 60 с)
    // доставляет 74.8 % (замер: total ≤60 с → 1196/1599).
    //
    // Пол только ПОДНИМАЕТ: вызывающий, попросивший больше (`ladder_timeout_ms`), больше и получит.
    // Холодный или мёртвый кольцо сюда не попадает — эти ветки возвращаются до постановки задачи,
    // поэтому 45 с не превращают отказ в ожидание.
    wait_ms: ringWaitMs(timeoutMs),
  };
  let r = await ringCall(env, payload, fetchImpl);
  // ONE retry for a transient fault (a cold ring that just booted, a provider 5xx). A budget
  // refusal (429) is not retried — the cap is real and retrying inside the same minute is wasted.
  // 413 (input too long for the free tier) is decided locally before anything is queued: the same
  // payload cannot fit on a second attempt either, so there is nothing to retry — unlike a transient
  // fault or a cold ring, where a second look genuinely can succeed.
  if (shouldRetryRingCall(r)) r = await ringCall(env, payload, fetchImpl);
  if (r.status !== 200 || !r.data?.ok) {
    // No status code in the message on purpose: '429'/'503' would classify as a quota skip (up to
    // 1h), and a pool that is merely cold or briefly over its per-minute cap is transient.
    const base = r.data?.error || r.data?.kind || 'no answer';
    // #34 for the ring path: the pool keeps finish_reason/usage on a failure too, so an empty
    // body can say WHY — finish=length, out=0, reasoning=N instead of an opaque `zen-rings: ok`.
    const diag = guardDiag(r.data, { body });
    // #45 — ONE same-rung retry, but only for the case it was built for: the model answered and
    // we judged that answer empty. A budget refusal or a cooldown must not be retried (the cap
    // is real), and a stream caller already paid the whole watchdog for a synthesised stream, so
    // paying it twice is exactly what the guard-retry comment excludes.
    const emptyOk = r.data?.kind === 'ok' && !r.data?.ok;
    return {
      ok: false,
      ...(ringGuardRetry(r.data, body) ? { guard: true } : {}),
      error: `zen-rings: ${base}${diag}`,
    };
  }
  const content = String(r.data.text || '').trim();
  const hasTools = Array.isArray(r.data.tool_calls) && r.data.tool_calls.length > 0;
  if (isEmptyAnswer(content) && !hasTools) return { ok: false, guard: true, error: `empty answer${guardDiag(r.data, { body })}` };
  const actualModel = `zen-rings/${r.data.model || rung}`;
  const message = { role: 'assistant', content };
  if (hasTools) message.tool_calls = r.data.tool_calls;
  const finish = r.data.finish_reason || (hasTools ? 'tool_calls' : 'stop');
  const completion = {
    id: `zen-rings-${r.data.task_id || crypto.randomUUID()}`,
    object: 'chat.completion',
    created: Math.floor(Date.now() / 1000),
    model: actualModel,
    choices: [{ index: 0, message, finish_reason: finish }],
    ...(r.data.usage ? { usage: r.data.usage } : {}),
  };
  if (!body.stream) return { ok: true, data: completion, content, winner: { model: actualModel, keyIndex: 0 } };
  const enc = new TextEncoder();
  const head = { ...completion, choices: [{ index: 0, delta: message, finish_reason: null }] };
  const tail = { choices: [{ index: 0, delta: {}, finish_reason: finish }] };
  const sse = `data: ${JSON.stringify(head)}\n\ndata: ${JSON.stringify(tail)}\n\ndata: [DONE]\n\n`;
  const stream = new ReadableStream({ start(c) { c.enqueue(enc.encode(sse)); c.close(); } });
  return { ok: true, stream };
}

// A 504 carrying task_id means the work has already been enqueued and charged. Polling that id
// is the only safe continuation; a second invoke would charge and serve a duplicate request.
export function shouldRetryRingCall(result) {
  return result.status !== 200 && !result.data?.task_id && result.status !== 429 && result.status !== 413;
}

/**
 * @param {object} body   OpenAI chat.completions body; `model` = ladder name ("service",
 *                        legacy alias "deepseek", "service:review", alias "free-ladder"). stream:true → SSE (rung chosen
 *                        before the first token); tools / tool_choice passed through as is.
 * @param {object} ctx    { env, config, store, fetchImpl, timeoutMs=20000, totalTimeoutMs, now,
 *                          pinRung, conversation, appSlug, appTitle }
 *   appSlug / appTitle: OpenRouter app attribution (#33) — slug for HTTP-Referer (default
 *                 'llm-ladder'), display name for X-OpenRouter-Title (default 'Trained Assist').
 *   conversation: stable per-conversation id (Kh from the route) — enables the sticky rung
 *                 (epic #17): the conversation sticks to ONE rung so its provider-side prompt
 *                 cache survives turn after turn. null/undefined → byte-for-byte today.
 *   store (async): snapshot(poolSize?, pinKey?) → { health, keys, pin }
 *                  recordFailure(model, {cls, retryAfterMs}, {pinRemove}),
 *                  recordSuccess(model, {pin}), rotateKey(poolSize, ttlMs, failedIndex), park(models, untilMs),
 *                  pinStats() → {count, byRung}
 * @returns {Promise<{ok:true, model, data?, content?, stream?, attempts, pin?} |
 *                   {ok:false, status, error, attempts, pin?}>}
 *   pin: 'new' | 'hit' | 'moved' | 'gone' | null — only set for keyed calls.
 */
export async function run(body, { env, config, store, fetchImpl = fetch, timeoutMs = 20000, totalTimeoutMs = null, ttfbMs = TTFB_TIMEOUT_MS, pinRung = null, conversation = null, appSlug = null, appTitle = null, onProgress } = {}) {
  // The ladder's own size estimate, computed once. It is returned on EVERY outcome —
  // successful or not — so the trace can answer "how big were the requests that failed?"
  // without needing the provider's usage counter, which only exists on a 200.
  //
  // INPUT only (#341): the column is named estimated_tokens_in and must answer how big the
  // request we SENT was. Feeding it the caller's output budget made an 11-token prompt with
  // max_tokens=32768 read as 36 057 — the same number then picked the hedge band, so a trivial
  // call raced three identical copies at three keys. The context gate keeps its own
  // input+output estimate below, which is the only place max_tokens belongs.
  const estimatedTokensIn = estimateInputTokens(body);
  let all = rungsFor(config, body && body.model);
  if (!all) return { ok: false, status: 404, error: `unknown ladder: ${body && body.model}`, attempts: [], estimated_tokens_in: estimatedTokensIn };
  // Benchmarks: pin ONE rung of the ladder (health skips ignored, no failover) to measure it
  // through the worker without handing provider keys to the bench.
  if (pinRung) {
    if (!all.includes(pinRung)) return { ok: false, status: 400, error: `rung not in ladder: ${pinRung}`, attempts: [], estimated_tokens_in: estimatedTokensIn };
    all = [pinRung];
  } else if (config?.zen_ring === false) {
    // The zen ring is switched OFF (config: "zen_ring": false). Its runners are not registered,
    // so every ring rung answers `provider_capacity` — the ladder would pay a hop per rung before
    // it moves on. The rungs STAY in the config: when the runners are back, turning the ring on is
    // one flag and the order of the ladders is exactly what it was.
    all = all.filter((m) => !m.startsWith('zen-rings/'));
    if (!all.length) return { ok: false, status: 503, error: 'no rung left: the zen ring is switched off and the ladder has no other rung', attempts: [], estimated_tokens_in: estimatedTokensIn };
  }
  if (!pinRung) {
    // The checks' own verdict on dead models (#322): skip rungs their tables confirm dead (free
    // gone from the provider catalog for a day, zen down and unseen healthy for three), so a
    // retired slug stops costing a hop as soon as a check sees it — no config edit, no PR
    // (#319 was rejected for exactly that). Fail-open three ways: no D1 → no filter, the query
    // failing → no filter, the filter emptying the ladder → no filter (a ladder that must try
    // everything anyway). A pin is an explicit request and is never filtered.
    try {
      const dead = await deadRungs(env, Date.now());
      const kept = all.filter((m) => !dead.has(m));
      if (kept.length) all = kept;
    } catch { /* inventory outage must not interrupt ladder calls */ }
  }
  if (!pinRung && env?.LADDER_TRACE_DB) {
    // The Go block and the OpenRouter free block are ordered by the measured answer time; the
    // block keeps its place in the ladder (free rungs first, paid ones last).
    try { all = orderBenchmarkedRungs(all, await readBenchRows(env.LADDER_TRACE_DB), estimateTokens(body)); }
    catch { /* benchmark inventory outages must not interrupt ladder calls */ }
  }
  // Provider pool: env secrets (source of truth) + admin-managed credentials from D1. The env
  // pool stays the fallback — a missing PROVIDER_CREDS_KEY or a D1 outage must not change which
  // credentials the ladder uses today.
  let providerPool = { go: [], openrouter: [] };
  if (env?.LADDER_TRACE_DB && env.PROVIDER_CREDS_KEY) {
    try {
      providerPool = await resolveProviderPool(env, env.LADDER_TRACE_DB);
    } catch { /* pool resolution failure must not interrupt ladder calls */ }
  }
  if (!providerPool.go.length) providerPool.go = readPool(env);
  if (!providerPool.openrouter.length && env.OPENROUTER_API_KEY) providerPool.openrouter = [env.OPENROUTER_API_KEY];
  const pool = providerPool.go;
  const hasKey = m => (m.startsWith('opencode-go/') ? pool.length > 0
    : m.startsWith('opencode-zen/') ? !!env.OPENCODE_ZEN_RELAY_TOKEN
    : m.startsWith('zen-rings/') ? !!env.ZEN_DB
    : providerPool.openrouter.length > 0);
  const keyed = all.filter(hasKey);
  if (!keyed.length) return { ok: false, status: 503, error: 'no provider key configured', attempts: [], estimated_tokens_in: estimatedTokensIn };

  // OpenRouter context windows — the catalog's `context` column (256K / 512K / 1M per
  // model, or null when the provider has not published one). Loaded once per call so the
  // pre-filter below can refuse a rung whose window is smaller than the request BEFORE the
  // attempt, instead of burning a key on a 400 the provider wraps in silence.
  const orContextCatalog = await readOrContextCatalog(env);
  const snap = await store.snapshot(undefined, conversation);
  const now = Date.now();
  // OpenRouter ACCOUNT parks (issue #297): the free allowance is counted per account, so an account
  // that ran out is remembered until its own reset and stepped over — the OTHER accounts of the pool
  // keep serving the same rung. The map is local to this call on purpose: an account parked DURING
  // this call must not be picked again by the next rung of the same call.
  const orParked = { ...((snap.or && snap.or.parked) || {}) };
  // A TRANSIENT wobble on a Go rung (timeout / empty answer / 500 — the failure class behind the
  // 29.09 incident) must not keep the whole fleet on the paid tail for the full exponential
  // backoff (up to 5 min): cap it, so the Go tier is re-tested at least every 30s. Real limits
  // (quota parks from key exhaustion, 503, config) keep their own TTL — hammering them helps no
  // one. Zen rungs (#36) get the same cap: they sit in the free segment, and a long skip there
  // pushes calls onto the rate-limited OpenRouter free pool and then the paid tail.
  const GO_TRANSIENT_SKIP_CAP_MS = 30 * 1000;
  const skipped = m => {
    const h = snap.health[m];
    if (!h) return false;
    if (h.skipUntil === null) return true;
    let until = h.skipUntil;
    if ((m.startsWith('opencode-go/') || m.startsWith('opencode-zen/')) && h.class === 'transient' && h.lastFailureAt) {
      until = Math.min(until, h.lastFailureAt + GO_TRANSIENT_SKIP_CAP_MS);
    }
    return until > now;
  };

  // ── Sticky rung (epic #17) ─────────────────────────────────────────────────────────────────
  // A valid pin = the conversation already served from a rung that is still part of THIS ladder
  // and has a key. The pinned rung then goes FIRST and is tried despite the global skipUntil
  // (another conversation's failure must not reset this one's prompt cache); other rungs keep
  // today's `!skipped` filter with the all-keyed fallback.
  const pinned = pinRung ? null : (snap.pin && all.includes(snap.pin.rung) && hasKey(snap.pin.rung) ? snap.pin : null);
  const goHealthy = keyed.some(m => m.startsWith('opencode-go/') && !skipped(m));
  // R6 — return from the paid tail: a pin parked on an OpenRouter rung must NOT keep this
  // conversation on the paid tier while any healthy Go rung is standing by. Skip the pin's
  // rung-first order for THIS call; if Go answers, the pin moves back to Go.
  const preferGoOverPin = pinned && pinned.rung.startsWith('openrouter/') && goHealthy;
  let pinState = conversation ? (pinned && !preferGoOverPin ? 'hit' : 'new') : null;
  // ONE same-rung retry budget for the pinned rung (S5): a transient 5xx/timeout/empty on the
  // rung we are caching on is retried once before the ladder moves — an "at most one switch" rule.
  let pinRetryLeft = pinned && !preferGoOverPin ? 1 : 0;

  const live = pinRung ? keyed : keyed.filter(m => !skipped(m));
  let rungs;
  if (pinned && !preferGoOverPin) {
    const rest = live.filter(m => m !== pinned.rung);
    rungs = [pinned.rung, ...(rest.length ? rest : keyed.filter(m => m !== pinned.rung))];
  } else {
    rungs = live.length ? live : keyed;
  }

  // ── Pre-Ladder Context Optimization (#331) ───────────────────────────────────────────
  // If the input exceeds the first free rung's context window, split the history into
  // chunks, ask that rung to summarize each chunk's relevance, and replace the prompt
  // with a compact version. This keeps the free tier from dying on oversized contexts
  // (measured: 21 of 28 `no first token in time` refusals were on payloads ≥ 100 KB).
  // Fail-open: if the optimizer throws or returns nothing, the original body is used.
  // Env override `LADDER_CONTEXT_OPTIMIZER_LIMIT` forces a specific limit (for staging/tests).
  const initialTokens = estimateTokens(body);
  const firstFreeRung = rungs.find(m => m.startsWith('opencode-go/') && m.endsWith('-free'));
  if (firstFreeRung) {
    const rungContext = contextOf(firstFreeRung, orContextCatalog);
    // Safety margin: keep 15% headroom for response tokens and tokenizer variance.
    const effectiveLimit = rungContext ? Math.floor(rungContext * 0.85) : Infinity;
    // Env override: if set, use it instead (e.g., LADDER_CONTEXT_OPTIMIZER_LIMIT=20000 for staging).
    const forcedLimit = env?.LADDER_CONTEXT_OPTIMIZER_LIMIT
      ? Number(env.LADDER_CONTEXT_OPTIMIZER_LIMIT)
      : null;
    const threshold = forcedLimit && forcedLimit > 0 ? forcedLimit : effectiveLimit;

    if (threshold !== Infinity && initialTokens > threshold) {
      try {
        const optimized = await optimizeContext(
          body, env, fetchImpl, firstFreeRung, attempt, threshold
        );
        if (optimized !== body) {
          body = optimized;
          reportProgress(onProgress, { type: 'context_optimized', rung: firstFreeRung,
            tokens_before: initialTokens, tokens_after: estimateTokens(optimized),
            threshold, rung_context: rungContext });
        }
      } catch (e) {
        // Optimization is a best-effort pre-step: a failure here must not poison the request.
        console.error('[context-optimizer] failed, proceeding with original:', e && e.message);
      }
    }
  }

  let keyIndex = Math.min(snap.keys.active || 0, Math.max(0, pool.length - 1));
  // Free Go rungs rotate across the pool (owner 2026-10-03: «го фрии модели шли каруселью всё
  // время по одному запросу по очереди перебирая аккаунты»). When the ladder OPENS on a free rung
  // (build, free), the starting key comes from the round-robin cursor, so consecutive free calls
  // land on different accounts instead of all hammering the active one. Single-key pool → no-op.
  const head = rungs[0];
  if (pool.length > 1 && head && head.startsWith('opencode-go/') && head.endsWith('-free')) {
    keyIndex = nextFreeGoKeyIndex(pool.length);
  }
  const exhaustedAtStart = new Set(Object.keys(snap.keys.exhausted || {}).map(Number));
  // Платный парк (#341). Живое множество, а не снимок: платный ранг сажает сюда ключи прямо
  // по ходу звонка, и следующий платный ранг того же звонка обязан их видеть. Бесплатный ранг
  // эту карту НЕ читает — лимит платного тира ему не указ, иначе платный хоп своим 429 лишает
  // фоллбэк обхода ключей (инцидент 2026-10-28: beam-free отвечал 61 разу за тот же час).
  const paidParked = new Set(Object.keys(snap.keys.paid || {}).map(Number));
  // A SLOW failure (timeout / empty answer / transport — anything whose error string is not an
  // HTTP status line) gets ONE spare-key probe per call (owner 2026-09-29: «ключ залимитился →
  // переключаем на другой, всё»): a silently throttled key looks exactly like a slow model, and
  // bounded to one probe a Go outage cannot double the failover latency.
  // A FAST rejection (error starts with `HTTP <code>` — the whole prefix classification needs,
  // owner 2026-10-27: «начала ошибки достаточно») costs milliseconds, so it gets its own budget
  // PER RUNG below: the rung walks every usable key before declaring itself failed. Incident
  // 2026-10-27 (#311): active sat on the one key that 403s everything while a healthy key idled —
  // the single per-call probe was spent on another dead key, recordFailure fired the exponential
  // backoff, and the whole Go floor went dark for everyone with a working key in the pool.
  // pinRung (benchmarks) measures exactly ONE attempt on ONE rung — no probe there either.
  let slowProbeBudget = pinRung ? 0 : 1;
  // #316 — lazily-fetched Go usage snapshot for PAID rungs: `undefined` = not asked yet,
  // a Set = keys whose monthly window is exhausted, `null` = unknown (behave as before).
  // Fetched at most once per call; goMonthlyCappedKeys memoizes across calls.
  let goCapped;
  let goCappedChecked = false;
  // #45: one same-rung retry per rung for a completed-but-guard-failed answer (empty / non-JSON).
  const guardTried = new Set();
  let goParked = false;
  // OpenRouter credential rotations per call — bounded to ONE so a dead paid tail cannot spin
  // through the whole pool before the ladder gives up.
  let orRotations = 0;

  const wantJson = body.response_format && body.response_format.type === 'json_object';
  const deadline = totalTimeoutMs ? now + totalTimeoutMs : Infinity;
  const attempts = [];
  let previousFailedRung = null;
  for (const model of rungs) {
    const isGo = model.startsWith('opencode-go/');
    // Fast-rejection probe budget for THIS rung: every usable (not parked, not yet tried) key
    // gets one attempt before the rung itself is declared failed (#311).
    let fastProbeBudget = isGo && !pinRung ? Math.max(0, pool.length - 1) : 0;
    // #69: a full-key park (weekly limit on every key) skips only the PAID Go rungs — a free
     // Go rung keeps serving, it does not consume the allowance and must not die in the incident.
     if (isGo && goParked && !model.endsWith('-free')) continue;
    const left = deadline - Date.now();
    if (left < 500) { attempts.push({ model, outcome: 'skipped', error: 'time budget spent' }); break; }
    if (previousFailedRung) {
      reportProgress(onProgress, { type: 'failover', rung: model, from: previousFailedRung,
        attempt: attempts.length + 1 });
    }
    reportProgress(onProgress, { type: 'rung_selected', rung: model, attempt: attempts.length + 1 });
    let key = keyIndex;
    // OpenRouter cursor for THIS rung: the credential pool rotates across calls so the paid tail
    // spreads over several keys instead of hammering one. Go keeps its own rotation state.
    const isOpenrouter = model.startsWith('openrouter/');
    // Account selection steps over the accounts parked by their own limit (#297); the round-robin
    // cursor only says where to start.
    const orIndex = isOpenrouter
      ? nextUsableOpenrouterIndex(providerPool.openrouter.length, nextOpenrouterKeyIndex(providerPool.openrouter.length), orParked, Date.now())
      : null;
    if (isOpenrouter && orIndex < 0) {
      // EVERY account of the pool is parked until its limit resets — skip the rung WITHOUT a
      // provider request. That is the point of remembering the exhaustion: the old code kept
      // asking the capped account (7 rungs × every call), which is exactly what burned the quota.
      attempts.push({ model, outcome: 'skipped', error: 'every OpenRouter account is parked until its limit resets' });
      continue;
    }
    // Две оценки, две работы (#341). `rungInputTokens` — что мы ОТПРАВЛЯЕМ: по нему выбирается
    // полоса гонки и окно первого токена. `rungWindowTokens` — сколько должно ВЛЕЗТЬ в окно
    // модели: вход плюс бюджет ответа. Потолок мерится вторым числом, и только туда попадает
    // max_tokens. Раньше обе ветки читали одно число с max_tokens внутри, и промпт на 11 токенов
    // с max_tokens=32768 гонялся на трёх ключах сразу (incident 2026-10-28, #341).
    const rungInputTokens = estimatedTokensIn;
    const rungWindowTokens = estimateTokens(body);
    // #38: a rung whose measured context window cannot fit the input is skipped BEFORE the
    // attempt — measured 2026-10-07: opencode-go takes ~98K tokens, and at ~123K answers
    // `429 Endpoint is unavailable` while rotating through every key — 48 s and three keys for
    // no answer. Refusing before the attempt is the whole point, and refusing must NOT poison
    // the rung's health: the request is wrong for it, the model is fine (same rule as a context
    // overflow). The skip is recorded as `above the rung ceiling` so the diag can tell a real
    // context-class failure from a skipped-by-size gate.
    if (!canFitTokens(model, rungWindowTokens, orContextCatalog)) {
      attempts.push({ model, outcome: 'skipped',
        error: `input ~${rungWindowTokens}t above the rung ceiling` });
      continue;
    }
    const isFreeGo = isGo && model.endsWith('-free');
    const isPaidGo = isGo && !isFreeGo;
    const plan = hedgePlan(rungInputTokens);
    const rungBudget = Math.min(timeoutMs, left);
    // A SMALL context gets a SMALL budget — but only on the fast rung. Cutting zen to 0.4× would
    // destroy exactly what it is there for: a <2K prompt needs ~20 s there (9.4 s of zen + ~10 s
    // of queue), so the short budget must not leak onto the rung we fall back TO.
    const opts = {
      timeoutMs: isFreeGo ? Math.max(1_000, Math.round(rungBudget * plan.timeoutFactor)) : rungBudget,
      // Окно первого токена растёт с промптом (ttfbFactor): жирный запрос имеет префилл, и
      // фиксированные 15 с резали его до первого токена — см. замер и инцидент в size-policy.js.
      ttfbMs: Math.min(ttfbMs * ttfbFactor(rungInputTokens), left),
      wantJson, fetchImpl, conversation, appSlug, appTitle,
      // Resolved credential pool (env secrets + admin-managed tokens) and the OpenRouter cursor.
      pool: providerPool, orIndex,
    };
    const attemptKey = async (attemptKeyIndex) => {
      const result = await attempt(env, model, body, attemptKeyIndex, opts);
      return result;
    };
    // #316: a PAID rung must not START on a monthly-capped key — the gateway holds that request
    // until the rung budget expires (16–31 s of the live 2026-10-27 incident), while a fast
    // refusal would have cost milliseconds. The cap is not a fault, so shared `active` is left
    // alone: free rungs still ride the capped keys on purpose (the free tier survives the cap).
    if (isPaidGo && pool.length > 1) {
      if (!goCappedChecked) {
        goCappedChecked = true;
        goCapped = await goMonthlyCappedKeys(env, { fetchImpl });
      }
      if (goCapped && goCapped.size && goCapped.has(key)) {
        const alt = pool.findIndex((_, i) => !exhaustedAtStart.has(i) && !paidParked.has(i) && !goCapped.has(i));
        if (alt >= 0) key = alt;
      }
    }
    const tried = new Set([key]);
    // #316: keys whose fast 403 gets disproved by a healthy sibling during this rung's walk.
    const refused403Keys = new Set();
    // Гоним только бесплатный Go: у него четыре ключа, нет надбавки и нет дневной квоты, то есть
    // параллельный зов стоит только времени. Пин и стрим исключены — бенчмарк меряет один ранг,
    // а стрим закрепляет ранг на первом токене и отмена потерь тут отдельная история.
    const race = isFreeGo && plan.count > 1 && !pinRung && !body.stream && pool.length > 1;
    let r = race
      ? await raceGoKeys(model, key, plan.count, pool.length, tried, attemptKey)
      : await attemptKey(key);
    if (r.skip) continue;
    // #45 (owner «если ошибка то ретрай и далее потом по лесенке»): a guard-failed answer ALREADY
    // arrived — retrying the same rung costs a fraction of one ladder hop, while today's straight
    // descent pays double: a hop to a possibly pricier rung PLUS a health-skip that punishes every
    // other caller for 15s+ over one cheap flake (measured: an empty gpt-oss JSON in 285ms).
    // Bounded to ONE retry per rung; success records no failure at all. Visible in attempts as
    // outcome 'guard-retry' so flake rates stay measurable. Streams are excluded — a retry there
    // costs a full TTFB window, and the no-first-token path is not a guard failure.
    if (r.guard && !guardTried.has(model)) {
      guardTried.add(model);
      attempts.push({ model, outcome: 'guard-retry', key, error: r.error });
      reportProgress(onProgress, { type: 'retrying', rung: model, attempt: attempts.length, reason: 'invalid_response' });
      r = await attemptKey(key);
    }
    // `goParked` означает «для ПЛАТНЫХ рангов в пуле ничего не осталось» (#69). Бесплатный ранг
    // этим флагом не ограничивается: у него собственная карта пригодных ключей — платный парк
    // его не касается (#341), поэтому обход он обязан сделать сам. Раньше флаг глушил и этот
    // цикл: ступень не пропускалась, но и не получала ни одной запасной попытки — ровно одна на
    // ключе, который только что посадил платный 429, и отказ (инцидент 2026-10-28). Если
    // пригодного ключа правда нет, spare === -1 и цикл выходит сразу, без лишнего хопа.
    while (!r.ok && isGo && (!goParked || isFreeGo)) {
      const fault = keyFaultOf(r.error);
      if (fault) {
        // Тир решает, куда сажать (#341): временный лимит, увиденный ПЛАТНЫМ рангом, уходит в
        // платную карту и не лишает бесплатный фоллбэк обхода ключей. Всё остальное (мёртвый
        // ключ, отсутствие тарифа, лимит, увиденный бесплатным рангом) — свойство АККАУНТА и
        // сажается в общий exhausted, который читают все.
        const scope = fault.scope === 'rate_limit' && isPaidGo ? 'paid' : 'account';
        if (scope === 'paid') paidParked.add(key);
        const rot = await store.rotateKey(pool.length, fault.ttlMs, key, scope);
        if (!rot.rotated) {
          // #69: never park free Go rungs here — the allowance they don't consume is exactly
          // what the ladder must fall back on while every key is limited.
          await store.park(all.filter(m => m.startsWith('opencode-go/') && !m.endsWith('-free')), rot.retryAt);
          goParked = true;
          break;
        }
        attempts.push({ model, outcome: 'key-rotated', key, error: r.error });
        reportProgress(onProgress, { type: 'retrying', rung: model, attempt: attempts.length, reason: 'key_rotation' });
        key = rot.toIndex;
        keyIndex = key; // rotation is shared state: the rest of this call rides the spare key too
        tried.add(key);
        r = await attemptKey(key);
        if (r.skip) break;
        continue;
      }
      // Not a key-level signal. Never probe for problems the spare key cannot change: a context
      // overflow or a config-class rejection is a property of the request / the model, not the key.
      const cls = errorClass(r.error)?.class || 'transient';
      if (cls === 'context' || cls === 'config') break;
      // Бесплатный ранг обходит ВЕСЬ непробованный пул (#341). Его ограничивает только общий
      // exhausted — настоящее свойство аккаунта; платный парк и платный кап его не касаются.
      // Иначе у фоллбэк-ступени после 429 платного хопа остаётся ровно одна попытка на уже
      // припаркованном ключе — ровно так в инциденте 2026-10-28 погибал beam-free, хотя в пуле
      // был ключ, который в тот же час отвечал 61 разу.
      const spare = pool.findIndex((_, i) => !tried.has(i) && !exhaustedAtStart.has(i)
        && !(isPaidGo && paidParked.has(i))
        && !(isPaidGo && goCapped && goCapped.has(i)));
      if (spare < 0) break;
      // FAST rejection (the error string opens with `HTTP <code>`) → instant round trip, so the
      // rung keeps walking the usable pool: a dead key must not burn the one probe and hand the
      // rung to the exponential backoff while a healthy key idles (#311). SLOW failures keep the
      // one-per-call bound — a Go outage still cannot double the failover latency.
      if (/^HTTP\s+\d{3}\b/.test(String(r.error || ''))) {
        if (fastProbeBudget <= 0) break;
        fastProbeBudget--;
      } else {
        if (slowProbeBudget <= 0) break;
        slowProbeBudget--;
      }
      const failedKey = key;
      const refused403 = /^HTTP\s+403\b/.test(String(r.error || ''));
      if (refused403) refused403Keys.add(failedKey);
      attempts.push({ model, outcome: 'key-probe', key, error: r.error });
      reportProgress(onProgress, { type: 'retrying', rung: model, attempt: attempts.length, reason: 'key_probe' });
      key = spare;
      tried.add(key);
      r = await attemptKey(key);
      if (r.skip) break;
      if (r.ok) keyIndex = key; // the spare answered — keep it for the rest of this call
    }
    // #316: a healthy sibling served the very same payload → every fast 403 recorded during the
    // walk belonged to its KEY, not to the model or the request. Park those keys (rotateKey also
    // moves `active` off a parked start key, so the NEXT call stops paying the 403 tax). The TTL
    // is the quota one (15 min): a refusal that clears returns the key to the pool by itself.
    if (r.ok && refused403Keys.size) {
      refused403Keys.delete(key);
      for (const k of refused403Keys) {
        try {
          // 403 оставляем в ОБЩЕМ парке, как и раньше (#311/#316): его вывод «ключ отказал, а
          // здоровый брат отдал тот же payload» сделан про сам ключ, а не про тир, и ослаблять
          // это право тиром было бы откатом защиты. Тирная разводка нужна лимиту (429), который
          // и посадил все четыре ключа в инциденте 2026-10-28 (#341).
          await store.rotateKey(pool.length, KEY_QUOTA_TTL_MS, k, 'account');
        } catch { /* state is best-effort */ }
      }
    }
    // OpenRouter ACCOUNT limit (issue #297): the free allowance is counted per account, and the
    // provider names the limit and its reset moment in the body of the error. The account that hit
    // the limit is parked until that reset and stepped over by the remaining rungs of this call —
    // the RUNG stays eligible, so the accounts that still have allowance keep serving it. Before
    // this, one account's daily cap became an hour-long quota skip of the rung for EVERY account
    // of the pool (and the free profile had no other rung left).
    const orLimit = !r.ok && !r.skip && isOpenrouter ? (r.limit || orAccountLimitOf(r.error)) : null;
    if (orLimit) {
      const until = orParkUntil(orLimit, Date.now());
      const account = opts.orIndex;
      orParked[account] = Math.max(orParked[account] || 0, until);
      try { await store.parkAccount(account, until); } catch { /* fail-open: the park is an optimisation */ }
    }
    // OpenRouter credential rotation: several OpenRouter keys may sit in the pool, and a key-level
    // fault (401 revoked/invalid, 429 per-key rate limit) is a property of THAT key, not of the
    // rung — the next key can answer the same call. One rotation per rung keeps the latency
    // bounded: the paid tail is the last resort, not a place to spin through the whole pool.
    // 402 (insufficient credits) is an ACCOUNT-level limit — no credential rotation can fix it.
    if (!r.ok && !r.skip && isOpenrouter && providerPool.openrouter.length > 1 && orRotations < 1) {
      const fault = keyFaultOf(r.error);
      if (fault && !/\b402\b/.test(String(r.error || ''))) {
        orRotations++;
        attempts.push({ model, outcome: 'key-rotated', key: opts.orIndex, error: r.error });
        reportProgress(onProgress, { type: 'retrying', rung: model, attempt: attempts.length + 1, reason: 'key_rotation' });
        // The spare account must not be one of the parked ones (#297).
        const spare = nextUsableOpenrouterIndex(providerPool.openrouter.length, (opts.orIndex ?? 0) + 1, orParked, Date.now());
        if (spare >= 0) {
          opts.orIndex = spare;
          r = await attemptKey(key);
        }
      }
    }
    // Sticky same-rung retry (S5): the pinned rung on a paid (non-Go) rung gets ONE more chance on
    // a transient error before the ladder moves — Go already gets its spare-key probe above.
    if (!r.ok && !r.skip && !isGo && pinRetryLeft > 0 && pinned && model === pinned.rung) {
      const cls = errorClass(r.error)?.class || 'transient';
      if (cls === 'transient') {
        pinRetryLeft = 0;
        attempts.push({ model, outcome: 'pin-retry', key, error: r.error });
        reportProgress(onProgress, { type: 'retrying', rung: model, attempt: attempts.length, reason: 'pinned_rung' });
        r = await attemptKey(key);
      }
    }
    if (r.ok) {
      if (conversation && !pinRung) {
        pinState = model === (pinned && pinned.rung) ? 'hit' : (pinned ? 'moved' : 'new');
      }
      attempts.push({ model, outcome: 'ok', key });
      await store.recordSuccess(model, conversation && !pinRung ? { pin: { pinKey: conversation, rung: model } } : {});
      return { ok: true, model, data: r.data, content: r.content, stream: r.stream, attempts, pin: pinState,
        estimated_tokens_in: estimatedTokensIn };
    }
    attempts.push({ model, outcome: 'error', key, error: r.error });
    previousFailedRung = model;
    if (!goParked) {
      const cls = errorClass(r.error)?.class || 'transient';
      // ⚫-1 resolution (step 3): a context-class overflow while STUCK on the pinned rung returns
      // the error as-is (the same request would overflow every lower rung — a retry loop cannot
      // help) and INVALIDATES the pin, so the next turn picks fresh instead of dying on the model.
      if (conversation && cls === 'context' && pinned && model === pinned.rung) {
        await store.recordFailure(model, failureClass(r.error), { pinRemove: { pinKey: conversation, model } });
        pinState = 'gone';
        const m2 = /^HTTP[^\s]* (\d+)/.exec(r.error);
        return { ok: false, status: m2 ? Number(m2[1]) : 502, error: r.error, attempts, pin: 'gone',
          estimated_tokens_in: estimatedTokensIn };
      }
      // An ACCOUNT-level OpenRouter limit is not a rung problem: the account is parked until its own
      // reset, and the rung must stay eligible for the accounts that still have allowance (#297).
      // Recording it as 'quota' skipped the rung for an hour on EVERY account of the pool.
      const accountLimit = isOpenrouter ? (r.limit || orAccountLimitOf(r.error)) : null;
      await store.recordFailure(model, accountLimit ? { cls: 'transient' } : failureClass(r.error));
    }
  }
  return { ok: false, status: 502, error: 'every rung failed', attempts, pin: conversation ? pinState : null,
  estimated_tokens_in: estimatedTokensIn };
}
