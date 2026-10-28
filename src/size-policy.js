// Size policy — estimate request size for timeout and concurrency planning.
//
// Everything here is measured, not guessed; the numbers say where and when.
//
// ── Bands ───────────────────────────────────────────────────────────────────────────────────
// The owner's plan (2026-10-07): a small context deserves a small budget and a fast kill; a
// mid-size request is raced twice; a large one three times. The estimate
// controls timing and concurrency, never whether a provider receives the input.
export const BANDS = Object.freeze([
  // <2K — один ранг, короткий бюджет. 0.4 (= 8 с при дефолтных 20) оказался впритык: замер
  // 2026-10-08 на естественном промпте дал longcat 7.0 / 8.6 с, и одна попытка из трёх
  // обрывалась ровно на потолке. 0.6 (= 12 с) остаётся на 40 % короче дефолта, но уже выше
  // наблюдаемой латентности — «убить быстро» имеет смысл только если модель успевает ответить
  // здоровой.
  { max: 2_000, count: 1, timeoutFactor: 0.6 },
  { max: 32_000, count: 2, timeoutFactor: 1 },    //  2–32K — гоним две
  { max: 128_000, count: 3, timeoutFactor: 1 },   // 32–128K — гоним три
  { max: Infinity, count: 1, timeoutFactor: 1 },  // >128K — one request
]);

// Cheap UTF-8 estimate for timing and concurrency. The provider decides whether
// the request fits its context window.
//
// bytes/4 is the floor, not the answer: the provider's tokenizer counts the WHOLE
// request (messages + tools + the response we will ask for), and non-English text
// tokenises at ~2.5 chars/token instead of 4. So the estimate is raised by the
// response we are about to ask for (max_tokens) and by a 10 % floor for the
// tokenizer's own overhead — measured 2026-10-28 against ladder_calls.tokens_in
// (the provider's own count) for successful calls: the estimate tracks it within
// ~10 % across the 8K–1M range, and never overstates a real input.
export function estimateTokens(body) {
  const m = body?.messages;
  if (!Array.isArray(m) || !m.length) return 0;
  try {
    const bytes = new TextEncoder().encode(JSON.stringify(m)).length;
    const maxOut = Math.max(Number(body?.max_tokens) || 0, 0);
    return Math.ceil((bytes + maxOut * 4) / 4 * 1.1);
  } catch { return 0; }
}

// ── Context windows ──────────────────────────────────────────────────────────────────────────
// One place decides whether a rung may receive a request. Three sources, in priority:
//   1. opencode-go — measured ceiling (GO_CONTEXT in ladder.js, all 100K). The ladder
//      already skips rungs over it; this module just names the contract.
//   2. openrouter — the catalog's `context` column (256K / 512K / 1M per model, or
//      null when the provider has not published one). Loaded by the caller from D1;
//      this module is provider-agnostic and takes the map as an argument.
//   3. anything else — unknown. Returning null means "do not pre-filter": an unknown
//      window is not a reason to refuse a request that might fit.
//
// The gate in run() uses the result to skip a rung BEFORE the attempt. Skipping is
// recorded as `above the rung ceiling` so the diag can tell a real context-class
// failure from a skipped-by-size gate, and it must NOT poison the rung's health —
// the request is wrong for it, the model is fine.
export function contextOf(model, catalog = {}) {
  if (model.startsWith('opencode-go/')) {
    return GO_CONTEXT[model] || 100_000;
  }
  if (model.startsWith('openrouter/')) {
    const ctx = catalog[model];
    return Number.isFinite(ctx) && ctx > 0 ? ctx : null;
  }
  return null;
}

// Whether a rung may receive a request. The catalog argument carries the OpenRouter
// context windows (from free_models.context); Go ceilings live in GO_CONTEXT. A null window
// means "unknown — do not pre-filter": an unknown window is not a reason to refuse a
// request that might fit, and refusing on a guess would push every call onto the paid tail.
export function canFitTokens(model, tokens, catalog = {}) {
  const ctx = contextOf(model, catalog);
  return ctx === null || tokens <= ctx;
}

// Static Go ceilings — kept here so size-policy can answer "is this a Go model" without
// importing ladder.js (which would close the import cycle: ladder imports size-policy).
export const GO_CONTEXT = Object.freeze({
  'opencode-go/beam-free': 100_000,
  'opencode-go/mimo-v2.6-flash': 100_000,
  'opencode-go/qwen3.8-flash': 100_000,
  'opencode-go/deepseek-v4-pro': 100_000,
  'opencode-go/mimo-v2.6-flash-free': 100_000,
  'opencode-go/mimo-v2.5-free': 100_000,
  'opencode-go/qwen3.7-plus': 100_000,
  'opencode-go/deepseek-v4.1-flash': 100_000,
  'opencode-go/glm-5.3-flash': 100_000,
  'opencode-go/space-bunny-free': 100_000,
});

// ── TTFB ─────────────────────────────────────────────────────────────────────────────────────
// Окно первого токена должно расти вместе с промптом: у жирного запроса есть префилл, и 15 с ему
// не хватает. Замер 2026-10-08 по трассе за сутки: из 28 отказов `no first token in time` —
// 21 на payload ≥ 100 КБ, медиана 978 830 байт (≈ 245K токенов); на маленьких промптах их 3.
// Последствие видно в инциденте того же дня: обе ступени с большим окном (sante:free 262K и
// nemotron-3-ultra-550b-a55b:free 1M) health-skip'ались именно так, и лестница отвечала
// `every rung failed`, хотя физически промпт в обе умещался.
//
// Почему множитель, а не отдельное окно: вызывающий уже задаёт своё `ladder_ttfb_ms` —
// мы только поднимаем пол. Медленный первый токен у большого запроса здесь дороже
// потерявшегося окна failover.
export function ttfbFactor(tokens) {
  if (tokens < 32_000) return 1;      // до 32K — как и просили
  if (tokens < 128_000) return 2;     // 32–128K — двойное окно
  return 3;                           // >128K — тройное (15 с → 45 с)
}

// The band for a token count: { count, timeoutFactor }.
export function hedgePlan(tokens) {
  const band = BANDS.find((b) => tokens < b.max) || BANDS[BANDS.length - 1];
  return { count: band.count, timeoutFactor: band.timeoutFactor };
}
