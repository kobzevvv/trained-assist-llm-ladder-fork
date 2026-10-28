import { test } from 'node:test';
import assert from 'node:assert/strict';
import { estimateTokens, hedgePlan, ttfbFactor, contextOf, GO_CONTEXT, canFitTokens } from '../src/size-policy.js';

test('estimateTokens: a token count without talking to a provider', () => {
  assert.equal(estimateTokens(undefined), 0, 'no messages → 0, and it must not throw');
  assert.equal(estimateTokens({ messages: [] }), 0, 'empty array → 0');
  // The estimate counts the WHOLE JSON (roles, history, tool schemas), not just content, and
  // adds the response we are about to ask for (max_tokens) plus a 10 % floor for the
  // provider's own overhead — measured 2026-10-28 against ladder_calls.tokens_in for
  // successful calls: the estimate tracks the provider's count within ~10 % across the
  // 8K–1M range and never overstates a real input.
  const t400 = estimateTokens({ messages: [{ role: 'user', content: 'x'.repeat(400) }] });
  assert.ok(t400 >= 100 && t400 <= 130, `400 символов ≈ 100-130 токенов, получили ${t400}`);
  // the estimate is on the JSON, so history, roles and tool schemas all count
  const withHistory = estimateTokens({ messages: [{ role: 'system', content: 'a' }, { role: 'user', content: 'b'.repeat(400) }] });
  assert.ok(withHistory > 100, 'more messages → more tokens, not fewer');
  // max_tokens is part of the context: the provider counts input + the response we ask for.
  const withoutMax = estimateTokens({ messages: [{ role: 'user', content: 'x'.repeat(400) }] });
  const withMax = estimateTokens({ messages: [{ role: 'user', content: 'x'.repeat(400) }], max_tokens: 4000 });
  assert.ok(withMax > withoutMax + 800, `max_tokens 4000 adds ~1000+ tokens: ${withoutMax} → ${withMax}`);

  // Ключевой случай: кириллица занимает 2 байта на символ, поэтому оценка ПО БАЙТАМ даёт
  // почти вдвое больше, чем len/4 по символам. Если считать по символам — 523 КБ русского
  // текста выглядят как «~80K токенов» (в потолке) и уходят в шлюз, который отвечает 429.
  const cyr = { messages: [{ role: 'user', content: 'я'.repeat(4000) }] };
  const byChars = Math.ceil(JSON.stringify(cyr.messages).length / 4);
  const byBytes = estimateTokens(cyr);
  assert.ok(byBytes > byChars * 1.5, `по байтам должно быть заметно больше: ${byBytes} против ${byChars}`);
});

// contextOf: one place decides whether a rung may receive a request. Three sources, in
// priority — Go ceilings (measured), OpenRouter catalog (D1), null = unknown = do not filter.
test('contextOf: Go ceilings, OpenRouter catalog, unknown → null', () => {
  assert.equal(contextOf('opencode-go/beam-free', {}), 100_000, 'Go beam-free = 100K');
  assert.equal(contextOf('opencode-go/unknown-model', {}), 100_000, 'unknown Go model → 100K floor');
  assert.equal(contextOf('openrouter/x/y:free', { 'openrouter/x/y:free': 512_000 }), 512_000,
    'OpenRouter from the catalog');
  assert.equal(contextOf('openrouter/x/y:free', {}), null,
    'OpenRouter not in the catalog → null (do not pre-filter)');
  assert.equal(contextOf('zen-rings/big-pickle', {}), null,
    'zen-rings → null (the ring is switched off; nothing to pre-filter)');
  assert.equal(contextOf('opencode-zen/mimo-v2.6-flash-free', {}), null,
    'zen relay → null');
});

// canFitTokens: null window means "unknown — do not pre-filter", so a request that might fit
// is never refused on a guess.
test('canFitTokens: null window never refuses, a known window does', () => {
  const cat = { 'openrouter/x/y:free': 256_000 };
  assert.equal(canFitTokens('openrouter/x/y:free', 100_000, cat), true, 'fits');
  assert.equal(canFitTokens('openrouter/x/y:free', 300_000, cat), false, 'over the 256K window');
  assert.equal(canFitTokens('openrouter/unknown:free', 5_000_000, cat), true,
    'unknown window → never refuse, even at 5M');
  assert.equal(canFitTokens('opencode-go/beam-free', 90_000), true, 'Go under 100K');
  assert.equal(canFitTokens('opencode-go/beam-free', 100_001), false, 'Go over 100K');
});

// The owner's bands (2026-10-07): a small context gets a small budget, a mid-size request is
// raced twice, a large one three times. The bands are declared here so the wiring in run() and
// the second step (the race itself) read from one place.
test('hedgePlan: bands by token count — 1 / 2 / 3 / 1', () => {
  assert.deepEqual(hedgePlan(0), { count: 1, timeoutFactor: 0.6 }, 'a tiny prompt gets a short budget');
  assert.deepEqual(hedgePlan(1_999), { count: 1, timeoutFactor: 0.6 });
  assert.deepEqual(hedgePlan(2_000), { count: 2, timeoutFactor: 1 }, 'bands are exclusive at the top');
  assert.deepEqual(hedgePlan(31_999), { count: 2, timeoutFactor: 1 });
  assert.deepEqual(hedgePlan(32_000), { count: 3, timeoutFactor: 1 });
  assert.deepEqual(hedgePlan(127_999), { count: 3, timeoutFactor: 1 });
  assert.deepEqual(hedgePlan(128_000), { count: 1, timeoutFactor: 1 }, 'past the ceiling band it is one rung again');
  assert.deepEqual(hedgePlan(5_000_000), { count: 1, timeoutFactor: 1 }, 'and it never throws');
});

// Замер 2026-10-08 (трасса за сутки): 21 из 28 отказов `no first token in time` — на payload
// ≥ 100 КБ, медиана 978 830 байт. Жирный промпт имеет префилл, 15 с ему не хватает.
test('ttfbFactor: окно первого токена растёт вместе с промптом', () => {
  assert.equal(ttfbFactor(0), 1, 'пустой запрос — как просили');
  assert.equal(ttfbFactor(31_999), 1, 'до 32K — без надбавки');
  assert.equal(ttfbFactor(32_000), 2, '32K — двойное окно (15 с → 30 с)');
  assert.equal(ttfbFactor(127_999), 2, '128K — всё ещё двойное');
  assert.equal(ttfbFactor(128_000), 3, '>128K — тройное (15 с → 45 с)');
  assert.equal(ttfbFactor(248_678), 3, 'медиана инцидента — на максимуме');
  assert.ok(ttfbFactor(1_000_000) <= 3, 'надбавка ограничена: failover не должен виснуть');
});
