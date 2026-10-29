// Provider messages and bodies are not a caller-facing or durable diagnostic contract. Keep the
// rung + outcome + a stable reason/status while dropping arbitrary upstream response text.
//
// Вторая обязанность — УЗНАВАТЬ СВОИ же сообщения. /v1/calls и /v1/analytics прогоняют через
// эту же функцию попытки, уже сохранённые в D1, поэтому каждое каноническое сообщение обязано
// пережить повторный проход: иначе `provider returned HTTP 429` деградирует в общий
// `provider request failed`, http_status теряется, и /v1/analytics сводит шесть статусов к
// двум бакетам (#339).

// Собственные причины лестницы: строку сгенерировал ЭТОТ воркер, в ней только наши числа —
// ни тел провайдера, ни промптов, ни ключей. Прятать её за `provider request failed` — значит
// стереть единственное объяснение, почему ступень НЕ ПРОБОВАЛАСЬ: в инциденте 2026-10-28
// 23:05Z 183 из 363 отказов не содержали ни одной попытки к провайдеру, а в трассе стояло
// `provider request failed`. Поэтому такие попытки несут `source: 'ladder'` — провайдер не
// вызывался, и потребитель получает `ladder_unavailable`, а не `provider_unavailable` (#339).
const LADDER_OWNED = [
  { re: /^time budget spent$/, reason: 'ladder_time_budget' },
  { re: /^every OpenRouter account is parked until its limit resets$/, reason: 'openrouter_pool_parked' },
  { re: /^input ~\d+t above the rung ceiling$/, reason: 'input_above_rung_ceiling' },
  // Кольцо вешает этот отказ на свой префикс (`zen-rings: …`) и может дописать наш же
  // диагностический хвост в скобках — цифры наши, хвощ тоже, поэтому начало достаточно.
  { re: /^(?:zen-rings: )?input is too long for the free tier: \d+ bytes, limit \d+/, reason: 'input_too_long_for_free_tier' },
  { re: /^(?:zen-rings: )?input is too long for the ring payload/, reason: 'input_too_long_for_ring_payload' },
];

export function safeAttemptDiagnostics(attempts = []) {
  return attempts.map((attempt) => {
    const model = String(attempt.model || 'unknown');
    const raw = String(attempt.error || '');
    const outcome = String(attempt.outcome || 'error');
    // Индекс ключа из СВОЕГО пула (#341). Это наше структурное поле: маленькое целое, не тело
    // провайдера и не промпт, поэтому контракт #256 его не касается. key-probe и key-rotated
    // несли его с самого начала, а терялся он на последнем шагу — из-за чего в инциденте
    // 2026-10-28 приходилось восстанавливать, какие именно ключи садились, по модели-источнику
    // 429, вместо того чтобы просто посмотреть на индексы.
    // Проходит только НЕОТРИЦАТЕЛЬНОЕ ЦЕЛОЕ: индекс пула — это 0..poolSize-1. Любое другое
    // значение (строка, дробь, отрицательное, объект) отбрасывается целиком, так что даже cred,
    // случайно положенный в это поле, в D1 не попадёт.
    const key = Number.isInteger(attempt.key) && attempt.key >= 0 ? { key: attempt.key } : {};

    if (!raw) return { model, outcome, ...key };

    // Своё сообщение доезжает дословно: и на записи, и при повторном чтении из D1.
    const own = LADDER_OWNED.find((o) => o.re.test(raw));
    if (own) return { model, outcome, ...key, source: 'ladder', reason_code: own.reason, error: raw };

    const source = model.startsWith('zen-rings/') ? 'zen_pool' : 'provider';
    // Префикс `provider returned` распознаётся тоже: /v1/calls и /v1/analytics передают сюда
    // уже нормализованную строку, и без него статус деградировал бы до `provider request failed`.
    const http = /^(?:provider returned )?HTTP\s+(\d{3})\b/i.exec(raw);

    if (http) {
      return {
        model, outcome, ...key, source,
        reason_code: 'provider_http_error',
        http_status: Number(http[1]),
        error: `provider returned HTTP ${http[1]}`,
      };
    }
    if (/timeout|timed out|aborterror|no first token in time|before first token|provider timed out/i.test(raw)) {
      return { model, outcome, ...key, source, reason_code: 'provider_timeout', error: 'provider timed out' };
    }
    if (/warming up|cold ring|pool_backlog|no warm worker|capacity unavailable/i.test(raw)) {
      return { model, outcome, ...key, source: 'zen_pool', reason_code: 'provider_capacity', error: 'Zen worker capacity unavailable' };
    }
    if (/empty answer|empty response|invalid json|no usable answer/i.test(raw)) {
      return { model, outcome, ...key, source, reason_code: 'provider_empty_response', error: 'provider returned no usable answer' };
    }
    if (/fetch failed|network|socket|econn|tls|upstream request failed|connection failed/i.test(raw)) {
      return { model, outcome, ...key, source, reason_code: 'provider_transport_error', error: 'provider connection failed' };
    }

    return { model, outcome, ...key, source, reason_code: 'provider_error', error: 'provider request failed' };
  });
}
