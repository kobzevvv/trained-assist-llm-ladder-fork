// Мониторинг ошибок по моделям и размерам запросов (issue #335).
// Решает проблему PR #333: "контекстный пре-фильтр по реальным потолкам моделей"
//
// Модуль отслеживает частоту ошибок для каждой модели и распределение ошибок
// по размерам запросов (estimated_tokens_in). Это позволяет обнаруживать:
//   - изменения в частоте ошибок по моделям
//   - сдвиги в распределении ошибок по размерам запросов
//   - модели, которые начали чаще падать на больших запросах
//
// Модуль не зависит от Workerd runtime (как src/handler.js), чтобы работать
// в node --test и в воркере.

// Бакеты по размеру запроса (в tokens), согласованы с context windows в config/contexts.json
export const SIZE_BUCKETS = [
  { name: 'small', max: 4096 },      // Маленькие вызовы (классификаторы, кнопки)
  { name: 'medium', max: 16384 },    // Средние (форматирование, суммаризация)
  { name: 'large', max: 65536 },     // Большие (контекстные вызовы)
  { name: 'xlarge', max: Infinity }, // Очень большие (жирные промпты)
];

// Определение бакета по размеру запроса
export function bucketOf(estimatedTokens) {
  if (estimatedTokens == null || estimatedTokens < 0 || Number.isNaN(estimatedTokens)) return 'unknown';
  if (estimatedTokens === Infinity) return 'xlarge';
  if (!Number.isFinite(estimatedTokens)) return 'unknown';
  for (const bucket of SIZE_BUCKETS) {
    if (estimatedTokens <= bucket.max) return bucket.name;
  }
  return 'xlarge';
}

// Сбор метрик ошибок по моделям за период.
// Возвращает массив объектов с метриками по каждой модели.
export async function collectErrorMetrics(db, {
  since = Date.now() - 24 * 3600 * 1000,
  until = Date.now(),
} = {}) {
  if (!db) return [];

  const { results = [] } = await db.prepare(`
    SELECT
      model,
      COUNT(*) as total_calls,
      SUM(CASE WHEN ok = 0 THEN 1 ELSE 0 END) as failed_calls,
      AVG(estimated_tokens_in) as avg_estimated_tokens,
      -- Распределение по размерам (вызовы)
      SUM(CASE WHEN estimated_tokens_in <= 4096 THEN 1 ELSE 0 END) as small_calls,
      SUM(CASE WHEN estimated_tokens_in > 4096 AND estimated_tokens_in <= 16384 THEN 1 ELSE 0 END) as medium_calls,
      SUM(CASE WHEN estimated_tokens_in > 16384 AND estimated_tokens_in <= 65536 THEN 1 ELSE 0 END) as large_calls,
      SUM(CASE WHEN estimated_tokens_in > 65536 THEN 1 ELSE 0 END) as xlarge_calls,
      -- Распределение по размерам (ошибки)
      SUM(CASE WHEN ok = 0 AND estimated_tokens_in <= 4096 THEN 1 ELSE 0 END) as small_failures,
      SUM(CASE WHEN ok = 0 AND estimated_tokens_in > 4096 AND estimated_tokens_in <= 16384 THEN 1 ELSE 0 END) as medium_failures,
      SUM(CASE WHEN ok = 0 AND estimated_tokens_in > 16384 AND estimated_tokens_in <= 65536 THEN 1 ELSE 0 END) as large_failures,
      SUM(CASE WHEN ok = 0 AND estimated_tokens_in > 65536 THEN 1 ELSE 0 END) as xlarge_failures
    FROM ladder_calls
    WHERE ts >= ?1 AND ts <= ?2 AND model IS NOT NULL
    GROUP BY model
    ORDER BY total_calls DESC
  `).bind(since, until).all();

  return results.map(row => ({
    model: row.model,
    totalCalls: Number(row.total_calls) || 0,
    failedCalls: Number(row.failed_calls) || 0,
    errorRate: Number(row.total_calls) > 0 ? Number(row.failed_calls) / Number(row.total_calls) : 0,
    avgEstimatedTokens: Number(row.avg_estimated_tokens) || null,
    sizeDistribution: {
      small: {
        calls: Number(row.small_calls) || 0,
        failures: Number(row.small_failures) || 0,
      },
      medium: {
        calls: Number(row.medium_calls) || 0,
        failures: Number(row.medium_failures) || 0,
      },
      large: {
        calls: Number(row.large_calls) || 0,
        failures: Number(row.large_failures) || 0,
      },
      xlarge: {
        calls: Number(row.xlarge_calls) || 0,
        failures: Number(row.xlarge_failures) || 0,
      },
    },
  }));
}

// Детект изменений в частоте ошибок и распределении по размерам.
// Сравнивает текущие метрики с предыдущими.
// Возвращает массив изменений.
export function detectErrorRateChange(current, previous, { threshold = 0.1 } = {}) {
  const changes = [];
  const prevByModel = new Map(previous.map(p => [p.model, p]));

  for (const curr of current) {
    const prev = prevByModel.get(curr.model);

    // Новая модель (не было в предыдущем периоде)
    if (!prev) {
      if (curr.totalCalls >= 10) { // Игнорируем модели с малым количеством вызовов
        changes.push({
          model: curr.model,
          type: 'new_model',
          currentErrorRate: curr.errorRate,
          totalCalls: curr.totalCalls,
        });
      }
      continue;
    }

    // Проверка изменения общей частоты ошибок
    const errorRateDiff = Math.abs(curr.errorRate - prev.errorRate);
    // Используем epsilon для сравнения с плавающей точкой
    if (errorRateDiff > threshold - 1e-10 && curr.totalCalls >= 10) {
      changes.push({
        model: curr.model,
        type: 'error_rate_change',
        previousErrorRate: prev.errorRate,
        currentErrorRate: curr.errorRate,
        diff: errorRateDiff,
        direction: curr.errorRate > prev.errorRate ? 'worse' : 'better',
      });
    }

    // Проверка изменения распределения по размерам
    for (const bucket of ['small', 'medium', 'large', 'xlarge']) {
      const currData = curr.sizeDistribution?.[bucket] || { calls: 0, failures: 0 };
      const prevData = prev.sizeDistribution?.[bucket] || { calls: 0, failures: 0 };

      // Пропускаем бакеты с малым количеством вызовов
      if (currData.calls < 5 || prevData.calls < 5) continue;

      const currRate = currData.calls > 0 ? currData.failures / currData.calls : 0;
      const prevRate = prevData.calls > 0 ? prevData.failures / prevData.calls : 0;
      const rateDiff = Math.abs(currRate - prevRate);

      // Используем epsilon для сравнения с плавающей точкой
      if (rateDiff > threshold - 1e-10) {
        changes.push({
          model: curr.model,
          type: 'size_distribution_change',
          bucket,
          previousRate: prevRate,
          currentRate: currRate,
          diff: rateDiff,
          direction: currRate > prevRate ? 'worse' : 'better',
          calls: currData.calls,
          failures: currData.failures,
        });
      }
    }
  }

  return changes;
}

// Запись метрик в историю для отслеживания трендов.
export async function writeErrorMetrics(db, metrics, {
  periodStart = Date.now() - 24 * 3600 * 1000,
  periodEnd = Date.now(),
  now = Date.now(),
} = {}) {
  if (!db || !metrics.length) return 0;

  const statements = metrics.map(m => ({
    sql: `INSERT INTO error_metrics_history
      (collected_at, period_start, period_end, model_id, total_calls, failed_calls,
       error_rate, avg_estimated_tokens, size_distribution, raw_metrics)
      VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)`,
    params: [
      now, periodStart, periodEnd, m.model, m.totalCalls, m.failedCalls,
      m.errorRate, m.avgEstimatedTokens,
      JSON.stringify(m.sizeDistribution), JSON.stringify(m),
    ],
  }));

  // D1 не поддерживает multi-row VALUES; батчим по 50
  for (let i = 0; i < statements.length; i += 50) {
    const chunk = statements.slice(i, i + 50);
    if (typeof db.batch === 'function') {
      await db.batch(chunk.map(s => db.prepare(s.sql).bind(...s.params)));
    } else {
      for (const s of chunk) {
        await db.prepare(s.sql).bind(...s.params).run();
      }
    }
  }

  return statements.length;
}

// Чтение последних метрик для сравнения.
export async function readPreviousErrorMetrics(db, { before = Date.now(), limit = 100 } = {}) {
  if (!db) return [];

  const { results = [] } = await db.prepare(`
    SELECT model_id, total_calls, failed_calls, error_rate, avg_estimated_tokens, size_distribution
    FROM error_metrics_history
    WHERE collected_at < ?1
    ORDER BY collected_at DESC
    LIMIT ?2
  `).bind(before, limit).all();

  return results.map(row => ({
    model: row.model_id,
    totalCalls: Number(row.total_calls) || 0,
    failedCalls: Number(row.failed_calls) || 0,
    errorRate: Number(row.error_rate) || 0,
    avgEstimatedTokens: Number(row.avg_estimated_tokens) || null,
    sizeDistribution: JSON.parse(row.size_distribution || '{}'),
  }));
}

// Полный цикл сбора и детекта изменений.
export async function monitorErrorRates(db, {
  since = Date.now() - 24 * 3600 * 1000,
  until = Date.now(),
  threshold = 0.1,
  write = true,
} = {}) {
  // Сбор текущих метрик
  const current = await collectErrorMetrics(db, { since, until });

  // Чтение предыдущих метрик
  const previous = await readPreviousErrorMetrics(db, { before: since });

  // Детект изменений
  const changes = detectErrorRateChange(current, previous, { threshold });

  // Запись текущих метрик
  if (write && current.length > 0) {
    await writeErrorMetrics(db, current, { periodStart: since, periodEnd: until, now: until });
  }

  return {
    collectedAt: new Date(until).toISOString(),
    periodStart: new Date(since).toISOString(),
    periodEnd: new Date(until).toISOString(),
    models: current.length,
    totalCalls: current.reduce((sum, m) => sum + m.totalCalls, 0),
    totalFailures: current.reduce((sum, m) => sum + m.failedCalls, 0),
    changes,
    metrics: current,
  };
}
