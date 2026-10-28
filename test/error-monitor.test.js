// Тесты для error-monitor.js (issue #335).
// Мониторинг частоты ошибок по моделям и распределению по размерам запросов.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  SIZE_BUCKETS,
  bucketOf,
  collectErrorMetrics,
  detectErrorRateChange,
  writeErrorMetrics,
  readPreviousErrorMetrics,
  monitorErrorRates,
} from '../src/error-monitor.js';

// ── Вспомогательные функции ──────────────────────────────────────────────────

// Фейковый D1 для тестирования
function createFakeDb(ladderCalls = []) {
  return {
    tables: {
      ladder_calls: ladderCalls,
      error_metrics_history: [],
    },
    prepare(sql) {
      const db = this;
      return {
        bind(...params) {
          return {
            async all() {
              if (sql.includes('FROM ladder_calls')) {
                return { results: db.queryLadderCalls(params[0], params[1]) };
              }
              if (sql.includes('FROM error_metrics_history')) {
                return { results: db.queryErrorMetrics(params[0], params[1]) };
              }
              return { results: [] };
            },
            async run() {
              if (sql.includes('INSERT INTO error_metrics_history')) {
                db.tables.error_metrics_history.push({
                  collected_at: params[0],
                  period_start: params[1],
                  period_end: params[2],
                  model_id: params[3],
                  total_calls: params[4],
                  failed_calls: params[5],
                  error_rate: params[6],
                  avg_estimated_tokens: params[7],
                  size_distribution: params[8],
                  raw_metrics: params[9],
                });
                return { success: true };
              }
              return { success: true };
            },
          };
        },
      };
    },
    queryLadderCalls(since, until) {
      // Простая агрегация по model
      const byModel = new Map();
      for (const call of this.tables.ladder_calls) {
        if (call.ts < since || call.ts > until || !call.model) continue;
        const m = byModel.get(call.model) || {
          model: call.model,
          total_calls: 0,
          failed_calls: 0,
          estimated_tokens_sum: 0,
          small_calls: 0, medium_calls: 0, large_calls: 0, xlarge_calls: 0,
          small_failures: 0, medium_failures: 0, large_failures: 0, xlarge_failures: 0,
        };
        m.total_calls++;
        if (call.ok === 0) m.failed_calls++;
        if (call.estimated_tokens_in != null) {
          m.estimated_tokens_sum += call.estimated_tokens_in;
          const bucket = bucketOf(call.estimated_tokens_in);
          m[`${bucket}_calls`]++;
          if (call.ok === 0) m[`${bucket}_failures`]++;
        }
        byModel.set(call.model, m);
      }
      return [...byModel.values()].map(m => ({
        ...m,
        avg_estimated_tokens: m.total_calls > 0 ? m.estimated_tokens_sum / m.total_calls : null,
      }));
    },
    queryErrorMetrics(before, limit) {
      return this.tables.error_metrics_history
        .filter(r => r.collected_at < before)
        .sort((a, b) => b.collected_at - a.collected_at)
        .slice(0, limit)
        .map(r => ({
          model_id: r.model_id,
          total_calls: r.total_calls,
          failed_calls: r.failed_calls,
          error_rate: r.error_rate,
          avg_estimated_tokens: r.avg_estimated_tokens,
          size_distribution: r.size_distribution,
        }));
    },
  };
}

// Генерация тестовых вызовов
function generateCalls({ model, count, failures, avgTokens, ts = Date.now() }) {
  const calls = [];
  for (let i = 0; i < count; i++) {
    const isFailure = i < failures;
    calls.push({
      ts,
      model,
      ok: isFailure ? 0 : 1,
      estimated_tokens_in: avgTokens + (i % 100) * 10,
    });
  }
  return calls;
}

// ── bucketOf ─────────────────────────────────────────────────────────────────

test('bucketOf: определяет бакет по размеру запроса', () => {
  assert.equal(bucketOf(100), 'small');
  assert.equal(bucketOf(4096), 'small');
  assert.equal(bucketOf(4097), 'medium');
  assert.equal(bucketOf(16384), 'medium');
  assert.equal(bucketOf(16385), 'large');
  assert.equal(bucketOf(65536), 'large');
  assert.equal(bucketOf(65537), 'xlarge');
  assert.equal(bucketOf(Infinity), 'xlarge');
  assert.equal(bucketOf(-1), 'unknown');
  assert.equal(bucketOf(null), 'unknown');
  assert.equal(bucketOf(undefined), 'unknown');
  assert.equal(bucketOf(NaN), 'unknown');
});

test('SIZE_BUCKETS: бакеты покрывают весь диапазон', () => {
  assert.equal(SIZE_BUCKETS.length, 4);
  assert.equal(SIZE_BUCKETS[0].name, 'small');
  assert.equal(SIZE_BUCKETS[3].max, Infinity);
});

// ── collectErrorMetrics ──────────────────────────────────────────────────────

test('collectErrorMetrics: собирает метрики по моделям', async () => {
  const now = Date.now();
  const calls = [
    ...generateCalls({ model: 'model-a', count: 100, failures: 10, avgTokens: 2000, ts: now }),
    ...generateCalls({ model: 'model-b', count: 50, failures: 5, avgTokens: 10000, ts: now }),
  ];
  const db = createFakeDb(calls);

  const metrics = await collectErrorMetrics(db, { since: now - 1000, until: now + 1000 });

  assert.equal(metrics.length, 2);
  const modelA = metrics.find(m => m.model === 'model-a');
  assert.ok(modelA);
  assert.equal(modelA.totalCalls, 100);
  assert.equal(modelA.failedCalls, 10);
  assert.ok(Math.abs(modelA.errorRate - 0.1) < 0.001);
});

test('collectErrorMetrics: учитывает распределение по размерам', async () => {
  const now = Date.now();
  const calls = [
    // Маленькие вызовы (small)
    ...generateCalls({ model: 'test', count: 10, failures: 2, avgTokens: 1000, ts: now }),
    // Средние вызовы (medium)
    ...generateCalls({ model: 'test', count: 10, failures: 3, avgTokens: 10000, ts: now }),
  ];
  const db = createFakeDb(calls);

  const metrics = await collectErrorMetrics(db, { since: now - 1000, until: now + 1000 });

  assert.equal(metrics.length, 1);
  const test = metrics[0];
  assert.equal(test.sizeDistribution.small.calls, 10);
  assert.equal(test.sizeDistribution.small.failures, 2);
  assert.equal(test.sizeDistribution.medium.calls, 10);
  assert.equal(test.sizeDistribution.medium.failures, 3);
});

test('collectErrorMetrics: пустой db возвращает пустой массив', async () => {
  const metrics = await collectErrorMetrics(null);
  assert.deepEqual(metrics, []);
});

// ── detectErrorRateChange ────────────────────────────────────────────────────

test('detectErrorRateChange: обнаруживает изменение частоты ошибок', () => {
  const current = [{
    model: 'test',
    totalCalls: 100,
    failedCalls: 30,
    errorRate: 0.3,
    sizeDistribution: {},
  }];
  const previous = [{
    model: 'test',
    totalCalls: 100,
    failedCalls: 10,
    errorRate: 0.1,
    sizeDistribution: {},
  }];

  const changes = detectErrorRateChange(current, previous, { threshold: 0.1 });

  assert.equal(changes.length, 1);
  assert.equal(changes[0].type, 'error_rate_change');
  assert.equal(changes[0].direction, 'worse');
  assert.ok(Math.abs(changes[0].diff - 0.2) < 0.001);
});

test('detectErrorRateChange: не срабатывает при малых изменениях', () => {
  const current = [{
    model: 'test',
    totalCalls: 100,
    failedCalls: 11,
    errorRate: 0.11,
    sizeDistribution: {},
  }];
  const previous = [{
    model: 'test',
    totalCalls: 100,
    failedCalls: 10,
    errorRate: 0.1,
    sizeDistribution: {},
  }];

  const changes = detectErrorRateChange(current, previous, { threshold: 0.1 });

  assert.equal(changes.length, 0);
});

test('detectErrorRateChange: обнаруживает новую модель', () => {
  const current = [{
    model: 'new-model',
    totalCalls: 50,
    failedCalls: 5,
    errorRate: 0.1,
    sizeDistribution: {},
  }];
  const previous = [];

  const changes = detectErrorRateChange(current, previous, { threshold: 0.1 });

  assert.equal(changes.length, 1);
  assert.equal(changes[0].type, 'new_model');
  assert.equal(changes[0].model, 'new-model');
});

test('detectErrorRateChange: игнорирует модели с малым количеством вызовов', () => {
  const current = [{
    model: 'rare-model',
    totalCalls: 5, // Мало вызовов
    failedCalls: 3,
    errorRate: 0.6,
    sizeDistribution: {},
  }];
  const previous = [];

  const changes = detectErrorRateChange(current, previous, { threshold: 0.1 });

  assert.equal(changes.length, 0);
});

test('detectErrorRateChange: обнаруживает изменение распределения по размерам', () => {
  const current = [{
    model: 'test',
    totalCalls: 100,
    failedCalls: 10,
    errorRate: 0.1,
    sizeDistribution: {
      small: { calls: 50, failures: 2 }, // 4% ошибок
      medium: { calls: 50, failures: 8 }, // 16% ошибок
      large: { calls: 0, failures: 0 },
      xlarge: { calls: 0, failures: 0 },
    },
  }];
  const previous = [{
    model: 'test',
    totalCalls: 100,
    failedCalls: 10,
    errorRate: 0.1,
    sizeDistribution: {
      small: { calls: 50, failures: 8 }, // 16% ошибок
      medium: { calls: 50, failures: 2 }, // 4% ошибок
      large: { calls: 0, failures: 0 },
      xlarge: { calls: 0, failures: 0 },
    },
  }];

  const changes = detectErrorRateChange(current, previous, { threshold: 0.1 });

  // Должны обнаружиться изменения в small и medium бакетах
  const sizeChanges = changes.filter(c => c.type === 'size_distribution_change');
  assert.ok(sizeChanges.length >= 2);
  assert.ok(sizeChanges.some(c => c.bucket === 'small' && c.direction === 'better'));
  assert.ok(sizeChanges.some(c => c.bucket === 'medium' && c.direction === 'worse'));
});

test('detectErrorRateChange: игнорирует бакеты с малым количеством вызовов', () => {
  const current = [{
    model: 'test',
    totalCalls: 100,
    failedCalls: 10,
    errorRate: 0.1,
    sizeDistribution: {
      small: { calls: 3, failures: 3 }, // 100% ошибок, но мало вызовов
      medium: { calls: 97, failures: 7 },
      large: { calls: 0, failures: 0 },
      xlarge: { calls: 0, failures: 0 },
    },
  }];
  const previous = [{
    model: 'test',
    totalCalls: 100,
    failedCalls: 10,
    errorRate: 0.1,
    sizeDistribution: {
      small: { calls: 0, failures: 0 },
      medium: { calls: 100, failures: 10 },
      large: { calls: 0, failures: 0 },
      xlarge: { calls: 0, failures: 0 },
    },
  }];

  const changes = detectErrorRateChange(current, previous, { threshold: 0.1 });

  // small бакет игнорируется (3 < 5 вызовов)
  const sizeChanges = changes.filter(c => c.type === 'size_distribution_change' && c.bucket === 'small');
  assert.equal(sizeChanges.length, 0);
});

// ── writeErrorMetrics / readPreviousErrorMetrics ─────────────────────────────

test('writeErrorMetrics: записывает метрики в историю', async () => {
  const db = createFakeDb();
  const metrics = [
    {
      model: 'test-model',
      totalCalls: 100,
      failedCalls: 10,
      errorRate: 0.1,
      avgEstimatedTokens: 5000,
      sizeDistribution: { small: { calls: 50, failures: 5 }, medium: { calls: 50, failures: 5 } },
    },
  ];

  const written = await writeErrorMetrics(db, metrics);

  assert.equal(written, 1);
  assert.equal(db.tables.error_metrics_history.length, 1);
  assert.equal(db.tables.error_metrics_history[0].model_id, 'test-model');
  assert.equal(db.tables.error_metrics_history[0].total_calls, 100);
});

test('readPreviousErrorMetrics: читает предыдущие метрики', async () => {
  const db = createFakeDb();
  const now = Date.now();

  // Записываем метрики
  await writeErrorMetrics(db, [
    { model: 'a', totalCalls: 10, failedCalls: 1, errorRate: 0.1 },
    { model: 'b', totalCalls: 20, failedCalls: 2, errorRate: 0.1 },
  ], { now: now - 1000 });

  // Читаем предыдущие
  const previous = await readPreviousErrorMetrics(db, { before: now });

  assert.equal(previous.length, 2);
  assert.ok(previous.some(p => p.model === 'a'));
  assert.ok(previous.some(p => p.model === 'b'));
});

// ── monitorErrorRates (интеграция) ───────────────────────────────────────────

test('monitorErrorRates: полный цикл сбора и детекта изменений', async () => {
  const now = Date.now();
  const calls = [
    ...generateCalls({ model: 'test-model', count: 100, failures: 20, avgTokens: 5000, ts: now }),
  ];
  const db = createFakeDb(calls);

  // Первый запуск (нет предыдущих метрик)
  const result1 = await monitorErrorRates(db, {
    since: now - 1000,
    until: now + 1000,
    write: true,
  });

  assert.equal(result1.models, 1);
  assert.equal(result1.totalCalls, 100);
  assert.equal(result1.totalFailures, 20);
  assert.equal(result1.changes.length, 1); // new_model
  assert.equal(result1.changes[0].type, 'new_model');

  // Второй запуск через час (предыдущие метрики есть)
  const later = now + 3600 * 1000;
  const calls2 = [
    ...generateCalls({ model: 'test-model', count: 100, failures: 30, avgTokens: 5000, ts: later }),
  ];
  db.tables.ladder_calls = calls2;

  const result2 = await monitorErrorRates(db, {
    since: later - 1000,
    until: later + 1000,
    write: true,
  });

  // Теперь должно обнаружиться изменение частоты ошибок (с 20% до 30%)
  const errorRateChanges = result2.changes.filter(c => c.type === 'error_rate_change');
  assert.ok(errorRateChanges.length >= 1, `expected error_rate_change, got: ${JSON.stringify(result2.changes)}`);
  assert.equal(errorRateChanges[0].direction, 'worse');
});

// ── Экспорт SIZE_BUCKETS ─────────────────────────────────────────────────────

test('SIZE_BUCKETS: экспортируется для использования в других модулях', () => {
  assert.ok(Array.isArray(SIZE_BUCKETS));
  assert.ok(SIZE_BUCKETS.every(b => b.name && (Number.isFinite(b.max) || b.max === Infinity)));
});
