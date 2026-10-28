#!/usr/bin/env node
// Песочница для тестирования логики мониторинга (issue #335).
//
// Layer A (≈1–3 с): эфемерная локальная среда с фейковыми провайдерами
//   - Фейковый D1 с заданными данными
//   - Фейковые HTTP серверы для провайдеров
//   - Тестирование collectErrorMetrics и detectErrorRateChange
//   - Тестирование записи catalog_changes при изменениях
//
// Layer B (опционально): интеграционный тест с реальным D1 (если доступен)
//   - Проверка записи/чтения метрик
//   - Проверка детекта изменений каталога
//
// Запуск:
//   npm run test:sandbox:monitoring              оба слоя
//   npm run test:sandbox:monitoring -- --fast    только Layer A
//   SANDBOX_LIVE=0 npm run test:sandbox:monitoring   то же, что --fast
//
// Уровень: S5 — скрипт сам поднимает эфемерную среду (фейковый D1, фейковые
// провайдеры) и гоняет сценарий end-to-end за секунды на фикстурах.

import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';

const require = createRequire(import.meta.url);
const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.resolve(HERE, '..', '..');

const FAST = process.argv.includes('--fast') || process.env.SANDBOX_LIVE === '0';

const failures = [];
const ROOT = fs.mkdtempSync(path.join(os.tmpdir(), 'monitoring-sandbox-'));

const log = (...a) => console.log('[sandbox]', ...a);
function check(cond, msg) {
  if (cond) console.log('   ok  -', msg);
  else { failures.push(msg); console.log('   FAIL-', msg); }
}

// ── Фейковый D1 для тестирования ─────────────────────────────────────────────

function createFakeDb(ladderCalls = []) {
  return {
    tables: {
      ladder_calls: ladderCalls,
      error_metrics_history: [],
      catalog_changes: [],
      free_models: [],
    },
    prepare(sql) {
      const db = this;
      const stmt = {
        _params: [],
        bind(...params) { stmt._params = params; return stmt; },
        async all() {
          if (sql.includes('FROM ladder_calls')) {
            return { results: db.queryLadderCalls(stmt._params[0], stmt._params[1]) };
          }
          if (sql.includes('FROM error_metrics_history')) {
            return { results: db.queryErrorMetrics(stmt._params[0], stmt._params[1]) };
          }
          if (sql.includes('FROM catalog_changes')) {
            return { results: db.tables.catalog_changes.slice() };
          }
          return { results: [] };
        },
        async run() {
          if (sql.includes('INSERT INTO error_metrics_history')) {
            db.tables.error_metrics_history.push({
              collected_at: stmt._params[0],
              period_start: stmt._params[1],
              period_end: stmt._params[2],
              model_id: stmt._params[3],
              total_calls: stmt._params[4],
              failed_calls: stmt._params[5],
              error_rate: stmt._params[6],
              avg_estimated_tokens: stmt._params[7],
              size_distribution: stmt._params[8],
              raw_metrics: stmt._params[9],
            });
            return { success: true };
          }
          if (sql.includes('INSERT INTO catalog_changes')) {
            db.tables.catalog_changes.push({
              detected_at: stmt._params[0],
              provider: stmt._params[1],
              change_type: stmt._params[2],
              model_id: stmt._params[3],
              details: stmt._params[4],
              benchmark_triggered: stmt._params[5],
            });
            return { success: true };
          }
          return { success: true };
        },
      };
      return stmt;
    },
    queryLadderCalls(since, until) {
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
          const bucket = call.estimated_tokens_in <= 4096 ? 'small' :
                         call.estimated_tokens_in <= 16384 ? 'medium' :
                         call.estimated_tokens_in <= 65536 ? 'large' : 'xlarge';
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
function generateCalls({ model, count, failures, avgTokens, ts }) {
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

// ── Layer A: тестирование мониторинга ошибок ────────────────────────────────

async function layerA() {
  log('Layer A (эфемерная среда: фейковый D1 + фикстуры)');

  // Импорт модулей
  const { collectErrorMetrics, detectErrorRateChange, monitorErrorRates } = require(
    path.join(REPO, 'src', 'error-monitor.js')
  );

  log('-- SC1: collectErrorMetrics собирает метрики по моделям');
  const now = Date.now();
  const calls1 = [
    ...generateCalls({ model: 'opencode-go/model-a-free', count: 100, failures: 10, avgTokens: 2000, ts: now }),
    ...generateCalls({ model: 'opencode-go/model-b-free', count: 50, failures: 5, avgTokens: 10000, ts: now }),
  ];
  const db1 = createFakeDb(calls1);
  const metrics1 = await collectErrorMetrics(db1, { since: now - 1000, until: now + 1000 });

  check(metrics1.length === 2, `SC1: две модели в метриках (получено ${metrics1.length})`);
  const modelA = metrics1.find(m => m.model === 'opencode-go/model-a-free');
  check(modelA && modelA.totalCalls === 100, `SC1: model-a 100 вызовов (получено ${modelA?.totalCalls})`);
  check(modelA && Math.abs(modelA.errorRate - 0.1) < 0.001, `SC1: model-a 10% ошибок (получено ${modelA?.errorRate})`);

  log('-- SC2: detectErrorRateChange обнаруживает изменение частоты ошибок');
  const current2 = [{
    model: 'test-model',
    totalCalls: 100,
    failedCalls: 30,
    errorRate: 0.3,
    sizeDistribution: { small: { calls: 50, failures: 15 }, medium: { calls: 50, failures: 15 } },
  }];
  const previous2 = [{
    model: 'test-model',
    totalCalls: 100,
    failedCalls: 10,
    errorRate: 0.1,
    sizeDistribution: { small: { calls: 50, failures: 5 }, medium: { calls: 50, failures: 5 } },
  }];
  const changes2 = detectErrorRateChange(current2, previous2, { threshold: 0.1 });
  check(changes2.length >= 1, `SC2: изменения обнаружены (получено ${changes2.length})`);
  check(changes2.some(c => c.type === 'error_rate_change'), 'SC2: тип error_rate_change');
  check(changes2.some(c => c.direction === 'worse'), 'SC2: направление worse');

  log('-- SC3: monitorErrorRates — полный цикл');
  const db3 = createFakeDb(generateCalls({ model: 'test-model', count: 100, failures: 20, avgTokens: 5000, ts: now }));
  const result1 = await monitorErrorRates(db3, { since: now - 1000, until: now + 1000, write: true });
  check(result1.models === 1, `SC3: одна модель (получено ${result1.models})`);
  check(result1.changes.length === 1 && result1.changes[0].type === 'new_model', 'SC3: первая модель — new_model');

  // Второй запуск через час
  const later = now + 3600 * 1000;
  db3.tables.ladder_calls = generateCalls({ model: 'test-model', count: 100, failures: 30, avgTokens: 5000, ts: later });
  const result2 = await monitorErrorRates(db3, { since: later - 1000, until: later + 1000, write: true });
  const errorRateChanges = result2.changes.filter(c => c.type === 'error_rate_change');
  check(errorRateChanges.length >= 1, `SC3: изменение частоты ошибок обнаружено (получено ${errorRateChanges.length})`);
  check(db3.tables.error_metrics_history.length === 2, `SC3: метрики записаны в историю (получено ${db3.tables.error_metrics_history.length})`);

  log('-- SC4: детект распределения по размерам запросов');
  const current4 = [{
    model: 'test-model',
    totalCalls: 100,
    failedCalls: 10,
    errorRate: 0.1,
    sizeDistribution: {
      small: { calls: 50, failures: 2 },   // 4% ошибок
      medium: { calls: 50, failures: 8 },  // 16% ошибок
    },
  }];
  const previous4 = [{
    model: 'test-model',
    totalCalls: 100,
    failedCalls: 10,
    errorRate: 0.1,
    sizeDistribution: {
      small: { calls: 50, failures: 8 },   // 16% ошибок
      medium: { calls: 50, failures: 2 },  // 4% ошибок
    },
  }];
  const changes4 = detectErrorRateChange(current4, previous4, { threshold: 0.1 });
  const sizeChanges = changes4.filter(c => c.type === 'size_distribution_change');
  check(sizeChanges.length >= 2, `SC4: изменения распределения обнаружены (получено ${sizeChanges.length})`);
  check(sizeChanges.some(c => c.bucket === 'small' && c.direction === 'better'), 'SC4: small bucket улучшился');
  check(sizeChanges.some(c => c.bucket === 'medium' && c.direction === 'worse'), 'SC4: medium bucket ухудшился');

  log('-- SC5: запись catalog_changes при изменениях каталога');
  const { collectFreeModels } = require(path.join(REPO, 'src', 'free-models.js'));

  // Фейковый fetch для провайдеров
  const fakeFetch = async (url) => {
    if (url.includes('opencode.ai/zen/go/v1/models')) {
      return new Response(JSON.stringify({
        data: [
          { id: 'new-model-free', name: 'New Model' },
          { id: 'existing-model-free', name: 'Existing Model' },
        ],
      }), { status: 200 });
    }
    if (url.includes('openrouter.ai/api/v1/models')) {
      return new Response(JSON.stringify({
        data: [
          { id: 'test-model:free', name: 'Test', pricing: { prompt: '0', completion: '0' } },
        ],
      }), { status: 200 });
    }
    if (url.includes('opencode.ai/zen/v1/models')) {
      return new Response(JSON.stringify({ data: [] }), { status: 200 });
    }
    return new Response('{}', { status: 200 });
  };

  const db5 = createFakeDb();
  // Предзаполняем существующую модель
  db5.tables.free_models = [
    { provider: 'opencode-go', model_id: 'opencode-go/existing-model-free', name: 'Existing Model' },
  ];

  const run5 = await collectFreeModels(
    { OPENCODE_GO_API_KEYS: 'test-key' },
    db5,
    { fetchImpl: fakeFetch, probe: false, write: true, now }
  );

  check(run5.catalog_changes > 0, `SC5: изменения каталога записаны (получено ${run5.catalog_changes})`);
  check(db5.tables.catalog_changes.length > 0, 'SC5: catalog_changes не пустая');
  const appeared = db5.tables.catalog_changes.filter(c => c.change_type === 'appeared');
  check(appeared.length >= 1, `SC5: есть появившиеся модели (получено ${appeared.length})`);
}

// ── Layer B: интеграционный тест с реальным D1 ───────────────────────────────

async function layerB() {
  log('Layer B (интеграционный тест)');
  if (FAST) {
    log('SKIP: --fast');
    return 'skipped: --fast';
  }

  // Проверяем наличие токена Cloudflare
  const token = process.env.CLOUDFLARE_API_TOKEN;
  const account = process.env.CLOUDFLARE_ACCOUNT_ID;
  if (!token || !account) {
    log('SKIP: нет CLOUDFLARE_API_TOKEN или CLOUDFLARE_ACCOUNT_ID');
    return 'skipped: no credentials';
  }

  // TODO: Реализовать интеграционный тест с реальным D1
  // Проверка записи/чтения метрик
  // Проверка детекта изменений каталога

  return 'skipped: not implemented';
}

// ── main ─────────────────────────────────────────────────────────────────────

async function main() {
  log('Песочница мониторинга автоматизации (issue #335)');
  log('цель: S5 — замкнутый цикл сценария за секунды на фикстурах' + (FAST ? ' (живой слой выключен: --fast)' : ''));

  const startedA = Date.now();
  await layerA();
  const secsA = ((Date.now() - startedA) / 1000).toFixed(1);

  let liveState = 'skipped: layer A failed';
  let secsB = '0.0';
  if (!failures.length && !FAST) {
    const startedB = Date.now();
    liveState = await layerB();
    secsB = ((Date.now() - startedB) / 1000).toFixed(1);
  } else if (FAST) {
    liveState = 'skipped: --fast';
  }

  const level = liveState === 'skipped: not implemented' || liveState === 'skipped: no credentials' || liveState === 'skipped: --fast'
    ? `S5 (эфемерная среда на фикстурах); живой слой: ${liveState}`
    : `S5 (эфемерная среда на фикстурах) + интеграционный тест`;

  console.log('');
  log(`уровень: ${level}`);
  log(`время цикла: Layer A ${secsA}s, живой слой ${secsB}s`);
  if (failures.length) { log(`провалено проверок: ${failures.length}`); log('RESULT: FAIL'); return 1; }
  log('RESULT: PASS');
  return 0;
}

main().then(code => {
  try { fs.rmSync(ROOT, { recursive: true, force: true }); } catch {}
  process.exit(code);
}).catch(e => {
  console.error('[sandbox] error:', (e && e.stack) || e);
  try { fs.rmSync(ROOT, { recursive: true, force: true }); } catch {}
  console.log('[sandbox] RESULT: FAIL');
  process.exit(1);
});
