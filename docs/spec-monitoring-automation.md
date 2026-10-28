# Спецификация: Автоматизация мониторинга частоты ошибок и изменений в списке бесплатных моделей

## Контекст (issue #335)

После PR #333 (контекстный пре-фильтр по реальным потолкам моделей + estimated_tokens_in в трассе) и PR #334 (идемпотентная миграция 0017 для колонки в D1) требуется настроить автоматизацию мониторинга.

**Основные требования**:
1. Отслеживать частоту ошибок для каждой модели
2. Обнаруживать изменения в списке бесплатных Go моделей
3. При изменении списка автоматически запускать замеры
4. Реализовать механизм тестирования на изолированной среде
5. Обеспечить песочницу для проверки логики мониторинга

## Текущее состояние

### Существующая инфраструктура

1. **Сбор инвентаря бесплатных моделей** (`collect-free-models.yml`):
   - Запускается каждые 4 часа
   - Собирает каталоги от всех провайдеров (opencode-go, opencode-zen, openrouter)
   - Записывает изменения в D1 таблицу `free_models`
   - Уже отслеживает появление/исчезновение/изменение моделей

2. **Мониторинг живого воркера** (`ladder-watch.yml`):
   - Запускается каждые 5 минут
   - Проверяет `/health`, делает тестовые вызовы
   - Отправляет уведомления в Telegram при изменении состояния

3. **Трассировка вызовов** (`ladder_calls` таблица в D1):
   - Записывает каждый вызов с метаданными (trace_id, model, ok, ms, attempts)
   - Содержит estimated_tokens_in (добавлено в PR #333)
   - Уже собирает статистику ошибок

4. **Analytics скрипт** (`analytics.py`):
   - Генерирует отчеты по использованию
   - Уже считает частоту ошибок по моделям

### Пробелы

1. **Нет автоматического обнаружения изменений в окне моделей**:
   - PR #333 добавил контекстный пре-фильтр, но нет мониторинга его эффективности
   - Не отслеживается, изменилось ли распределение ошибок по размерам запросов

2. **Нет автоматического запуска замеров при изменениях**:
   - Список бесплатных моделей меняется, но замеры не перезапускаются автоматически
   - Нужен триггер: "список изменился → запустить бенчмарк"

3. **Нет staging окружения для тестирования логики мониторинга**:
   - Тесты используют фейковые провайдеры, но нет промежуточной среды

## Архитектура решения

### 1. Мониторинг частоты ошибок по моделям

**Новый модуль**: `src/error-monitor.js`

```javascript
// Мониторинг ошибок по моделям и размерам запросов
export async function collectErrorMetrics(db, { since = 24 * 3600 * 1000 } = {}) {
  // 1. Ошибки по моделям за период
  // 2. Распределение ошибок по размерам запросов (estimated_tokens_in)
  // 3. Сравнение с предыдущим периодом (детект изменений)
  // 4. Возврат: { models: [...], sizeDistribution: [...], changes: [...] }
}

export function detectWindowChange(current, previous) {
  // Статистический тест на изменение распределения
  // Хи-квадрат или KL-дивергенция
  // Пороговые значения для алертинга
}
```

**D1 таблица**: `error_metrics_history`
```sql
CREATE TABLE error_metrics_history (
  id INTEGER PRIMARY KEY,
  collected_at INTEGER NOT NULL,
  period_start INTEGER NOT NULL,
  period_end INTEGER NOT NULL,
  model_id TEXT NOT NULL,
  total_calls INTEGER NOT NULL,
  failed_calls INTEGER NOT NULL,
  error_rate REAL NOT NULL,
  size_bucket TEXT NOT NULL, -- 'small', 'medium', 'large', 'xlarge'
  estimated_tokens_in_avg INTEGER,
  raw_metrics TEXT NOT NULL -- JSON с деталями
);
```

### 2. Мониторинг изменений в списке бесплатных Go моделей

**Улучшение существующего скрипта** `collect-free-models.mjs`:

```javascript
// Добавить в collectFreeModels():
if (diff.appeared.length > 0 || diff.gone.length > 0) {
  // Автоматический запуск бенчмарка для новых моделей
  await triggerBenchmarkForChanges(diff);
  
  // Запись события об изменении
  await recordCatalogChange(db, diff);
}
```

**Новая D1 таблица**: `catalog_changes`
```sql
CREATE TABLE catalog_changes (
  id INTEGER PRIMARY KEY,
  detected_at INTEGER NOT NULL,
  provider TEXT NOT NULL,
  change_type TEXT NOT NULL, -- 'appeared', 'gone', 'changed'
  model_id TEXT NOT NULL,
  details TEXT, -- JSON с деталями изменения
  benchmark_triggered INTEGER DEFAULT 0
);
```

### 3. Автоматический запуск замеров

**Новый workflow**: `.github/workflows/auto-benchmark.yml`

```yaml
name: auto-benchmark-on-change
on:
  workflow_dispatch:
    inputs:
      force:
        description: "Force benchmark even if no changes detected"
        default: "false"
  
jobs:
  detect-and-benchmark:
    runs-on: ubuntu-latest
    steps:
      - name: Check for catalog changes
        id: check
        run: |
          # Запускаем сбор и проверяем изменения
          node scripts/collect-free-models.mjs --check-changes
          
      - name: Run benchmarks if changes detected
        if: steps.check.outputs.changes_detected == 'true'
        run: |
          # Запускаем бенчмарк только для изменившихся моделей
          node scripts/collect-free-models.mjs --benchmark-changed
```

### 4. Staging окружение

**Подход**: Изолированная D1 база + отдельный воркер

```javascript
// src/staging-adapter.js
export function createStagingEnv(productionEnv) {
  return {
    ...productionEnv,
    LADDER_TRACE_DB: createStagingDb(),
    // Изолированные ключи для тестовых вызовов
    OPENROUTER_API_KEY: process.env.STAGING_OPENROUTER_KEY,
    // Отключены реальные уведомления
    TELEGRAM_BOT_TOKEN: null,
  };
}
```

**Sandbox тест**: `scripts/sandbox/monitoring-sandbox.mjs`

```javascript
// Сценарий тестирования мониторинга:
// 1. Поднимаем фейковые провайдеры с заданным распределением ошибок
// 2. Запускаем collectErrorMetrics
// 3. Проверяем детект изменений
// 4. Проверяем автоматический запуск бенчмарка
// 5. Проверяем запись в D1
```

## Детали реализации

### Шаг 1: Модуль мониторинга ошибок

**Файл**: `src/error-monitor.js`

```javascript
// Мониторинг ошибок по моделям с учетом размеров запросов.
// Решает проблему PR #333: "контекстный пре-фильтр по реальным потолкам моделей"

import { estimateTokens } from './size-policy.js';

// Бакеты по размеру запроса (в tokens)
export const SIZE_BUCKETS = [
  { name: 'small', max: 4096 },
  { name: 'medium', max: 16384 },
  { name: 'large', max: 65536 },
  { name: 'xlarge', max: Infinity },
];

// Сбор метрик за период
export async function collectErrorMetrics(db, { 
  since = Date.now() - 24 * 3600 * 1000,
  until = Date.now(),
} = {}) {
  const { results } = await db.prepare(`
    SELECT 
      model,
      COUNT(*) as total_calls,
      SUM(CASE WHEN ok = 0 THEN 1 ELSE 0 END) as failed_calls,
      AVG(estimated_tokens_in) as avg_estimated_tokens,
      -- Распределение по размерам
      SUM(CASE WHEN estimated_tokens_in <= 4096 THEN 1 ELSE 0 END) as small_calls,
      SUM(CASE WHEN estimated_tokens_in > 4096 AND estimated_tokens_in <= 16384 THEN 1 ELSE 0 END) as medium_calls,
      SUM(CASE WHEN estimated_tokens_in > 16384 AND estimated_tokens_in <= 65536 THEN 1 ELSE 0 END) as large_calls,
      SUM(CASE WHEN estimated_tokens_in > 65536 THEN 1 ELSE 0 END) as xlarge_calls,
      -- Ошибки по размерам
      SUM(CASE WHEN ok = 0 AND estimated_tokens_in <= 4096 THEN 1 ELSE 0 END) as small_failures,
      SUM(CASE WHEN ok = 0 AND estimated_tokens_in > 4096 AND estimated_tokens_in <= 16384 THEN 1 ELSE 0 END) as medium_failures,
      SUM(CASE WHEN ok = 0 AND estimated_tokens_in > 16384 AND estimated_tokens_in <= 65536 THEN 1 ELSE 0 END) as large_failures,
      SUM(CASE WHEN ok = 0 AND estimated_tokens_in > 65536 THEN 1 ELSE 0 END) as xlarge_failures
    FROM ladder_calls
    WHERE ts >= ?1 AND ts <= ?2
    GROUP BY model
    ORDER BY total_calls DESC
  `).bind(since, until).all();
  
  return results.map(row => ({
    model: row.model,
    totalCalls: row.total_calls,
    failedCalls: row.failed_calls,
    errorRate: row.total_calls > 0 ? row.failed_calls / row.total_calls : 0,
    avgEstimatedTokens: row.avg_estimated_tokens,
    sizeDistribution: {
      small: { calls: row.small_calls, failures: row.small_failures },
      medium: { calls: row.medium_calls, failures: row.medium_failures },
      large: { calls: row.large_calls, failures: row.large_failures },
      xlarge: { calls: row.xlarge_calls, failures: row.xlarge_failures },
    },
  }));
}

// Детект изменений в распределении ошибок
export function detectErrorRateChange(current, previous, { threshold = 0.1 } = {}) {
  const changes = [];
  
  for (const curr of current) {
    const prev = previous.find(p => p.model === curr.model);
    if (!prev) {
      // Новая модель
      changes.push({
        model: curr.model,
        type: 'new_model',
        currentErrorRate: curr.errorRate,
      });
      continue;
    }
    
    // Проверка изменения общей частоты ошибок
    const errorRateDiff = Math.abs(curr.errorRate - prev.errorRate);
    if (errorRateDiff > threshold) {
      changes.push({
        model: curr.model,
        type: 'error_rate_change',
        previousErrorRate: prev.errorRate,
        currentErrorRate: curr.errorRate,
        diff: errorRateDiff,
      });
    }
    
    // Проверка изменения распределения по размерам
    for (const bucket of ['small', 'medium', 'large', 'xlarge']) {
      const currRate = curr.sizeDistribution[bucket].calls > 0 
        ? curr.sizeDistribution[bucket].failures / curr.sizeDistribution[bucket].calls 
        : 0;
      const prevRate = prev.sizeDistribution[bucket].calls > 0
        ? prev.sizeDistribution[bucket].failures / prev.sizeDistribution[bucket].calls
        : 0;
      
      if (Math.abs(currRate - prevRate) > threshold) {
        changes.push({
          model: curr.model,
          type: 'size_distribution_change',
          bucket,
          previousRate: prevRate,
          currentRate: currRate,
        });
      }
    }
  }
  
  return changes;
}
```

### Шаг 2: Интеграция с collect-free-models

**Изменения в `src/free-models.js`**:

```javascript
// Добавить в collectFreeModels() после вычисления diff:
const catalogChange = {
  appeared: diff.appeared.length,
  gone: diff.gone.length,
  changed: diff.changed.length,
};

if (catalogChange.appeared > 0 || catalogChange.gone > 0) {
  // Автоматический запуск бенчмарка для новых моделей
  const newGoModels = diff.appeared.filter(r => 
    r.provider === 'opencode-go' && r.model_id.endsWith('-free')
  );
  
  if (newGoModels.length > 0 && write) {
    for (const row of newGoModels) {
      const result = await benchmarkGoModel(row, env, { fetchImpl, now });
      if (result) {
        await writeGoBenchmark(db, result);
        benchmarks.push(result);
      }
    }
  }
  
  // Запись события в catalog_changes
  if (write) {
    await recordCatalogChanges(db, diff, now);
  }
}
```

### Шаг 3: D1 миграция

**Файл**: `schema/migrations/0018_monitoring_automation.sql`

```sql
-- 0018: Мониторинг автоматизации (issue #335)
--
-- 1. История метрик ошибок для отслеживания трендов
-- 2. Журнал изменений каталога для автоматического запуска замеров

-- История метрик ошибок
CREATE TABLE IF NOT EXISTS error_metrics_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  collected_at INTEGER NOT NULL,
  period_start INTEGER NOT NULL,
  period_end INTEGER NOT NULL,
  model_id TEXT NOT NULL,
  total_calls INTEGER NOT NULL,
  failed_calls INTEGER NOT NULL,
  error_rate REAL NOT NULL,
  avg_estimated_tokens INTEGER,
  size_distribution TEXT NOT NULL, -- JSON
  raw_metrics TEXT NOT NULL -- JSON с деталями
);

CREATE INDEX IF NOT EXISTS idx_error_metrics_model ON error_metrics_history (model_id, collected_at);
CREATE INDEX IF NOT EXISTS idx_error_metrics_collected ON error_metrics_history (collected_at);

-- Журнал изменений каталога
CREATE TABLE IF NOT EXISTS catalog_changes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  detected_at INTEGER NOT NULL,
  provider TEXT NOT NULL,
  change_type TEXT NOT NULL, -- 'appeared', 'gone', 'changed'
  model_id TEXT NOT NULL,
  details TEXT, -- JSON с деталями изменения
  benchmark_triggered INTEGER DEFAULT 0,
  benchmark_result TEXT -- JSON с результатом бенчмарка
);

CREATE INDEX IF NOT EXISTS idx_catalog_changes_detected ON catalog_changes (detected_at);
CREATE INDEX IF NOT EXISTS idx_catalog_changes_model ON catalog_changes (model_id);
```

### Шаг 4: Sandbox тестирование

**Файл**: `scripts/sandbox/monitoring-sandbox.mjs`

```javascript
#!/usr/bin/env node
// Песочница для тестирования логики мониторинга (issue #335).
//
// Layer A: Эфемерная среда с фейковыми провайдерами
//   - Фейковый D1 с заданными данными
//   - Фейковые HTTP серверы для провайдеров
//   - Тестирование collectErrorMetrics и detectErrorRateChange
//
// Layer B: Интеграционный тест с реальным D1 (если доступен)
//   - Проверка записи/чтения метрик
//   - Проверка детекта изменений каталога

import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';

const require = createRequire(import.meta.url);
const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.resolve(HERE, '..', '..');

const failures = [];
const ROOT = fs.mkdtempSync(path.join(os.tmpdir(), 'monitoring-sandbox-'));

const log = (...a) => console.log('[sandbox]', ...a);
function check(cond, msg) {
  if (cond) console.log('   ok  -', msg);
  else { failures.push(msg); console.log('   FAIL-', msg); }
}

// Фейковый D1 для тестирования
class FakeD1 {
  constructor() {
    this.tables = {
      ladder_calls: [],
      error_metrics_history: [],
      catalog_changes: [],
      free_models: [],
    };
  }
  
  prepare(sql) {
    const db = this;
    return {
      bind(...params) {
        return {
          async all() {
            // Простая имитация SQL запросов для тестов
            if (sql.includes('FROM ladder_calls')) {
              return { results: db.generateLadderCalls(params[0], params[1]) };
            }
            return { results: [] };
          },
          async run() {
            return { success: true };
          },
        };
      },
    };
  }
  
  generateLadderCalls(since, until) {
    // Генерация тестовых данных
    return [
      {
        model: 'opencode-go/test-model-free',
        total_calls: 100,
        failed_calls: 10,
        avg_estimated_tokens: 5000,
        small_calls: 60, medium_calls: 30, large_calls: 10, xlarge_calls: 0,
        small_failures: 5, medium_failures: 3, large_failures: 2, xlarge_failures: 0,
      },
    ];
  }
}

async function main() {
  log('Песочница мониторинга автоматизации (issue #335)');
  log('цель: S5 — замкнутый цикл сценария за секунды на фикстурах');
  
  // Импорт модулей
  const { collectErrorMetrics, detectErrorRateChange } = require(
    path.join(REPO, 'src', 'error-monitor.js')
  );
  
  // Layer A: тестирование collectErrorMetrics
  log('-- Layer A: collectErrorMetrics');
  const db = new FakeD1();
  const metrics = await collectErrorMetrics(db, {
    since: Date.now() - 24 * 3600 * 1000,
    until: Date.now(),
  });
  
  check(metrics.length > 0, 'метрики собраны');
  check(metrics[0].model === 'opencode-go/test-model-free', 'модель определена');
  check(metrics[0].totalCalls === 100, 'общее количество вызовов');
  check(metrics[0].failedCalls === 10, 'количество ошибок');
  check(Math.abs(metrics[0].errorRate - 0.1) < 0.001, 'частота ошибок = 10%');
  
  // Layer A: тестирование detectErrorRateChange
  log('-- Layer A: detectErrorRateChange');
  const previous = [{
    model: 'opencode-go/test-model-free',
    totalCalls: 100,
    failedCalls: 20,
    errorRate: 0.2,
    sizeDistribution: {
      small: { calls: 60, failures: 10 },
      medium: { calls: 30, failures: 6 },
      large: { calls: 10, failures: 4 },
      xlarge: { calls: 0, failures: 0 },
    },
  }];
  
  const changes = detectErrorRateChange(metrics, previous, { threshold: 0.05 });
  check(changes.length > 0, 'изменения обнаружены');
  check(changes.some(c => c.type === 'error_rate_change'), 'изменение частоты ошибок');
  
  // Итог
  console.log('');
  log('уровень: S5 (эфемерная среда на фикстурах)');
  if (failures.length) { 
    log(`провалено проверок: ${failures.length}`); 
    log('RESULT: FAIL'); 
    return 1; 
  }
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
```

### Шаг 5: Workflow для автоматических замеров

**Файл**: `.github/workflows/auto-benchmark.yml`

```yaml
name: auto-benchmark-on-catalog-change
on:
  workflow_dispatch:
    inputs:
      force:
        description: "Force benchmark even if no changes detected"
        default: "false"
      dry_run:
        description: "Collect + diff only, do not write"
        default: "false"
  
jobs:
  detect-and-benchmark:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: 22
          
      - name: Detect catalog changes
        id: detect
        run: |
          set -euo pipefail
          node scripts/collect-free-models.mjs \
            --probe false \
            --dry-run true \
            --json > catalog-diff.json
            
          # Проверяем изменения
          APPEARED=$(jq '.diff.appeared | length' catalog-diff.json)
          GONE=$(jq '.diff.gone | length' catalog-diff.json)
          
          if [ "$APPEARED" -gt 0 ] || [ "$GONE" -gt 0 ]; then
            echo "changes_detected=true" >> "$GITHUB_OUTPUT"
            echo "Changes detected: appeared=$APPEARED, gone=$GONE"
          else
            echo "changes_detected=false" >> "$GITHUB_OUTPUT"
            echo "No changes detected"
          fi
          
      - name: Run benchmarks for changed models
        if: steps.detect.outputs.changes_detected == 'true' || github.event.inputs.force == 'true'
        run: |
          set -euo pipefail
          node scripts/collect-free-models.mjs \
            --probe true \
            --probe-limit 20 \
            --benchmark-changed true \
            --dry-run "${{ github.event.inputs.dry_run || 'false' }}" \
            --out benchmark-results.md | tee benchmark.log
            
          printf '\n%s\n' "$(cat benchmark-results.md)" >> "$GITHUB_STEP_SUMMARY"
          
      - uses: actions/upload-artifact@v4
        if: always()
        with:
          name: benchmark-results
          path: |
            catalog-diff.json
            benchmark-results.md
            benchmark.log
          if-no-files-found: ignore
```

## Тестирование

### Юнит-тесты

**Файл**: `test/error-monitor.test.js`

```javascript
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { collectErrorMetrics, detectErrorRateChange } from '../src/error-monitor.js';

test('collectErrorMetrics: собирает метрики по моделям', async () => {
  // Arrange
  const db = createFakeDb();
  
  // Act
  const metrics = await collectErrorMetrics(db);
  
  // Assert
  assert.ok(metrics.length > 0);
  assert.ok(metrics[0].model);
  assert.ok(Number.isFinite(metrics[0].errorRate));
});

test('detectErrorRateChange: обнаруживает изменение частоты ошибок', () => {
  // Arrange
  const current = [{ model: 'test', errorRate: 0.3 }];
  const previous = [{ model: 'test', errorRate: 0.1 }];
  
  // Act
  const changes = detectErrorRateChange(current, previous, { threshold: 0.1 });
  
  // Assert
  assert.equal(changes.length, 1);
  assert.equal(changes[0].type, 'error_rate_change');
});

test('detectErrorRateChange: не срабатывает при малых изменениях', () => {
  const current = [{ model: 'test', errorRate: 0.11 }];
  const previous = [{ model: 'test', errorRate: 0.10 }];
  
  const changes = detectErrorRateChange(current, previous, { threshold: 0.1 });
  
  assert.equal(changes.length, 0);
});
```

### Интеграционные тесты

**Файл**: `test/free-models-integration.test.js`

```javascript
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { collectFreeModels } from '../src/free-models.js';

test('collectFreeModels: записывает изменения в catalog_changes', async () => {
  // Arrange
  const db = createFakeDb();
  const env = createFakeEnv();
  
  // Act
  const result = await collectFreeModels(env, db, { write: true });
  
  // Assert
  const changes = await db.prepare('SELECT * FROM catalog_changes').all();
  // Проверяем, что изменения записаны (если были)
});
```

## План внедрения

### Фаза 1: Базовая инфраструктура (1-2 дня)
1. Создать D1 миграцию `0018_monitoring_automation.sql`
2. Реализовать `src/error-monitor.js` с базовой функциональностью
3. Написать юнит-тесты
4. Добавить в `package.json` скрипт `test:sandbox:monitoring`

### Фаза 2: Интеграция с collect-free-models (1 день)
1. Модифицировать `src/free-models.js` для записи в `catalog_changes`
2. Добавить автоматический запуск бенчмарка для новых моделей
3. Интеграционные тесты

### Фаза 3: Workflow автоматизации (0.5 дня)
1. Создать `.github/workflows/auto-benchmark.yml`
2. Протестировать на staging

### Фаза 4: Мониторинг и алертинг (1 день)
1. Расширить `scripts/analytics.py` для отчетов по метрикам ошибок
2. Добавить Telegram уведомления о критических изменениях
3. Настроить dashboard

### Фаза 5: Staging окружение (1-2 дня)
1. Создать `src/staging-adapter.js`
2. Настроить изолированную D1 базу для staging
3. Sandbox тестирование

## Метрики успеха

1. **Частота ошибок**: Уменьшение доли ошибок HTTP 400 на 50% за первые 2 недели
2. **Время реакции**: Автоматическое обнаружение изменений в каталоге < 1 часа
3. **Покрытие тестами**: > 90% кода мониторинга покрыто тестами
4. **Staging**: Возможность тестировать изменения без влияния на прод

## Риски и митигация

1. **Ложные срабатывания детекта изменений**:
   - Использовать статистические тесты (хи-квадрат)
   - Настроить пороговые значения на основе исторических данных

2. **Нагрузка на D1**:
   - Ограничить частоту сбора метрик (раз в час)
   - Использовать индексы для ускорения запросов

3. **Стоимость замеров**:
   - Ограничить количество бенчмарков за запуск
   - Приоритизировать модели с наибольшим трафиком

## Связанные документы

- [PR #333: Контекстный пре-фильтр](https://github.com/trained-assist/trained-assist-llm-ladder/pull/333)
- [PR #334: Идемпотентная миграция 0017](https://github.com/trained-assist/trained-assist-llm-ladder/pull/334)
- [Issue #335: Автоматизация мониторинга](https://github.com/trained-assist/trained-assist-llm-ladder/issues/335)
