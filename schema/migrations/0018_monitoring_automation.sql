-- 0018: Мониторинг автоматизации (issue #335)
--
-- Две новые таблицы для отслеживания частоты ошибок и изменений в каталоге:
--
-- 1. error_metrics_history — история метрик ошибок по моделям.
--    Позволяет отслеживать тренды частоты ошибок и распределение ошибок
--    по размерам запросов (small/medium/large/xlarge).
--    Используется для детекта изменений в окне моделей.
--
-- 2. catalog_changes — журнал изменений в каталоге бесплатных моделей.
--    Записывает появление/исчезновение/изменение моделей.
--    Используется для автоматического запуска бенчмарков при изменениях.

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
  size_distribution TEXT NOT NULL, -- JSON: { small: {calls, failures}, medium: {...}, ... }
  raw_metrics TEXT NOT NULL        -- JSON с полными метриками
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
  details TEXT,              -- JSON с деталями изменения
  benchmark_triggered INTEGER DEFAULT 0,
  benchmark_result TEXT      -- JSON с результатом бенчмарка
);

CREATE INDEX IF NOT EXISTS idx_catalog_changes_detected ON catalog_changes (detected_at);
CREATE INDEX IF NOT EXISTS idx_catalog_changes_model ON catalog_changes (model_id);
CREATE INDEX IF NOT EXISTS idx_catalog_changes_benchmark ON catalog_changes (benchmark_triggered);
