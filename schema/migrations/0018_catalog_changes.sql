-- Журнал изменений каталога (issue #335): когда модель появилась/ушла/изменилась и запускался ли
-- автоматический бенчмарк. Нужен для истории и алертинга по воронке бесплатных моделей.
--
-- Без этой таблицы КАЖДЫЙ сбор с записью падал (HTTP 500 `no such table: catalog_changes`), то
-- есть с момента #335 ни один прогон не обновлял free_models: in_ladder оставался устаревшим,
-- а провайдер opencode-go вообще не добирался до проб (#341, п. 2 и 3). Миграция идёт
-- идемпотентно, CI применяет её на каждый деплой.
--
-- Тела провайдеров, промпты и ключи сюда не попадают: details держит только имена изменённых
-- полей, а не их значения.
CREATE TABLE IF NOT EXISTS catalog_changes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  detected_at INTEGER NOT NULL,
  provider TEXT NOT NULL,
  change_type TEXT NOT NULL,
  model_id TEXT NOT NULL,
  details TEXT,
  benchmark_triggered INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_catalog_changes_window
  ON catalog_changes (detected_at, provider, change_type);

CREATE INDEX IF NOT EXISTS idx_catalog_changes_model
  ON catalog_changes (model_id, detected_at DESC);
