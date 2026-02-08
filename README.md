# ClickTester — тестирование структуры таблицы ClickHouse

Утилита на **Go** для проверки структуры таблицы ClickHouse (партиции, индексы, проекции, гранулы) и выполнения набора тестовых запросов с замером метрик и генерацией HTML-отчёта.

## Требования

- **Go 1.24+** (см. `go.mod`)
- **ClickHouse** (поддерживаются native 9000/9440 и HTTP/HTTPS 8123/8443)

## Быстрый старт

```bash
# Сборка
go build -o clicktester.exe ./cmd/clicktester

# Запуск с конфигом по умолчанию (CLI: тесты → HTML-файл)
.\clicktester.exe -config configs/default.yaml

# Режим сервера: браузер со списком тестов, запуск всех или по одному
.\clicktester.exe -serve -config configs/default.yaml -port 8080

# Стресс-тест 5 мин, 10 потоков (запрос из stress_test.query_name в конфиге)
.\clicktester.exe -stress -config configs/default.yaml

# Переопределение числа воркеров и пути отчёта
.\clicktester.exe -config configs/default.yaml -workers 8 -output reports/run.html
```

После запуска в консоль выводится сводка (`tasks=…, passed=…, failed=…`), детальный отчёт сохраняется в HTML (по умолчанию `reports/report.html`).

## Конфигурация (YAML)

Путь к конфигу задаётся флагом `-config` (по умолчанию `configs/default.yaml`).

### Основные секции

| Секция | Назначение |
|--------|------------|
| `clickhouse` | Подключение: `host`, `port` (9000 — native, 9440 — native TLS; 8123 — HTTP, 8443 — HTTPS), `database`, `user`, `password`, `table_name`, `secure` (TLS). При `secure: true` опционально: `tls_skip_verify`, `tls_ca_file` (PEM с CA), `tls_pfx_file` (клиентский сертификат PFX/P12 для mTLS), `tls_pfx_password` |
| `test_params` | Параметры для подстановки в шаблоны запросов: `projectCode`, `appName`, `namespace`, `level`, `text_token` |
| `execution` | `workers` — число параллельных воркеров, `query_timeout_sec` — таймаут запроса (сек) |
| `report` | `output_path` — путь к HTML-отчёту; `thresholds` — пороги для статусов warn/fail |
| `stress_test` | Опционально: `duration_minutes`, `workers`, `query_name` — для режима `-stress` |
| `structure_checks` | Список структурных проверок (партиции, индексы, проекции, настройки гранул) |
| `query_templates` | Список шаблонов запросов с подстановкой параметров (для стресса — шаблон с `$time_offset_ms$`) |

### Плейсхолдеры в запросах

В тексте запроса подставляются:

- `$table_name$` → `database.table_name`
- `$projectCode$`, `$appName$`, `$namespace$`, `$level$`, `$text_token$` → значения из `test_params`
- `$time_offset_ms$` → в обычных тестах 0; в стресс-тесте подставляется на каждый запрос (1, 2, 3, …) для обхода кэша

### Структурные проверки (`structure_checks`)

- **partitions** — список партиций и объём данных (`system.parts`)
- **indexes** — data skipping индексы (`system.data_skipping_indices`)
- **projections** — проекции (`system.projection_parts`)
- **granules_settings** — настройки гранул (`SHOW CREATE TABLE`)

У каждой проверки можно указать `name`, `type` и опционально `description`.

### Шаблоны запросов (`query_templates`)

Каждый элемент: `name`, `description` (кратко, что проверяется), `query`, `collect_explain`, `collect_stats`.  
В `configs/default.yaml` приведены примеры по образцу `benchmark-dso-config/application-new.yml`: выборки по проекту/приложению/namespace за 15 мин, 1 ч, 1 день, 4 дня, а также агрегации по интервалам (1/5/30 мин).

## Флаги CLI

| Флаг | Описание | По умолчанию |
|------|----------|--------------|
| `-config` | Путь к YAML/JSON конфигу | `configs/default.yaml` |
| `-workers` | Число воркеров (0 = из конфига) | 0 |
| `-output` | Путь к HTML-отчёту (переопределяет конфиг) | — |
| `-format` | Формат вывода: `html`, `json` или `both` (при `both` пишутся HTML и JSON) | html |
| `-stress` | Запустить стресс-тест (N мин, N потоков, один запрос с меняющимся временем) | false |
| `-serve` | Запустить HTTP-сервер и открыть браузер со списком тестов | false |
| `-port` | Порт HTTP-сервера (при `-serve`) | 8080 |

При `-serve` приложение поднимает веб-интерфейс: список тестов из конфига, кнопка «Запустить все» и «Запустить» у каждого теста. Результаты (статус, время, гранулы, read rows, ошибка) отображаются в таблице. Остановка — Ctrl+C.

**Стресс-тест (`-stress`)** — в течение N минут в N потоков выполняется один выбранный запрос. В шаблоне запроса должен быть плейсхолдер `$time_offset_ms$` (например, `... - toIntervalMillisecond($time_offset_ms$) ...`); на каждый запрос он заменяется на новое значение (0, 1, 2, …), чтобы запрос не кэшировался. Цель — проверить деградацию БД под нагрузкой. В конфиге задаётся секция `stress_test`: `duration_minutes`, `workers`, `query_name` (имя из `query_templates`). В выводе: total, success, failed, cancelled, QPS, латентности **p50/p95/p99** (мс), примеры ошибок.

**Как читать перцентили латентности:**
- **p50 (медиана)** — у половины запросов время ответа было не больше этого значения (мс). Отражает «типичную» задержку.
- **p95** — у 95% запросов задержка была не больше этого значения. Показывает «хвост»: редкие тяжёлые ответы.
- **p99** — у 99% запросов задержка не больше этого значения. Сильнее всего реагирует на всплески и конкуренцию за ресурсы.

Чем выше p95 и p99 относительно p50, тем больше разброс: часть запросов выполняется заметно дольше. Рост p95/p99 при увеличении числа воркеров при том же QPS говорит о росте очередей и конкуренции за БД.

## HTML-отчёт

В отчёте отображаются:

- Метаданные запуска: дата, хост, БД, таблица, число воркеров, пороги.
- Таблица результатов: №, Name, Type, Status (ok / warn / fail), **Projection** (yes/no по EXPLAIN), Granules, Read Rows, Read MB, Duration, Rows, Error или текст EXPLAIN. Строки можно раскрыть для просмотра описания и SQL.
- При `-format json` или `-format both` дополнительно пишется JSON (по умолчанию рядом с HTML, расширение .json): метаданные и массив результатов с полями запроса (name, description, query) и метриками (pass, granules, read_rows, projection_used и т.д.).

Статусы для запросов типа `query`:

- **ok** — запрос выполнен и метрики ниже порогов.
- **warn** — превышен `granules_warn` или `read_rows_warn`.
- **fail** — ошибка выполнения или превышен `granules_fail`.

## Структура проекта

```
ClickTester/
├── cmd/clicktester/main.go   # точка входа, флаги, загрузка конфига, запуск тестов, запись отчёта
├── internal/
│   ├── config/               # загрузка конфига, BuildTasks, подстановка параметров
│   ├── chclient/             # клиент ClickHouse (native), Query, Explain, ExtractGranules
│   ├── runner/               # пул воркеров, выполнение задач, сбор результатов
│   ├── report/               # HTML-шаблон, WriteHTML, статусы по порогам
│   ├── server/               # режим -serve: HTTP-сервер, /api/tasks, /api/run, UI (embed index.html)
│   └── tests/                # Task, TestResult, RunResult
├── configs/default.yaml      # пример конфига (structure_checks + query_templates)
├── Create_db_v11.sql         # референсная схема таблицы
├── benchmark-dso-config/     # образец query-templates и agg-templates
├── docs/improvements-proposal.md  # предложения по тестам и улучшениям
├── go.mod, go.sum
└── README.md
```

## Дальнейшие улучшения

Идеи новых тестов (structure: ORDER BY, TTL, codecs, parts_count; query: по messageId/eventId/attributes/mdc), доработки отчёта, конфига, CLI и тестирования кода собраны в **[docs/improvements-proposal.md](docs/improvements-proposal.md)**.

## Ссылки

- Схема таблицы: `Create_db_v11.sql`
- Примеры запросов и агрегаций: `benchmark-dso-config/application-new.yml`
