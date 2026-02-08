// Package config — сбор задач из конфига (structure_checks + query_templates).
package config

import (
	"fmt"
	"strconv"
	"strings"

	"clicktester/internal/tests"
)

// BuildTasks формирует список задач из structure_checks и query_templates.
// Для структурных проверок подставляются database и table_name из конфига.
// Для query_templates выполняется подстановка всех параметров (table_name, projectCode, appName и т.д.).
func BuildTasks(cfg *Config) ([]tests.Task, error) {
	var out []tests.Task
	id := 1

	db := cfg.ClickHouse.Database
	table := cfg.ClickHouse.TableName
	fullTable := db + "." + table

	for _, sc := range cfg.StructureChecks {
		q, err := structureQuery(sc.Type, db, table)
		if err != nil {
			return nil, fmt.Errorf("structure check %q: %w", sc.Name, err)
		}
		desc := sc.Description
		if desc == "" {
			desc = structureDescription(sc.Type)
		}
		out = append(out, tests.Task{
			ID:          id,
			Name:        sc.Name,
			Description: desc,
			Type:        tests.TaskTypeStructure,
			Query:       q,
			Opts:        tests.TaskOpts{},
		})
		id++
	}

	for _, qt := range cfg.QueryTemplates {
		q := SubstituteQueryParams(qt.Query, fullTable, &cfg.TestParams)
		out = append(out, tests.Task{
			ID:          id,
			Name:        qt.Name,
			Description: qt.Description,
			Type:        tests.TaskTypeQuery,
			Query:       q,
			Opts: tests.TaskOpts{
				CollectExplain: qt.CollectExplain,
				CollectStats:   qt.CollectStats,
			},
		})
		id++
	}

	return out, nil
}

// StressQueryByName возвращает запрос для стресс-теста по имени шаблона (query_templates).
// В возвращённой строке остаётся плейсхолдер $time_offset_ms$ для подстановки на каждый запрос.
func StressQueryByName(cfg *Config, queryName string) (string, error) {
	fullTable := cfg.ClickHouse.Database + "." + cfg.ClickHouse.TableName
	for _, qt := range cfg.QueryTemplates {
		if qt.Name == queryName {
			return SubstituteQueryParamsForStress(qt.Query, fullTable, &cfg.TestParams), nil
		}
	}
	return "", fmt.Errorf("query template %q not found", queryName)
}

// SubstituteQueryParams заменяет в запросе плейсхолдеры на значения из конфига.
// fullTable — значение для $table_name$ (например "logs_db.app_logs_v10").
// Для обычных тестов time_offset_ms подставляется из конфига (обычно 0).
func SubstituteQueryParams(query, fullTable string, p *TestParams) string {
	return substituteQueryParams(query, fullTable, p, true)
}

// SubstituteQueryParamsForStress подставляет все плейсхолдеры, кроме $time_offset_ms$
// (остаётся в запросе для подстановки в раннере стресс-теста на каждый запрос).
func SubstituteQueryParamsForStress(query, fullTable string, p *TestParams) string {
	return substituteQueryParams(query, fullTable, p, false)
}

func substituteQueryParams(query, fullTable string, p *TestParams, includeTimeOffset bool) string {
	repl := map[string]string{
		"$table_name$":   fullTable,
		"$projectCode$":  p.ProjectCode,
		"$appName$":      p.AppName,
		"$namespace$":    p.Namespace,
		"$level$":        p.Level,
		"$text_token$":   p.TextToken,
	}
	if includeTimeOffset {
		repl["$time_offset_ms$"] = strconv.Itoa(p.TimeOffsetMs)
	}
	s := query
	for k, v := range repl {
		s = strings.ReplaceAll(s, k, v)
	}
	return s
}

// structureQuery возвращает SQL для структурной проверки по типу.
func structureQuery(checkType, database, table string) (string, error) {
	switch checkType {
	case "partitions":
		return fmt.Sprintf(
			"SELECT partition, sum(rows) AS rows, sum(bytes_on_disk) AS bytes FROM system.parts WHERE database = '%s' AND table = '%s' AND active GROUP BY partition ORDER BY partition",
			escapeSingleQuotes(database), escapeSingleQuotes(table)), nil
	case "indexes":
		return fmt.Sprintf(
			"SELECT name, type, expr, granularity FROM system.data_skipping_indices WHERE database = '%s' AND table = '%s'",
			escapeSingleQuotes(database), escapeSingleQuotes(table)), nil
	case "projections":
		return fmt.Sprintf(
			"SELECT name, partition, part_type, rows FROM system.projection_parts WHERE database = '%s' AND table = '%s'",
			escapeSingleQuotes(database), escapeSingleQuotes(table)), nil
	case "granules_settings":
		return fmt.Sprintf("SHOW CREATE TABLE %s.%s",
			escapeIdentifier(database), escapeIdentifier(table)), nil
	default:
		return "", fmt.Errorf("unknown structure check type: %s", checkType)
	}
}

func structureDescription(checkType string) string {
	switch checkType {
	case "partitions":
		return "Проверка наличия и списка партиций таблицы (system.parts)."
	case "indexes":
		return "Проверка наличия data skipping индексов и их типов (bloom_filter, tokenbf_v1)."
	case "projections":
		return "Проверка наличия проекций (например counter_with_dims)."
	case "granules_settings":
		return "Проверка настроек гранул (SHOW CREATE TABLE)."
	default:
		return ""
	}
}

func escapeSingleQuotes(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}

func escapeIdentifier(s string) string {
	// Минимальная защита: если есть спецсимволы — обернуть в бэктики (ClickHouse использует backticks).
	if s == "" || strings.ContainsAny(s, " \t\n\r\"'`;") {
		return "`" + strings.ReplaceAll(s, "`", "``") + "`"
	}
	return s
}
