// Package report — JSON-экспорт результатов (мета + результаты с информацией о запросах).
package report

import (
	"encoding/json"
	"os"

	"clicktester/internal/tests"
)

// ExportData — данные для JSON-экспорта (мета + результаты с полями запроса: name, description, query и т.д.).
type ExportData struct {
	Meta    ReportMeta        `json:"meta"`
	Total   int               `json:"total"`
	Passed  int               `json:"passed"`
	Failed  int               `json:"failed"`
	Results []tests.TestResult `json:"results"`
}

// WriteJSON записывает результат прогона и метаданные в JSON по пути outputPath.
// В каждый элемент results входят и информация о запросе (name, description, query), и метрики (pass, granules, read_rows и т.д.).
func WriteJSON(outputPath string, r *tests.RunResult, meta *ReportMeta) error {
	if meta == nil {
		meta = &ReportMeta{}
	}
	data := ExportData{
		Meta:    *meta,
		Total:   r.Total,
		Passed:  r.Passed,
		Failed:  r.Failed,
		Results: r.Results,
	}
	raw, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(outputPath, raw, 0644)
}
