// Package report — генерация HTML-отчёта по результатам тестов.
package report

import (
	"bytes"
	"fmt"
	"html"
	"os"
	"strings"
	"text/template"
	"time"

	"clicktester/internal/tests"
)

// ReportMeta — метаданные для заголовка отчёта (опционально).
type ReportMeta struct {
	GeneratedAt       string
	Host               string
	Database           string
	Table              string
	Workers            int
	GranulesWarn       int
	GranulesFail       int
	ReadRowsWarn       int
}

// rowView — одна строка таблицы с вычисленным статусом (все поля — примитивы для шаблона).
type rowView struct {
	TaskID       int
	Name         string
	Description  string
	TypeStr      string
	Status       string
	Error        string
	Granules     int
	ReadRows     uint64
	ReadMB       string
	Duration     string
	RowsReturned int
	ExplainText  string
}

// reportData — данные для шаблона.
type reportData struct {
	Meta   ReportMeta
	Total  int
	Passed int
	Failed int
	Rows   []rowView
}

// WriteHTML записывает RunResult в HTML-файл по пути outputPath.
// meta может быть nil — тогда заголовок без конфига; пороги для warn/fail берутся из meta.
func WriteHTML(outputPath string, r *tests.RunResult, meta *ReportMeta) error {
	if meta == nil {
		meta = &ReportMeta{GeneratedAt: time.Now().Format("2006-01-02 15:04:05")}
	}
	if meta.GeneratedAt == "" {
		meta.GeneratedAt = time.Now().Format("2006-01-02 15:04:05")
	}

	rows := make([]rowView, 0, len(r.Results))
	for _, res := range r.Results {
		rv := rowView{
			TaskID:       res.TaskID,
			Name:         res.Name,
			Description:  res.Description,
			TypeStr:      string(res.Type),
			Status:       rowStatus(res, meta),
			Error:        res.Error,
			Granules:     res.Granules,
			ReadRows:     res.ReadRows,
			RowsReturned: res.RowsReturned,
			ExplainText:  res.ExplainText,
		}
		if res.ReadBytes > 0 {
			rv.ReadMB = fmt.Sprintf("%.2f", float64(res.ReadBytes)/(1024*1024))
		} else if string(res.Type) == "query" {
			rv.ReadMB = "0.00"
		} else {
			rv.ReadMB = "—"
		}
		if res.DurationMs > 0 {
			rv.Duration = fmt.Sprintf("%.2f", res.DurationMs)
		} else {
			rv.Duration = "—"
		}
		rows = append(rows, rv)
	}

	data := reportData{
		Meta:   *meta,
		Total:  r.Total,
		Passed: r.Passed,
		Failed: r.Failed,
		Rows:   rows,
	}

	tmpl := template.Must(template.New("report").Funcs(funcMap).Parse(reportTemplate))
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return err
	}
	return os.WriteFile(outputPath, buf.Bytes(), 0644)
}

func rowStatus(res tests.TestResult, meta *ReportMeta) string {
	if !res.Pass {
		return "fail"
	}
	if meta.GranulesFail > 0 && res.Granules >= meta.GranulesFail {
		return "fail"
	}
	if (meta.GranulesWarn > 0 && res.Granules >= meta.GranulesWarn) ||
		(meta.ReadRowsWarn > 0 && int(res.ReadRows) >= meta.ReadRowsWarn) {
		return "warn"
	}
	return "ok"
}

var funcMap = template.FuncMap{
	"safe": func(s interface{}) string { return html.EscapeString(fmt.Sprintf("%v", s)) },
	"str":  func(v interface{}) string { return fmt.Sprintf("%v", v) },
	"shortQuery": func(s string, max int) string {
		s = strings.TrimSpace(s)
		if len(s) <= max {
			return s
		}
		return s[:max] + "..."
	},
	"shortExplain": func(s string, max int) string {
		s = strings.TrimSpace(s)
		if len(s) <= max {
			return s
		}
		return s[:max] + "..."
	},
}

const reportTemplate = `<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <title>ClickHouse Table Structure Test Report</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 1rem 2rem; background: #f5f5f5; }
    h1 { color: #222; }
    .meta { color: #666; font-size: 0.9rem; margin-bottom: 1rem; }
    .summary { margin: 1rem 0; padding: 1rem; background: #fff; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }
    .summary span { margin-right: 1.5rem; }
    table { border-collapse: collapse; width: 100%; background: #fff; box-shadow: 0 1px 3px rgba(0,0,0,0.08); border-radius: 8px; overflow: hidden; }
    th, td { padding: 0.5rem 0.75rem; text-align: left; border-bottom: 1px solid #eee; }
    th { background: #374151; color: #fff; font-weight: 600; }
    tr:hover { background: #f9fafb; }
    .status-ok { color: #059669; font-weight: 600; }
    .status-warn { color: #d97706; font-weight: 600; }
    .status-fail { color: #dc2626; font-weight: 600; }
    .error { color: #dc2626; font-size: 0.85rem; max-width: 40em; }
    .explain { font-size: 0.8rem; white-space: pre-wrap; max-height: 8em; overflow: auto; background: #f9fafb; padding: 0.5rem; border-radius: 4px; }
  </style>
</head>
<body>
  <h1>ClickHouse Table Structure Test Report</h1>
  <div class="meta">
    Generated: {{ safe .Meta.GeneratedAt }}
    {{ if .Meta.Host }} | Host: {{ safe .Meta.Host }}{{ end }}
    {{ if .Meta.Database }} | Database: {{ safe .Meta.Database }}{{ end }}
    {{ if .Meta.Table }} | Table: {{ safe .Meta.Table }}{{ end }}
    {{ if .Meta.Workers }} | Workers: {{ .Meta.Workers }}{{ end }}
  </div>
  <div class="summary">
    <span><strong>Total:</strong> {{ .Total }}</span>
    <span><strong>Passed:</strong> <span class="status-ok">{{ .Passed }}</span></span>
    <span><strong>Failed:</strong> <span class="status-fail">{{ .Failed }}</span></span>
  </div>
  <table>
    <thead>
      <tr>
        <th>#</th>
        <th>Name</th>
        <th>Description</th>
        <th>Type</th>
        <th>Status</th>
        <th>Granules</th>
        <th>Read Rows</th>
        <th>Read MB</th>
        <th>Duration (ms)</th>
        <th>Rows</th>
        <th>Error / Details</th>
      </tr>
    </thead>
    <tbody>
      {{ range .Rows }}
      <tr>
        <td>{{ .TaskID }}</td>
        <td>{{ safe .Name }}</td>
        <td>{{ safe .Description }}</td>
        <td>{{ safe .TypeStr }}</td>
        <td><span class="status-{{ .Status }}">{{ .Status }}</span></td>
        <td>{{ if eq .TypeStr "query" }}{{ .Granules }}{{ else }}—{{ end }}</td>
        <td>{{ if eq .TypeStr "query" }}{{ .ReadRows }}{{ else }}—{{ end }}</td>
        <td>{{ .ReadMB }}</td>
        <td>{{ .Duration }}</td>
        <td>{{ if eq .TypeStr "query" }}{{ .RowsReturned }}{{ else }}—{{ end }}</td>
        <td>
          {{ if .Error }}<span class="error">{{ safe .Error }}</span>{{ end }}
          {{ if and (not .Error) .ExplainText }}<details><summary>EXPLAIN</summary><div class="explain">{{ safe .ExplainText }}</div></details>{{ end }}
        </td>
      </tr>
      {{ end }}
    </tbody>
  </table>
</body>
</html>
`
