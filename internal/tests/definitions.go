// Package tests определяет типы тестов и результатов для runner и отчёта.
package tests

// Task — одна задача для выполнения (структурная проверка или запрос).
type Task struct {
	ID          int
	Name        string
	Description string
	Type        TaskType
	Query       string
	Opts        TaskOpts
}

// TaskType — тип задачи.
type TaskType string

const (
	TaskTypeStructure TaskType = "structure"
	TaskTypeQuery     TaskType = "query"
)

// TaskOpts — опции выполнения (EXPLAIN, сбор статистики).
type TaskOpts struct {
	CollectExplain bool
	CollectStats   bool
}

// PartitionInfo — сведения о партиции из system.parts (для отчёта).
type PartitionInfo struct {
	Partition string `json:"partition"`
	Rows      uint64 `json:"rows"`
	Bytes     uint64 `json:"bytes"`
}

// TestResult — результат выполнения одной задачи (поля с json для экспорта).
type TestResult struct {
	TaskID           int              `json:"task_id"`
	Name             string           `json:"name"`
	Description      string           `json:"description"`
	Type             TaskType         `json:"type"`
	Query            string           `json:"query"`
	Pass             bool             `json:"pass"`
	Error            string           `json:"error,omitempty"`
	Granules         int              `json:"granules"`
	ReadRows         uint64           `json:"read_rows"`
	ReadBytes        uint64           `json:"read_bytes"`
	MemoryUsage      uint64           `json:"memory_usage"`
	QueryID          string           `json:"query_id,omitempty"`          // для поиска в system.query_log
	Partitions       []string         `json:"partitions,omitempty"`       // ID партиций из query_log
	PartitionDetails []PartitionInfo  `json:"partition_details,omitempty"` // строки/байты по партициям из system.parts
	DurationMs       float64          `json:"duration_ms"`
	RowsReturned     int              `json:"rows_returned"`
	ProjectionUsed   bool             `json:"projection_used"`
	ExplainText      string           `json:"explain_text,omitempty"`
}

// RunResult — агрегированный результат прогона всех тестов.
type RunResult struct {
	Total   int
	Passed  int
	Failed  int
	Results []TestResult
}
