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

// TestResult — результат выполнения одной задачи.
type TestResult struct {
	TaskID       int
	Name         string
	Description  string
	Type         TaskType
	Pass         bool
	Error        string
	Granules     int
	ReadRows     uint64
	ReadBytes    uint64
	DurationMs   float64
	RowsReturned int
	ExplainText  string
}

// RunResult — агрегированный результат прогона всех тестов.
type RunResult struct {
	Total   int
	Passed  int
	Failed  int
	Results []TestResult
}
