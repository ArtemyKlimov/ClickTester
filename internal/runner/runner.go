// Package runner — выполнение тестов через клиент ClickHouse (пул воркеров, сбор результатов).
package runner

import (
	"context"
	"sync"
	"time"

	"clicktester/internal/chclient"
	"clicktester/internal/tests"
)

// Run запускает все задачи через client с пулом из workers горутин и возвращает агрегированный результат.
// Задачи раздаются воркерам по индексу; результаты собираются в порядке задач.
func Run(ctx context.Context, tasks []tests.Task, workers int, client chclient.Client, queryTimeout time.Duration) (*tests.RunResult, error) {
	if workers < 1 {
		workers = 1
	}
	if len(tasks) == 0 {
		return &tests.RunResult{}, nil
	}

	n := len(tasks)
	result := &tests.RunResult{
		Total:   n,
		Results: make([]tests.TestResult, n),
	}

	// Канал индексов задач для воркеров.
	taskCh := make(chan int, n)
	for i := 0; i < n; i++ {
		taskCh <- i
	}
	close(taskCh)

	// Канал результатов: (индекс, результат).
	type resultItem struct {
		idx int
		res tests.TestResult
	}
	resultCh := make(chan resultItem, n)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range taskCh {
				res := runOne(ctx, tasks[i], client, queryTimeout)
				resultCh <- resultItem{idx: i, res: res}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	for item := range resultCh {
		result.Results[item.idx] = item.res
		if item.res.Pass {
			result.Passed++
		} else {
			result.Failed++
		}
	}

	return result, nil
}

func runOne(ctx context.Context, t tests.Task, client chclient.Client, queryTimeout time.Duration) tests.TestResult {
	tr := tests.TestResult{
		TaskID:      t.ID,
		Name:        t.Name,
		Description: t.Description,
		Type:        t.Type,
		Pass:        false,
	}

	if queryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, queryTimeout)
		defer cancel()
	}

	switch t.Type {
	case tests.TaskTypeStructure:
		_, _, _, err := client.Query(ctx, t.Query)
		tr.Pass = err == nil
		if err != nil {
			tr.Error = err.Error()
		}
	case tests.TaskTypeQuery:
		if t.Opts.CollectExplain {
			explainText, err := client.Explain(ctx, t.Query)
			if err != nil {
				tr.Error = "EXPLAIN: " + err.Error()
				return tr
			}
			tr.ExplainText = explainText
			tr.Granules = chclient.ExtractGranules(explainText)
		}

		start := time.Now()
		rows, readRows, readBytes, err := client.Query(ctx, t.Query)
		tr.DurationMs = time.Since(start).Seconds() * 1000
		if err != nil {
			tr.Error = err.Error()
			return tr
		}

		tr.Pass = true
		tr.RowsReturned = rows
		tr.ReadRows = readRows
		tr.ReadBytes = readBytes
	}

	return tr
}
