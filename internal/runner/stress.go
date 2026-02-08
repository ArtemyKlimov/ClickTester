// Package runner — стресс-тест: N минут в N потоков один запрос с меняющимся $time_offset_ms$.
package runner

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"clicktester/internal/chclient"
)

const timeOffsetPlaceholder = "$time_offset_ms$"

// StressResult — результат стресс-теста.
type StressResult struct {
	Total        int      // всего запросов (success + failed + cancelled)
	Success      int      // успешных
	Failed       int      // с ошибкой БД/сети
	Cancelled    int      // оборваны по отмене контекста (конец теста)
	DurationSec  float64  // длительность в секундах
	QPS          float64  // запросов в секунду
	LatencyP50Ms float64  // медиана задержки, мс
	LatencyP95Ms float64  // p95 задержки, мс
	LatencyP99Ms float64  // p99 задержки, мс
	ErrorSamples []string // примеры ошибок (до 5)
}

// RunStress запускает стресс-тест: до отмены ctx в workers горутинах выполняется baseQuery.
// В baseQuery должен быть плейсхолдер $time_offset_ms$; на каждый запрос он заменяется на новое значение (0, 1, 2, ...),
// чтобы запрос не кэшировался. Возвращает сводку: total, success, failed, QPS, перцентили задержки.
func RunStress(ctx context.Context, baseQuery string, workers int, queryTimeout time.Duration, client chclient.Client) *StressResult {
	if workers < 1 {
		workers = 1
	}
	if !strings.Contains(baseQuery, timeOffsetPlaceholder) {
		// без плейсхолдера все запросы одинаковые (кэш)
		baseQuery = baseQuery + " -- no $time_offset_ms$"
	}

	var counter uint64
	var latenciesMu sync.Mutex
	latencies := make([]float64, 0, 1024)
	var errorsMu sync.Mutex
	errorSamples := make([]string, 0, 5)

	resultCh := make(chan struct {
		durationMs float64
		err       error
	}, workers*32)

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				offset := atomic.AddUint64(&counter, 1)
				q := strings.ReplaceAll(baseQuery, timeOffsetPlaceholder, strconv.FormatUint(offset, 10))
				t0 := time.Now()
				var err error
				if queryTimeout > 0 {
					runCtx, cancel := context.WithTimeout(ctx, queryTimeout)
					_, _, _, _, err = client.Query(runCtx, q)
					cancel()
				} else {
					_, _, _, _, err = client.Query(ctx, q)
				}
				durationMs := time.Since(t0).Seconds() * 1000
				resultCh <- struct {
					durationMs float64
					err       error
				}{durationMs, err}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	var total, success, failed, cancelled int
	for r := range resultCh {
		total++
		if r.err != nil {
			if isContextCanceled(r.err) {
				cancelled++
			} else {
				failed++
				errorsMu.Lock()
				if len(errorSamples) < 5 {
					errorSamples = append(errorSamples, r.err.Error())
				}
				errorsMu.Unlock()
			}
		} else {
			success++
			latenciesMu.Lock()
			latencies = append(latencies, r.durationMs)
			latenciesMu.Unlock()
		}
	}
	durationSec := time.Since(start).Seconds()

	result := &StressResult{
		Total:        total,
		Success:      success,
		Failed:       failed,
		Cancelled:    cancelled,
		DurationSec:  durationSec,
		ErrorSamples: errorSamples,
	}
	if result.DurationSec > 0 {
		result.QPS = float64(total) / result.DurationSec
	}
	if len(latencies) > 0 {
		sort.Float64s(latencies)
		n := len(latencies)
		result.LatencyP50Ms = percentile(latencies, n, 50)
		result.LatencyP95Ms = percentile(latencies, n, 95)
		result.LatencyP99Ms = percentile(latencies, n, 99)
	}
	return result
}

func isContextCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func percentile(sorted []float64, n, p int) float64 {
	if n == 0 {
		return 0
	}
	idx := (n * p) / 100
	if idx >= n {
		idx = n - 1
	}
	return sorted[idx]
}
