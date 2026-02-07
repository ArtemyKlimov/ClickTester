// Package server — HTTP-сервер для режима -serve: список тестов, запуск всех или по одному, открытие браузера.
package server

import (
	_ "embed"
	"context"
	"encoding/json"
	"net/http"
	"os/exec"
	"runtime"
	"strconv"
	"time"

	"clicktester/internal/chclient"
	"clicktester/internal/config"
	"clicktester/internal/runner"
	"clicktester/internal/tests"
)

//go:embed index.html
var indexHTML []byte

// TaskItem — элемент списка тестов для API (включая query для раскрытия на UI).
type TaskItem struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Type        string `json:"type"`
	Query       string `json:"query"`
}

// RunRequest — тело POST /api/run. Пустой taskIDs — запустить все.
type RunRequest struct {
	TaskIDs []int `json:"taskIDs"`
}

// Run запускает HTTP-сервер на port, открывает браузер по baseURL (например http://localhost:8080).
// Блокирует до остановки сервера (Shutdown или прерывание).
func Run(ctx context.Context, cfg *config.Config, taskList []tests.Task, client chclient.Client, port int, baseURL string) error {
	queryTimeout := time.Duration(cfg.Execution.QueryTimeoutSec) * time.Second
	workers := cfg.Execution.Workers
	if workers < 1 {
		workers = 1
	}
	if port <= 0 {
		port = 8080
	}
	addr := ":" + strconv.Itoa(port)
	srv := &http.Server{Addr: addr}

	// Маршруты
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})
	http.HandleFunc("/api/tasks", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		list := make([]TaskItem, 0, len(taskList))
		for _, t := range taskList {
			list = append(list, TaskItem{ID: t.ID, Name: t.Name, Description: t.Description, Type: string(t.Type), Query: t.Query})
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(list)
	})
	http.HandleFunc("/api/run", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req RunRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
			return
		}
		tasksToRun := taskList
		if len(req.TaskIDs) > 0 {
			idSet := make(map[int]bool)
			for _, id := range req.TaskIDs {
				idSet[id] = true
			}
			tasksToRun = make([]tests.Task, 0, len(req.TaskIDs))
			for _, t := range taskList {
				if idSet[t.ID] {
					tasksToRun = append(tasksToRun, t)
				}
			}
		}
		if len(tasksToRun) == 0 {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(&tests.RunResult{})
			return
		}
		result, err := runner.Run(ctx, tasksToRun, workers, client, queryTimeout)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(result)
	})

	go openBrowser(baseURL)

	return srv.ListenAndServe()
}

func openBrowser(url string) {
	// Небольшая задержка, чтобы сервер успел подняться.
	time.Sleep(500 * time.Millisecond)
	switch runtime.GOOS {
	case "windows":
		_ = exec.Command("cmd", "/c", "start", url).Start()
	case "darwin":
		_ = exec.Command("open", url).Start()
	default:
		_ = exec.Command("xdg-open", url).Start()
	}
}
