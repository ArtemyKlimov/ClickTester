// Package main — точка входа ClickHouse Table Structure Tester.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"clicktester/internal/chclient"
	"clicktester/internal/config"
	"clicktester/internal/report"
	"clicktester/internal/runner"
	"clicktester/internal/server"
)

func main() {
	cfgPath := flag.String("config", "configs/default.yaml", "path to YAML/JSON config")
	workers := flag.Int("workers", 0, "override number of workers (0 = use config)")
	output := flag.String("output", "", "path to output HTML report (overrides config)")
	format := flag.String("format", "html", "output format: html, json, or both")
	stress := flag.Bool("stress", false, "run stress test (N min, N workers, one query with shifting time to avoid cache)")
	serve := flag.Bool("serve", false, "start HTTP server and open browser with test list")
	port := flag.Int("port", 8080, "port for HTTP server (when -serve)")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}

	if *workers > 0 {
		cfg.Execution.Workers = *workers
	}
	if *output != "" {
		cfg.Report.OutputPath = *output
	}

	ctx := context.Background()

	if *stress {
		if cfg.StressTest == nil || cfg.StressTest.QueryName == "" {
			fmt.Fprintf(os.Stderr, "stress: config must have stress_test.query_name (and duration_minutes, workers)\n")
			os.Exit(1)
		}
		baseQuery, err := config.StressQueryByName(cfg, cfg.StressTest.QueryName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "stress: %v\n", err)
			os.Exit(1)
		}
		opts := chclient.ConnectOptions{
			Host:           cfg.ClickHouse.Host,
			Port:           cfg.ClickHouse.Port,
			Database:       cfg.ClickHouse.Database,
			User:           cfg.ClickHouse.User,
			Password:       cfg.ClickHouse.Password,
			Table:          cfg.ClickHouse.TableName,
			Secure:         cfg.ClickHouse.Secure,
			TLSSkipVerify:  cfg.ClickHouse.TLSSkipVerify,
			TLSCAFile:      cfg.ClickHouse.TLSCAFile,
			TLSPfxFile:     cfg.ClickHouse.TLSPfxFile,
			TLSPfxPassword: cfg.ClickHouse.TLSPfxPassword,
		}
		client, err := chclient.New(ctx, opts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "clickhouse: %v\n", err)
			os.Exit(1)
		}
		defer func() { _ = client.Close() }()
		duration := time.Duration(cfg.StressTest.DurationMinutes) * time.Minute
		workers := cfg.StressTest.Workers
		if workers < 1 {
			workers = cfg.Execution.Workers
		}
		if workers < 1 {
			workers = 1
		}
		queryTimeout := time.Duration(cfg.Execution.QueryTimeoutSec) * time.Second
		stressCtx, cancel := context.WithTimeout(ctx, duration)
		defer cancel()
		fmt.Printf("clicktester stress: duration=%v, workers=%d, query=%s\n", duration, workers, cfg.StressTest.QueryName)
		res := runner.RunStress(stressCtx, baseQuery, workers, queryTimeout, client)
		fmt.Printf("stress result: total=%d success=%d failed=%d cancelled=%d duration=%.1fs QPS=%.1f latency_p50=%.1fms p95=%.1fms p99=%.1fms\n",
			res.Total, res.Success, res.Failed, res.Cancelled, res.DurationSec, res.QPS, res.LatencyP50Ms, res.LatencyP95Ms, res.LatencyP99Ms)
		if len(res.ErrorSamples) > 0 {
			fmt.Fprintf(os.Stderr, "error samples:\n")
			for _, s := range res.ErrorSamples {
				fmt.Fprintf(os.Stderr, "  %s\n", s)
			}
		}
		return
	}

	if *serve {
		tasks, err := config.BuildTasks(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "build tasks: %v\n", err)
			os.Exit(1)
		}
		opts := chclient.ConnectOptions{
			Host:           cfg.ClickHouse.Host,
			Port:           cfg.ClickHouse.Port,
			Database:       cfg.ClickHouse.Database,
			User:           cfg.ClickHouse.User,
			Password:       cfg.ClickHouse.Password,
			Table:          cfg.ClickHouse.TableName,
			Secure:         cfg.ClickHouse.Secure,
			TLSSkipVerify:  cfg.ClickHouse.TLSSkipVerify,
			TLSCAFile:      cfg.ClickHouse.TLSCAFile,
			TLSPfxFile:     cfg.ClickHouse.TLSPfxFile,
			TLSPfxPassword: cfg.ClickHouse.TLSPfxPassword,
		}
		client, err := chclient.New(ctx, opts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "clickhouse: %v\n", err)
			os.Exit(1)
		}
		defer func() { _ = client.Close() }()
		if *port <= 0 {
			*port = 8080
		}
		baseURL := "http://127.0.0.1:" + strconv.Itoa(*port)
		fmt.Printf("clicktester: server at %s (Ctrl+C to stop)\n", baseURL)
		if err := server.Run(ctx, cfg, tasks, client, *port, baseURL); err != nil {
			fmt.Fprintf(os.Stderr, "server: %v\n", err)
			os.Exit(1)
		}
		return
	}

	opts := chclient.ConnectOptions{
		Host:           cfg.ClickHouse.Host,
		Port:           cfg.ClickHouse.Port,
		Database:       cfg.ClickHouse.Database,
		User:           cfg.ClickHouse.User,
		Password:       cfg.ClickHouse.Password,
		Table:          cfg.ClickHouse.TableName,
		Secure:         cfg.ClickHouse.Secure,
		TLSSkipVerify:  cfg.ClickHouse.TLSSkipVerify,
		TLSCAFile:      cfg.ClickHouse.TLSCAFile,
		TLSPfxFile:     cfg.ClickHouse.TLSPfxFile,
		TLSPfxPassword: cfg.ClickHouse.TLSPfxPassword,
	}
	client, err := chclient.New(ctx, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "clickhouse: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = client.Close() }()

	tasks, err := config.BuildTasks(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build tasks: %v\n", err)
		os.Exit(1)
	}

	queryTimeout := time.Duration(cfg.Execution.QueryTimeoutSec) * time.Second
	result, err := runner.Run(ctx, tasks, cfg.Execution.Workers, client, queryTimeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "runner: %v\n", err)
		os.Exit(1)
	}

	outPath := cfg.Report.OutputPath
	if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir report: %v\n", err)
		os.Exit(1)
	}
	reportMeta := &report.ReportMeta{
		GeneratedAt: time.Now().Format("2006-01-02 15:04:05"),
		Host:        cfg.ClickHouse.Host,
		Database:    cfg.ClickHouse.Database,
		Table:       cfg.ClickHouse.TableName,
		Workers:     cfg.Execution.Workers,
		GranulesWarn: cfg.Report.Thresholds.GranulesWarn,
		GranulesFail: cfg.Report.Thresholds.GranulesFail,
		ReadRowsWarn: cfg.Report.Thresholds.ReadRowsWarn,
	}
	writeHTML := *format == "html" || *format == "both"
	writeJSON := *format == "json" || *format == "both"
	jsonPath := outPath
	if strings.HasSuffix(strings.ToLower(outPath), ".html") {
		jsonPath = outPath[:len(outPath)-5] + ".json"
	} else {
		jsonPath = outPath + ".json"
	}
	if writeHTML {
		if err := report.WriteHTML(outPath, result, reportMeta); err != nil {
			fmt.Fprintf(os.Stderr, "report: %v\n", err)
			os.Exit(1)
		}
	}
	if writeJSON {
		if err := report.WriteJSON(jsonPath, result, reportMeta); err != nil {
			fmt.Fprintf(os.Stderr, "report json: %v\n", err)
			os.Exit(1)
		}
	}
	reportPaths := outPath
	if writeHTML && writeJSON {
		reportPaths = outPath + ", " + jsonPath
	} else if writeJSON {
		reportPaths = jsonPath
	}
	fmt.Printf("clicktester: tasks=%d, passed=%d, failed=%d, report=%s\n",
		result.Total, result.Passed, result.Failed, reportPaths)
	if result.Failed > 0 {
		for _, r := range result.Results {
			if !r.Pass {
				fmt.Fprintf(os.Stderr, "  FAIL %s (%s): %s\n", r.Name, r.Type, r.Error)
			}
		}
	}
}
