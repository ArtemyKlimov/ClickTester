// Package config загружает и валидирует конфигурацию приложения (YAML/JSON).
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config — корневая структура конфигурации.
type Config struct {
	ClickHouse     ClickHouse     `yaml:"clickhouse"`
	TestParams     TestParams     `yaml:"test_params"`
	Execution      Execution     `yaml:"execution"`
	Report         Report        `yaml:"report"`
	StructureChecks []StructureCheck `yaml:"structure_checks"`
	QueryTemplates []QueryTemplate `yaml:"query_templates"`
}

// ClickHouse — параметры подключения к ClickHouse.
type ClickHouse struct {
	Host      string `yaml:"host"`
	Port      int    `yaml:"port"`
	Database  string `yaml:"database"`
	User      string `yaml:"user"`
	Password  string `yaml:"password"`
	TableName string `yaml:"table_name"`
	Secure    bool   `yaml:"secure"`
}

// TestParams — параметры для подстановки в шаблоны запросов.
type TestParams struct {
	ProjectCode string `yaml:"projectCode"`
	AppName     string `yaml:"appName"`
	Namespace   string `yaml:"namespace"`
	Level       string `yaml:"level"`
	TextToken   string `yaml:"text_token"`
}

// Execution — параметры выполнения тестов.
type Execution struct {
	Workers         int `yaml:"workers"`
	QueryTimeoutSec int `yaml:"query_timeout_sec"`
}

// Report — параметры отчёта.
type Report struct {
	OutputPath string     `yaml:"output_path"`
	Thresholds Thresholds `yaml:"thresholds"`
}

// Thresholds — пороги для статусов ok/warn/fail.
type Thresholds struct {
	GranulesWarn   int `yaml:"granules_warn"`
	GranulesFail   int `yaml:"granules_fail"`
	ReadRowsWarn   int `yaml:"read_rows_warn"`
}

// StructureCheck — одна структурная проверка (партиции, индексы, проекции и т.д.).
type StructureCheck struct {
	Name        string `yaml:"name"`
	Type        string `yaml:"type"` // partitions, indexes, projections, granules_settings
	Description string `yaml:"description"`
}

// QueryTemplate — шаблон запроса с опциями сбора метрик.
type QueryTemplate struct {
	Name           string `yaml:"name"`
	Description    string `yaml:"description"`
	Query          string `yaml:"query"`
	CollectExplain bool   `yaml:"collect_explain"`
	CollectStats   bool   `yaml:"collect_stats"`
}

// Load читает конфиг из файла и парсит YAML.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}

	if err := validate(&cfg); err != nil {
		return nil, err
	}

	setDefaults(&cfg)
	return &cfg, nil
}

func validate(c *Config) error {
	if c.ClickHouse.Host == "" {
		return fmt.Errorf("clickhouse.host is required")
	}
	if c.ClickHouse.Database == "" {
		return fmt.Errorf("clickhouse.database is required")
	}
	if c.ClickHouse.Port == 0 {
		c.ClickHouse.Port = 9000 // native protocol (HTTP = 8123)
	}
	if len(c.StructureChecks) == 0 && len(c.QueryTemplates) == 0 {
		return fmt.Errorf("at least one structure_checks or query_templates entry is required")
	}
	return nil
}

func setDefaults(c *Config) {
	if c.Execution.Workers <= 0 {
		c.Execution.Workers = 1
	}
	if c.Report.OutputPath == "" {
		c.Report.OutputPath = "reports/report.html"
	}
}
