// Package chclient — клиент ClickHouse (native-протокол: подключение, запросы, EXPLAIN, сбор метрик).
package chclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Client интерфейс для выполнения запросов к ClickHouse.
type Client interface {
	Ping(ctx context.Context) error
	Query(ctx context.Context, query string) (rows int, readRows, readBytes uint64, err error)
	Explain(ctx context.Context, query string) (explainText string, err error)
	Close() error
}

// ConnectOptions — параметры подключения (из конфига).
type ConnectOptions struct {
	Host     string
	Port     int
	Database string
	User     string
	Password string
	Secure   bool
}

// nativeClient — реализация Client через clickhouse-go/v2 (native).
type nativeClient struct {
	conn driver.Conn
}

// New создаёт клиент и подключается к ClickHouse.
func New(ctx context.Context, opt ConnectOptions) (Client, error) {
	if opt.Port == 0 {
		opt.Port = 9000
	}
	addr := fmt.Sprintf("%s:%d", opt.Host, opt.Port)

	opts := &clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: opt.Database,
			Username: opt.User,
			Password: opt.Password,
		},
		DialTimeout: 10 * time.Second,
		MaxOpenConns: 2,
		MaxIdleConns: 1,
	}
	if opt.Secure {
		opts.TLS = &tls.Config{InsecureSkipVerify: true}
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("clickhouse ping: %w", err)
	}

	return &nativeClient{conn: conn}, nil
}

// Ping проверяет соединение.
func (c *nativeClient) Ping(ctx context.Context) error {
	return c.conn.Ping(ctx)
}

// Query выполняет запрос и возвращает число строк результата, read_rows и read_bytes (из Progress).
func (c *nativeClient) Query(ctx context.Context, query string) (rows int, readRows, readBytes uint64, err error) {
	var progressMu sync.Mutex
	progressRows := uint64(0)
	progressBytes := uint64(0)

	ctx = clickhouse.Context(ctx, clickhouse.WithProgress(func(p *clickhouse.Progress) {
		progressMu.Lock()
		progressRows += p.Rows
		progressBytes += p.Bytes
		progressMu.Unlock()
	}))

	rowIter, err := c.conn.Query(ctx, query)
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() { _ = rowIter.Close() }()

	for rowIter.Next() {
		rows++
	}
	if err = rowIter.Err(); err != nil {
		return rows, 0, 0, err
	}

	progressMu.Lock()
	readRows = progressRows
	readBytes = progressBytes
	progressMu.Unlock()

	return rows, readRows, readBytes, nil
}

// Explain выполняет EXPLAIN indexes=1 для запроса и возвращает текст вывода; из текста можно извлечь гранулы через ExtractGranules.
func (c *nativeClient) Explain(ctx context.Context, query string) (string, error) {
	explainQuery := "EXPLAIN indexes=1 " + query
	rowIter, err := c.conn.Query(ctx, explainQuery)
	if err != nil {
		return "", err
	}
	defer func() { _ = rowIter.Close() }()

	cols := rowIter.Columns()
	dest := make([]any, len(cols))
	for i := range dest {
		var s string
		dest[i] = &s
	}

	var sb strings.Builder
	for rowIter.Next() {
		if err := rowIter.Scan(dest...); err != nil {
			continue
		}
		for i, d := range dest {
			if i > 0 {
				sb.WriteString("\t")
			}
			if p, ok := d.(*string); ok && p != nil {
				sb.WriteString(*p)
			}
		}
		sb.WriteString("\n")
	}
	if err = rowIter.Err(); err != nil {
		return "", err
	}

	return sb.String(), nil
}

// Close закрывает соединение.
func (c *nativeClient) Close() error {
	return c.conn.Close()
}

// GranulesRegex — паттерн для строк вида "Granules: 123/456".
var GranulesRegex = regexp.MustCompile(`Granules:\s*(\d+)/(\d+)`)

// ExtractGranules извлекает минимальное число гранул (первое число в паре X/Y) из вывода EXPLAIN.
func ExtractGranules(explainText string) int {
	matches := GranulesRegex.FindAllStringSubmatch(explainText, -1)
	if len(matches) == 0 {
		return 0
	}
	minVal := 0
	for _, m := range matches {
		if len(m) < 2 {
			continue
		}
		var x int
		if _, err := fmt.Sscanf(m[1], "%d", &x); err == nil {
			if minVal == 0 || x < minVal {
				minVal = x
			}
		}
	}
	return minVal
}
