// Package chclient — клиент ClickHouse (native-протокол: подключение, запросы, EXPLAIN, сбор метрик).
package chclient

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"golang.org/x/crypto/pkcs12"
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
	Host          string
	Port          int
	Database      string
	User          string
	Password      string
	Secure         bool   // использовать TLS
	TLSSkipVerify  *bool  // не проверять сертификат (при secure по умолчанию true)
	TLSCAFile      string // путь к PEM с CA (опционально)
	TLSPfxFile     string // путь к PFX/P12 клиентскому сертификату (mTLS)
	TLSPfxPassword string // пароль к PFX (опционально)
}

// nativeClient — реализация Client через clickhouse-go/v2 (native или HTTP/HTTPS).
type nativeClient struct {
	conn    driver.Conn
	useHTTP bool // при true Progress не приходит от драйвера; используем system.query_log для read_rows/read_bytes
}

// Порты HTTP/HTTPS интерфейса ClickHouse (в отличие от native 9000/9440).
const (
	PortHTTP  = 8123
	PortHTTPS = 8443
)

// New создаёт клиент и подключается к ClickHouse.
// Для порта 8443 используется протокол HTTPS (HTTP + TLS), для 8123 — HTTP, иначе — native (9000/9440).
func New(ctx context.Context, opt ConnectOptions) (Client, error) {
	if opt.Port == 0 {
		opt.Port = 9000
	}
	addr := fmt.Sprintf("%s:%d", opt.Host, opt.Port)

	useHTTP := opt.Port == PortHTTP || opt.Port == PortHTTPS
	maxOpen := 2
	if useHTTP {
		maxOpen = 1 // один контур: основной запрос и lookup в query_log на одной ноде (query_log локальный)
	}
	opts := &clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: opt.Database,
			Username: opt.User,
			Password: opt.Password,
		},
		DialTimeout: 10 * time.Second,
		MaxOpenConns: maxOpen,
		MaxIdleConns: 1,
	}

	if useHTTP {
		opts.Protocol = clickhouse.HTTP
		opts.Settings = clickhouse.Settings{"session_id": "ct-" + hex.EncodeToString(mustRand(8))}
	}

	if opt.Secure || opt.Port == PortHTTPS {
		tlsCfg, err := buildTLSConfig(opt.TLSSkipVerify, opt.TLSCAFile, opt.TLSPfxFile, opt.TLSPfxPassword)
		if err != nil {
			return nil, fmt.Errorf("tls: %w", err)
		}
		opts.TLS = tlsCfg
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("clickhouse ping: %w", err)
	}

	return &nativeClient{conn: conn, useHTTP: useHTTP}, nil
}

// buildTLSConfig собирает tls.Config: CA для проверки сервера, опционально клиентский сертификат из PFX/P12.
func buildTLSConfig(skipVerify *bool, caFile, pfxFile, pfxPassword string) (*tls.Config, error) {
	insecure := skipVerify == nil || *skipVerify
	cfg := &tls.Config{InsecureSkipVerify: insecure, MinVersion: tls.VersionTLS12}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read ca file: %w", err)
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(pem)
		cfg.RootCAs = pool
		cfg.InsecureSkipVerify = false
	}
	if pfxFile != "" {
		pfxData, err := os.ReadFile(pfxFile)
		if err != nil {
			return nil, fmt.Errorf("read pfx file: %w", err)
		}
		cert, err := loadPFXAsTLSCert(pfxData, pfxPassword)
		if err != nil {
			return nil, fmt.Errorf("decode pfx: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// loadPFXAsTLSCert загружает клиентский сертификат из PFX/P12 (поддерживает цепочки и несколько «мешков»).
func loadPFXAsTLSCert(pfxData []byte, password string) (tls.Certificate, error) {
	blocks, err := pkcs12.ToPEM(pfxData, password)
	if err != nil {
		return tls.Certificate{}, err
	}
	var pemData []byte
	for _, b := range blocks {
		pemData = append(pemData, pem.EncodeToMemory(b)...)
	}
	return tls.X509KeyPair(pemData, pemData)
}

// Ping проверяет соединение.
func (c *nativeClient) Ping(ctx context.Context) error {
	return c.conn.Ping(ctx)
}

// Query выполняет запрос и возвращает число строк результата, read_rows и read_bytes.
// При native — из Progress; при HTTP/HTTPS — по query_id из system.query_log.
// Для HTTP передаём свой query_id в URL (?query_id=...) через WithQueryID; драйвер добавляет его в запрос.
func (c *nativeClient) Query(ctx context.Context, query string) (rows int, readRows, readBytes uint64, err error) {
	queryID := generateQueryID()
	var progressMu sync.Mutex
	progressRows := uint64(0)
	progressBytes := uint64(0)

	ctx = clickhouse.Context(ctx,
		clickhouse.WithQueryID(queryID),
		clickhouse.WithProgress(func(p *clickhouse.Progress) {
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

	if c.useHTTP && readRows == 0 && readBytes == 0 {
		_ = rowIter.Close()
		readRows, readBytes, _ = c.queryLogStats(ctx, queryID)
	}

	return rows, readRows, readBytes, nil
}

func generateQueryID() string {
	return "ct-" + hex.EncodeToString(mustRand(16))
}

func mustRand(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return []byte(fmt.Sprintf("%d", time.Now().UnixNano()))
	}
	return b
}

// queryLogStats возвращает read_rows и read_bytes из system.query_log по query_id (для HTTP).
// Для одной ноды: session_id + MaxOpenConns:1 держат запросы на одной ноде, SYSTEM FLUSH LOGS делает запись видимой.
// Для кластера: при подключении к одной ноде — то же; при балансировщике — fallback по clusterAllReplicas.
func (c *nativeClient) queryLogStats(ctx context.Context, queryID string) (readRows, readBytes uint64, err error) {
	bg, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var lastErr error
	tryQuery := func(q string) (uint64, uint64, bool) {
		rowIter, qErr := c.conn.Query(bg, q)
		if qErr != nil {
			lastErr = qErr
			return 0, 0, false
		}
		defer func() { _ = rowIter.Close() }()
		if !rowIter.Next() {
			lastErr = nil
			return 0, 0, false
		}
		var r, b uint64
		if qErr = rowIter.Scan(&r, &b); qErr != nil {
			lastErr = qErr
			return 0, 0, false
		}
		lastErr = nil
		return r, b, true
	}

	_ = c.conn.Exec(bg, "SYSTEM FLUSH LOGS")

	qLocal := fmt.Sprintf("SELECT read_rows, read_bytes FROM system.query_log WHERE query_id = '%s' AND type = 2 LIMIT 1", queryID)
	for _, d := range []time.Duration{0, 50 * time.Millisecond, 150 * time.Millisecond} {
		if d > 0 {
			time.Sleep(d)
		}
		if r, b, ok := tryQuery(qLocal); ok {
			return r, b, nil
		}
	}

	// Кластер: при подключении через балансировщик запрос мог уйти на другую ноду; ищем по всем репликам
	for _, clusterName := range []string{"default", "cluster"} {
		q := fmt.Sprintf("SELECT read_rows, read_bytes FROM clusterAllReplicas('%s', system.query_log) WHERE query_id = '%s' AND type = 2 LIMIT 1 SETTINGS skip_unavailable_shards = 1", clusterName, queryID)
		if r, b, ok := tryQuery(q); ok {
			return r, b, nil
		}
	}

	qLast := "SELECT read_rows, read_bytes FROM system.query_log WHERE user = currentUser() AND type = 2 AND event_time > now() - 10 AND position(query, 'system.query_log') = 0 ORDER BY event_time DESC LIMIT 1"
	if r, b, ok := tryQuery(qLast); ok {
		return r, b, nil
	}

	if lastErr != nil {
		log.Printf("[clicktester] HTTP: запрос к query_log: %v", lastErr)
	}
	log.Printf("[clicktester] HTTP: read_rows/read_bytes не получены (query_id=%s). Нужны: log_queries=1, права на system.query_log и при необходимости SYSTEM FLUSH LOGS.", queryID)
	return 0, 0, nil
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

// ProjectionUsed возвращает true, если в выводе EXPLAIN встречается упоминание проекции (Projection).
func ProjectionUsed(explainText string) bool {
	return strings.Contains(strings.ToLower(explainText), "projection")
}

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
