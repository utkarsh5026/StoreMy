package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"storemy/pkg/database"
)

type MetricsCollector struct {
	database       *database.Database
	queryCount     int64
	queryDurations []time.Duration
	errorCount     int64
	lastQueryTime  time.Time
	mu             sync.RWMutex
}

func NewMetricsCollector(db *database.Database) *MetricsCollector {
	return &MetricsCollector{
		database:       db,
		queryDurations: make([]time.Duration, 0),
		lastQueryTime:  time.Now(),
	}
}

func (mc *MetricsCollector) RecordQuery(duration time.Duration, err error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.queryCount++
	mc.queryDurations = append(mc.queryDurations, duration)
	mc.lastQueryTime = time.Now()

	// Keep only last 1000 durations to avoid memory issues
	if len(mc.queryDurations) > 1000 {
		mc.queryDurations = mc.queryDurations[len(mc.queryDurations)-1000:]
	}

	if err != nil {
		mc.errorCount++
	}
}

func (mc *MetricsCollector) GetMetrics() string {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	var totalDuration time.Duration
	for _, d := range mc.queryDurations {
		totalDuration += d
	}

	avgDuration := float64(0)
	if len(mc.queryDurations) > 0 {
		avgDuration = float64(totalDuration.Microseconds()) / float64(len(mc.queryDurations))
	}

	qps := float64(0)
	if len(mc.queryDurations) > 0 {
		timeSinceFirst := time.Since(mc.lastQueryTime.Add(-time.Duration(len(mc.queryDurations)) * time.Second))
		if timeSinceFirst > 0 {
			qps = float64(len(mc.queryDurations)) / timeSinceFirst.Seconds()
		}
	}

	// Get database stats
	tableCount := 0
	rowCount := int64(0)

	// Try to get table count - Note: information_schema might not be implemented yet
	// For now, we'll just report 0 or implement a simple catalog query later
	// if result, err := mc.database.ExecuteQuery("SELECT COUNT(*) FROM information_schema.tables"); err == nil && len(result.Rows) > 0 {
	// 	if count, ok := result.Rows[0][0].(int64); ok {
	// 		tableCount = int(count)
	// 	}
	// }

	// Prometheus format metrics
	metrics := fmt.Sprintf(`# HELP storemy_queries_total Total number of queries executed
# TYPE storemy_queries_total counter
storemy_queries_total %d

# HELP storemy_query_errors_total Total number of query errors
# TYPE storemy_query_errors_total counter
storemy_query_errors_total %d

# HELP storemy_query_duration_microseconds Average query duration in microseconds
# TYPE storemy_query_duration_microseconds gauge
storemy_query_duration_microseconds %.2f

# HELP storemy_queries_per_second Current queries per second rate
# TYPE storemy_queries_per_second gauge
storemy_queries_per_second %.2f

# HELP storemy_table_count Number of tables in the database
# TYPE storemy_table_count gauge
storemy_table_count %d

# HELP storemy_row_count_estimate Estimated total rows across all tables
# TYPE storemy_row_count_estimate gauge
storemy_row_count_estimate %d

# HELP storemy_up Database up status (1 = up, 0 = down)
# TYPE storemy_up gauge
storemy_up 1

# HELP storemy_last_query_timestamp_seconds Unix timestamp of last query
# TYPE storemy_last_query_timestamp_seconds gauge
storemy_last_query_timestamp_seconds %d
`,
		mc.queryCount,
		mc.errorCount,
		avgDuration,
		qps,
		tableCount,
		rowCount,
		mc.lastQueryTime.Unix(),
	)

	return metrics
}

func (mc *MetricsCollector) StartSimulation() {
	// Simulate some queries for demo purposes
	go func() {
		queries := []string{
			"SELECT * FROM users LIMIT 10",
			"SELECT COUNT(*) FROM users",
			"SELECT * FROM orders WHERE user_id = 1",
		}

		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			for _, query := range queries {
				start := time.Now()
				_, err := mc.database.ExecuteQuery(query)
				duration := time.Since(start)
				mc.RecordQuery(duration, err)
			}
		}
	}()
}

func main() {
	dbName := os.Getenv("DB_NAME")
	if dbName == "" {
		dbName = "storemy_db"
	}

	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "/app/data"
	}

	logDir := filepath.Clean(os.Getenv("LOG_DIR"))
	if logDir == "." {
		logDir = "/app/logs"
	}

	_ = os.MkdirAll(logDir, 0o750) // #nosec G703
	walPath := filepath.Join(logDir, "wal.log")

	metricsPort := os.Getenv("METRICS_PORT")
	if metricsPort == "" {
		metricsPort = "8080"
	}

	log.Printf("Starting StoreMy Metrics Exporter...")
	log.Printf("Database: %s, Data Directory: %s", dbName, dataDir) // #nosec G706
	log.Printf("Metrics Port: %s", metricsPort)                     // #nosec G706

	db, err := database.NewDatabase(dbName, dataDir, walPath)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	collector := NewMetricsCollector(db)

	collector.StartSimulation()

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, collector.GetMetrics())
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "OK")
	})

	srv := &http.Server{
		Addr:         ":" + metricsPort,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("Metrics available at http://localhost:%s/metrics", metricsPort) // #nosec G706
	log.Fatal(srv.ListenAndServe())
}
