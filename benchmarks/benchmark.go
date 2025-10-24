package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"slices"
	"sync"
	"time"

	"storemy/pkg/database"
)

type BenchmarkResult struct {
	QueryType         string        `json:"query_type"`
	Query             string        `json:"query"`
	Iterations        int           `json:"iterations"`
	TotalDuration     time.Duration `json:"total_duration_ns"`
	AvgDuration       time.Duration `json:"avg_duration_ns"`
	MinDuration       time.Duration `json:"min_duration_ns"`
	MaxDuration       time.Duration `json:"max_duration_ns"`
	MedianDuration    time.Duration `json:"median_duration_ns"`
	P95Duration       time.Duration `json:"p95_duration_ns"`
	P99Duration       time.Duration `json:"p99_duration_ns"`
	QueriesPerSecond  float64       `json:"queries_per_second"`
	ConcurrentQueries int           `json:"concurrent_queries"`
	SuccessCount      int           `json:"success_count"`
	ErrorCount        int           `json:"error_count"`
	Timestamp         time.Time     `json:"timestamp"`
}

type BenchmarkReport struct {
	StartTime     time.Time         `json:"start_time"`
	EndTime       time.Time         `json:"end_time"`
	TotalDuration time.Duration     `json:"total_duration"`
	Results       []BenchmarkResult `json:"results"`
	DatabaseName  string            `json:"database_name"`
	DataDir       string            `json:"data_dir"`
}

func main() {
	outputDir := os.Getenv("BENCHMARK_OUTPUT")
	if outputDir == "" {
		outputDir = "./benchmark-results"
	}

	iterations := 1000
	if iter := os.Getenv("BENCHMARK_ITERATIONS"); iter != "" {
		fmt.Sscanf(iter, "%d", &iterations)
	}

	concurrentQueries := 10
	if conc := os.Getenv("BENCHMARK_CONCURRENT_QUERIES"); conc != "" {
		fmt.Sscanf(conc, "%d", &concurrentQueries)
	}

	dbName := os.Getenv("DB_NAME")
	if dbName == "" {
		dbName = "benchmark_db"
	}

	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "/app/benchmark_data"
	}

	logDir := os.Getenv("LOG_DIR")
	if logDir == "" {
		logDir = "/app/benchmark_logs"
	}

	os.MkdirAll(outputDir, 0755)
	os.MkdirAll(logDir, 0755)

	walPath := fmt.Sprintf("%s/wal.log", logDir)

	log.Printf("Starting benchmark suite...")
	log.Printf("Database: %s, Data Directory: %s", dbName, dataDir)
	log.Printf("Iterations: %d, Concurrent Queries: %d", iterations, concurrentQueries)

	db, err := database.NewDatabase(dbName, dataDir, walPath)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	if err := setupBenchmarkData(db); err != nil {
		log.Fatalf("Failed to setup benchmark data: %v", err)
	}

	report := BenchmarkReport{
		StartTime:    time.Now(),
		DatabaseName: dbName,
		DataDir:      dataDir,
		Results:      []BenchmarkResult{},
	}

	benchmarks := []struct {
		name  string
		query string
	}{
		{"Simple SELECT", "SELECT * FROM users LIMIT 100"},
		{"SELECT with WHERE", "SELECT * FROM users WHERE age > 25"},
		{"SELECT with JOIN", "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id LIMIT 100"},
		{"Aggregate COUNT", "SELECT COUNT(*) FROM users"},
		{"Aggregate with GROUP BY", "SELECT age, COUNT(*) FROM users GROUP BY age"},
		{"INSERT", "INSERT INTO users (id, name, age, email) VALUES (99999, 'Bench User', 30, 'bench@test.com')"},
		{"UPDATE", "UPDATE users SET age = 31 WHERE id = 99999"},
		{"DELETE", "DELETE FROM users WHERE id = 99999"},
	}

	for _, bench := range benchmarks {
		log.Printf("\n=== Running benchmark: %s ===", bench.name)

		// Sequential benchmark
		seqResult := runBenchmark(db, bench.name, bench.query, iterations, 1)
		report.Results = append(report.Results, seqResult)
		printBenchmarkResult(seqResult)

		// Concurrent benchmark (for read queries)
		if bench.name != "INSERT" && bench.name != "UPDATE" && bench.name != "DELETE" {
			concName := bench.name + " (Concurrent)"
			concResult := runBenchmark(db, concName, bench.query, iterations, concurrentQueries)
			report.Results = append(report.Results, concResult)
			printBenchmarkResult(concResult)
		}
	}

	report.EndTime = time.Now()
	report.TotalDuration = report.EndTime.Sub(report.StartTime)

	// Save report
	timestamp := time.Now().Format("20060102_150405")
	jsonFile := fmt.Sprintf("%s/benchmark_report_%s.json", outputDir, timestamp)
	htmlFile := fmt.Sprintf("%s/benchmark_report_%s.html", outputDir, timestamp)

	saveJSONReport(report, jsonFile)
	saveHTMLReport(report, htmlFile)

	log.Printf("\n=== Benchmark Complete ===")
	log.Printf("Total Duration: %v", report.TotalDuration)
	log.Printf("Results saved to: %s", outputDir)
}

func setupBenchmarkData(db *database.Database) error {
	log.Println("Setting up benchmark data...")

	queries := []string{
		"CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name TEXT, age INT, email TEXT)",
		"CREATE TABLE IF NOT EXISTS orders (id INT PRIMARY KEY, user_id INT, amount DECIMAL, order_date TEXT)",
	}

	for _, query := range queries {
		if _, err := db.ExecuteQuery(query); err != nil {
			log.Printf("Setup query warning: %v", err)
		}
	}

	result, err := db.ExecuteQuery("SELECT COUNT(*) FROM users")

	shouldInsertData := true
	if err == nil && len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
		var count int64
		fmt.Sscanf(result.Rows[0][0], "%d", &count)
		if count >= 1000 {
			shouldInsertData = false
		}
	}

	if shouldInsertData {
		log.Println("Inserting sample data...")
		for i := 1; i <= 1000; i++ {
			insertQuery := fmt.Sprintf(
				"INSERT INTO users (id, name, age, email) VALUES (%d, 'User%d', %d, 'user%d@example.com')",
				i, i, 20+i%50, i,
			)
			db.ExecuteQuery(insertQuery)

			if i <= 500 {
				orderQuery := fmt.Sprintf(
					"INSERT INTO orders (id, user_id, amount, order_date) VALUES (%d, %d, %d.99, '2025-01-%02d')",
					i, i, 100+i%900, 1+i%28,
				)
				db.ExecuteQuery(orderQuery)
			}
		}
		log.Println("Sample data inserted successfully")
	}

	return nil
}

func runBenchmark(db *database.Database, queryType, query string, iterations, concurrent int) BenchmarkResult {
	durations := make([]time.Duration, 0, iterations)
	var mu sync.Mutex
	var wg sync.WaitGroup

	successCount := 0
	errorCount := 0
	startTime := time.Now()

	sem := make(chan struct{}, concurrent)

	for range iterations {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			queryStart := time.Now()
			_, err := db.ExecuteQuery(query)
			duration := time.Since(queryStart)

			mu.Lock()
			durations = append(durations, duration)
			if err != nil {
				errorCount++
			} else {
				successCount++
			}
			mu.Unlock()
		}()
	}

	wg.Wait()
	totalDuration := time.Since(startTime)

	slices.Sort(durations)

	var sum time.Duration
	minDur := durations[0]
	maxDur := durations[0]

	for _, d := range durations {
		sum += d
		if d < minDur {
			minDur = d
		}
		if d > maxDur {
			maxDur = d
		}
	}

	avgDur := sum / time.Duration(len(durations))
	medianDur := durations[len(durations)/2]
	p95Dur := durations[int(float64(len(durations))*0.95)]
	p99Dur := durations[int(float64(len(durations))*0.99)]
	qps := float64(iterations) / totalDuration.Seconds()

	return BenchmarkResult{
		QueryType:         queryType,
		Query:             query,
		Iterations:        iterations,
		TotalDuration:     totalDuration,
		AvgDuration:       avgDur,
		MinDuration:       minDur,
		MaxDuration:       maxDur,
		MedianDuration:    medianDur,
		P95Duration:       p95Dur,
		P99Duration:       p99Dur,
		QueriesPerSecond:  qps,
		ConcurrentQueries: concurrent,
		SuccessCount:      successCount,
		ErrorCount:        errorCount,
		Timestamp:         time.Now(),
	}
}

func printBenchmarkResult(result BenchmarkResult) {
	log.Printf("  Total Duration: %v", result.TotalDuration)
	log.Printf("  Avg Query Time: %v", result.AvgDuration)
	log.Printf("  Min/Max: %v / %v", result.MinDuration, result.MaxDuration)
	log.Printf("  Median: %v, P95: %v, P99: %v", result.MedianDuration, result.P95Duration, result.P99Duration)
	log.Printf("  Queries/Sec: %.2f", result.QueriesPerSecond)
	log.Printf("  Success/Error: %d / %d", result.SuccessCount, result.ErrorCount)
}

func saveJSONReport(report BenchmarkReport, filename string) {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		log.Printf("Error marshaling report: %v", err)
		return
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		log.Printf("Error writing JSON report: %v", err)
		return
	}

	log.Printf("JSON report saved: %s", filename)
}

func saveHTMLReport(report BenchmarkReport, filename string) {
	html := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head>
	<title>StoreMy Benchmark Report - %s</title>
	<style>
		body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
		.container { max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
		h1 { color: #333; border-bottom: 3px solid #4CAF50; padding-bottom: 10px; }
		h2 { color: #555; margin-top: 30px; }
		.summary { background: #e8f5e9; padding: 15px; border-radius: 5px; margin: 20px 0; }
		table { width: 100%%; border-collapse: collapse; margin: 20px 0; }
		th { background: #4CAF50; color: white; padding: 12px; text-align: left; }
		td { padding: 10px; border-bottom: 1px solid #ddd; }
		tr:hover { background: #f5f5f5; }
		.metric { display: inline-block; margin: 10px 20px 10px 0; }
		.metric-label { font-weight: bold; color: #666; }
		.metric-value { color: #4CAF50; font-size: 1.2em; }
		.query-cell { font-family: monospace; font-size: 0.9em; max-width: 400px; }
	</style>
</head>
<body>
	<div class="container">
		<h1>StoreMy Benchmark Report</h1>

		<div class="summary">
			<div class="metric">
				<span class="metric-label">Start Time:</span>
				<span class="metric-value">%s</span>
			</div>
			<div class="metric">
				<span class="metric-label">End Time:</span>
				<span class="metric-value">%s</span>
			</div>
			<div class="metric">
				<span class="metric-label">Total Duration:</span>
				<span class="metric-value">%v</span>
			</div>
			<div class="metric">
				<span class="metric-label">Database:</span>
				<span class="metric-value">%s</span>
			</div>
		</div>

		<h2>Benchmark Results</h2>
		<table>
			<tr>
				<th>Query Type</th>
				<th>Query</th>
				<th>Iterations</th>
				<th>Concurrent</th>
				<th>Avg Time</th>
				<th>Min/Max</th>
				<th>P95</th>
				<th>QPS</th>
				<th>Success Rate</th>
			</tr>
`,
		report.StartTime.Format("2006-01-02 15:04:05"),
		report.StartTime.Format("2006-01-02 15:04:05"),
		report.EndTime.Format("2006-01-02 15:04:05"),
		report.TotalDuration,
		report.DatabaseName,
	)

	for _, result := range report.Results {
		successRate := float64(result.SuccessCount) / float64(result.Iterations) * 100
		html += fmt.Sprintf(`
			<tr>
				<td><strong>%s</strong></td>
				<td class="query-cell">%s</td>
				<td>%d</td>
				<td>%d</td>
				<td>%v</td>
				<td>%v / %v</td>
				<td>%v</td>
				<td>%.2f</td>
				<td>%.1f%%</td>
			</tr>
`,
			result.QueryType,
			result.Query,
			result.Iterations,
			result.ConcurrentQueries,
			result.AvgDuration,
			result.MinDuration,
			result.MaxDuration,
			result.P95Duration,
			result.QueriesPerSecond,
			successRate,
		)
	}

	html += `
		</table>
	</div>
</body>
</html>
`

	if err := os.WriteFile(filename, []byte(html), 0644); err != nil {
		log.Printf("Error writing HTML report: %v", err)
		return
	}

	log.Printf("HTML report saved: %s", filename)
}
