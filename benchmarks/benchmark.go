package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"storemy/pkg/database"
)

// BenchmarkResult captures detailed performance metrics for a single benchmark test.
// It includes timing statistics, throughput metrics, and success/error counts.
type BenchmarkResult struct {
	QueryType         string        `json:"query_type"`         // Descriptive name of the benchmark test
	Query             string        `json:"query"`              // The actual SQL query being benchmarked
	Iterations        int           `json:"iterations"`         // Total number of times the query was executed
	TotalDuration     time.Duration `json:"total_duration_ns"`  // Total time taken for all iterations
	AvgDuration       time.Duration `json:"avg_duration_ns"`    // Average time per query execution
	MinDuration       time.Duration `json:"min_duration_ns"`    // Fastest query execution time
	MaxDuration       time.Duration `json:"max_duration_ns"`    // Slowest query execution time
	MedianDuration    time.Duration `json:"median_duration_ns"` // Median query execution time
	P95Duration       time.Duration `json:"p95_duration_ns"`    // 95th percentile execution time
	P99Duration       time.Duration `json:"p99_duration_ns"`    // 99th percentile execution time
	QueriesPerSecond  float64       `json:"queries_per_second"` // Throughput metric
	ConcurrentQueries int           `json:"concurrent_queries"` // Number of concurrent goroutines
	SuccessCount      int           `json:"success_count"`      // Number of successful query executions
	ErrorCount        int           `json:"error_count"`        // Number of failed query executions
	ErrorSamples      []string      `json:"error_samples"`      // Sample error messages for debugging
	Timestamp         time.Time     `json:"timestamp"`          // When this benchmark was executed
}

// BenchmarkReport aggregates results from all benchmark tests into a single report.
// It includes metadata about the test environment and timing information.
type BenchmarkReport struct {
	StartTime     time.Time         `json:"start_time"`     // When the benchmark suite started
	EndTime       time.Time         `json:"end_time"`       // When the benchmark suite completed
	TotalDuration time.Duration     `json:"total_duration"` // Total time for entire benchmark suite
	Results       []BenchmarkResult `json:"results"`        // Individual benchmark test results
	DatabaseName  string            `json:"database_name"`  // Name of the database being benchmarked
	DataDir       string            `json:"data_dir"`       // Directory where database files are stored
}

// main orchestrates the entire benchmark suite execution.
// It reads configuration from environment variables, initializes the database,
// runs all benchmarks, and generates both JSON and HTML reports.
//
// Environment variables:
//   - BENCHMARK_OUTPUT: Directory for output reports (default: ./benchmark-results)
//   - BENCHMARK_ITERATIONS: Number of iterations per benchmark (default: 1000)
//   - BENCHMARK_CONCURRENT_QUERIES: Number of concurrent queries (default: 10)
//   - DB_NAME: Database name (default: benchmark_db)
//   - DATA_DIR: Data directory path (default: /app/benchmark_data)
//   - LOG_DIR: Log directory path (default: /app/benchmark_logs)
func main() {
	outputDir := filepath.Clean(os.Getenv("BENCHMARK_OUTPUT"))
	if outputDir == "." {
		outputDir = "./benchmark-results"
	}

	iterations := 1000
	if iter := os.Getenv("BENCHMARK_ITERATIONS"); iter != "" {
		_, _ = fmt.Sscanf(iter, "%d", &iterations)
	}

	concurrentQueries := 10
	if conc := os.Getenv("BENCHMARK_CONCURRENT_QUERIES"); conc != "" {
		_, _ = fmt.Sscanf(conc, "%d", &concurrentQueries)
	}

	dbName := os.Getenv("DB_NAME")
	if dbName == "" {
		dbName = "benchmark_db"
	}

	dataDir := filepath.Clean(os.Getenv("DATA_DIR"))
	if dataDir == "." {
		dataDir = "/app/benchmark_data"
	}

	logDir := filepath.Clean(os.Getenv("LOG_DIR"))
	if logDir == "." {
		logDir = "/app/benchmark_logs"
	}

	_ = os.MkdirAll(outputDir, 0o750) // #nosec G703
	_ = os.MkdirAll(logDir, 0o750)    // #nosec G703

	walPath := filepath.Join(logDir, "wal.log")

	log.Printf("Starting benchmark suite...")
	log.Printf("Database: %s, Data Directory: %s", dbName, dataDir) // #nosec G706
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
		// Fast tests first to show failures quickly
		{"INSERT", "INSERT INTO users (id, name, age, email) VALUES (99999, 'Bench User', 30, 'bench@test.com')"},
		{"UPDATE", "UPDATE users SET age = 31 WHERE id = 99999"},
		{"DELETE", "DELETE FROM users WHERE id = 99999"},
		{"Simple SELECT", "SELECT * FROM users LIMIT 100"},
		{"SELECT with WHERE", "SELECT * FROM users WHERE age > 25"},
		// Slower tests at the end
		{"Aggregate COUNT", "SELECT COUNT(id) FROM users"},
		{"Aggregate with GROUP BY", "SELECT age, COUNT(id) FROM users GROUP BY age"},
		{"SELECT with JOIN", "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id LIMIT 100"},
	}

	for _, bench := range benchmarks {
		log.Printf("%s", "\n"+strings.Repeat("=", 80))
		log.Printf("TEST: %s", bench.name)
		log.Printf("%s", strings.Repeat("=", 80))
		log.Printf("Query: %s", bench.query)
		log.Printf("")

		// Sequential benchmark
		log.Printf("→ Running sequential test (%d iterations)...", iterations)
		seqResult := runBenchmark(db, bench.name, bench.query, iterations, 1)
		report.Results = append(report.Results, seqResult)
		printBenchmarkResult(seqResult)

		if bench.name != "INSERT" && bench.name != "UPDATE" && bench.name != "DELETE" {
			log.Printf("")
			log.Printf("→ Running concurrent test (%d parallel queries, %d iterations)...", concurrentQueries, iterations)
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

	log.Printf("%s", "\n"+strings.Repeat("=", 80))
	log.Printf("BENCHMARK SUITE COMPLETE")
	log.Printf("%s", strings.Repeat("=", 80))
	log.Printf("")
	log.Printf("  Summary:")
	log.Printf("    Total Duration:     %s", formatDuration(report.TotalDuration))
	log.Printf("    Tests Run:          %d", len(report.Results))
	log.Printf("    Database:           %s", dbName) // #nosec G706
	log.Printf("")
	log.Printf("  Saving reports...")

	saveJSONReport(report, jsonFile)
	saveHTMLReport(report, htmlFile)

	log.Printf("")
	log.Printf("  ✓ Reports saved to: %s", outputDir) // #nosec G706
	log.Printf("")
	log.Printf("%s", strings.Repeat("=", 80))
}

// setupBenchmarkData prepares the database with sample data for benchmarking.
// It creates necessary tables (users, orders) and inserts 1000 user records
// and 500 order records if they don't already exist.
//
// Parameters:
//   - db: The database instance to populate with test data
//
// Returns:
//   - error: Any error encountered during setup, or nil on success
func setupBenchmarkData(db *database.Database) error {
	log.Println("Setting up benchmark data...")

	log.Println("  Creating users table...")
	if _, err := db.ExecuteQuery("CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name TEXT, age INT, email TEXT)"); err != nil {
		return fmt.Errorf("failed to create users table: %v", err)
	}

	log.Println("  Creating orders table...")
	if _, err := db.ExecuteQuery("CREATE TABLE IF NOT EXISTS orders (id INT PRIMARY KEY, user_id INT, amount TEXT, order_date TEXT)"); err != nil {
		return fmt.Errorf("failed to create orders table: %v", err)
	}

	log.Println("  Verifying table creation...")

	tables := db.GetTables()
	log.Printf("  Tables in database: %v", tables) // #nosec G706

	result, err := db.ExecuteQuery("SELECT COUNT(id) FROM users")
	if err != nil {
		return fmt.Errorf("failed to verify users table (table may not be properly created): %v", err)
	}

	shouldInsertData := true
	if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
		var count int64
		_, _ = fmt.Sscanf(result.Rows[0][0], "%d", &count)
		log.Printf("  Found %d existing records in users table", count)
		if count >= 1000 {
			log.Printf("  Skipping data insertion (already populated)")
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
			_, _ = db.ExecuteQuery(insertQuery)

			if i <= 500 {
				orderQuery := fmt.Sprintf(
					"INSERT INTO orders (id, user_id, amount, order_date) VALUES (%d, %d, '%d.99', '2025-01-%02d')",
					i, i, 100+i%900, 1+i%28,
				)
				_, _ = db.ExecuteQuery(orderQuery)
			}
		}
		log.Println("Sample data inserted successfully")
	}

	return nil
}

// runBenchmark executes a single benchmark test with the specified parameters.
// It runs the query multiple times (iterations) with configurable concurrency,
// collects timing data, and calculates comprehensive statistics including
// percentiles and throughput metrics.
//
// The function uses goroutines for concurrent execution and a semaphore pattern
// to limit the number of concurrent queries. All timing data is collected safely
// using mutex synchronization.
//
// Parameters:
//   - db: The database instance to run queries against
//   - queryType: Descriptive name for this benchmark (e.g., "Simple SELECT")
//   - query: The SQL query to benchmark
//   - iterations: Total number of times to execute the query
//   - concurrent: Maximum number of concurrent query executions
//
// Returns:
//   - BenchmarkResult: Comprehensive statistics about the benchmark execution
func runBenchmark(db *database.Database, queryType, query string, iterations, concurrent int) BenchmarkResult {
	durations := make([]time.Duration, 0, iterations)
	var mu sync.Mutex
	var wg sync.WaitGroup

	successCount := 0
	errorCount := 0
	errorSamples := make([]string, 0, 5)
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
				if len(errorSamples) < 5 {
					errorSamples = append(errorSamples, err.Error())
				}
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
		ErrorSamples:      errorSamples,
		Timestamp:         time.Now(),
	}
}

// formatDuration formats a duration in a human-readable way with appropriate units.
// Examples: 1.23ms, 456.78µs, 12.34s
func formatDuration(d time.Duration) string {
	switch {
	case d >= time.Second:
		return fmt.Sprintf("%.2fs", d.Seconds())
	case d >= time.Millisecond:
		return fmt.Sprintf("%.2fms", float64(d.Microseconds())/1000.0)
	case d >= time.Microsecond:
		return fmt.Sprintf("%.2fµs", float64(d.Nanoseconds())/1000.0)
	default:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	}
}

// printBenchmarkResult outputs benchmark statistics to the console in a
// human-readable format. It displays timing metrics, percentiles, throughput,
// and success/error counts.
//
// Parameters:
//   - result: The benchmark result to print
func printBenchmarkResult(result BenchmarkResult) {
	successRate := float64(result.SuccessCount) / float64(result.Iterations) * 100

	log.Printf("  ┌─ Results")                                                                                            // #nosec G706
	log.Printf("  │  Total Time:        %s", formatDuration(result.TotalDuration))                                        // #nosec G706
	log.Printf("  │  Avg per Query:     %s", formatDuration(result.AvgDuration))                                          // #nosec G706
	log.Printf("  │  Min / Max:         %s / %s", formatDuration(result.MinDuration), formatDuration(result.MaxDuration)) // #nosec G706
	log.Printf("  │  Median (P50):      %s", formatDuration(result.MedianDuration))                                       // #nosec G706
	log.Printf("  │  P95:               %s", formatDuration(result.P95Duration))                                          // #nosec G706
	log.Printf("  │  P99:               %s", formatDuration(result.P99Duration))                                          // #nosec G706
	log.Printf("  │  Throughput:        %.0f queries/sec", result.QueriesPerSecond)                                       // #nosec G706
	log.Printf("  │  Success Rate:      %.1f%% (%d/%d)", successRate, result.SuccessCount, result.Iterations)             // #nosec G706

	if result.ErrorCount > 0 && len(result.ErrorSamples) > 0 {
		log.Printf("  │")
		log.Printf("  │  ⚠ Errors detected (%d failures):", result.ErrorCount) // #nosec G706
		for i, errMsg := range result.ErrorSamples {
			safe := strings.NewReplacer("\n", " ", "\r", " ").Replace(errMsg)
			if i == 0 {
				log.Printf("  │     Sample: %s", safe) // #nosec G706
			} else if i < 3 { // Show up to 3 unique errors
				log.Printf("  │            %s", safe) // #nosec G706
			}
		}
		if len(result.ErrorSamples) > 3 {
			log.Printf("  │     ... and %d more error(s)", len(result.ErrorSamples)-3) // #nosec G706
		}
	}

	log.Printf("  └─")
}

// saveJSONReport serializes the benchmark report to a JSON file.
// The JSON format allows for easy parsing and integration with other tools.
//
// Parameters:
//   - report: The complete benchmark report to save
//   - filename: Path where the JSON file should be written
func saveJSONReport(report BenchmarkReport, filename string) {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		log.Printf("Error marshaling report: %v", err)
		return
	}

	if err := os.WriteFile(filename, data, 0o600); err != nil { // #nosec G703
		log.Printf("Error writing JSON report: %v", err)
		return
	}

	log.Printf("JSON report saved: %s", filename) // #nosec G706
}

// saveHTMLReport generates a styled HTML report from the benchmark results.
// The report uses Tailwind CSS for styling and Cascadia Code font for a
// modern, professional appearance. It includes:
//   - Summary information (start/end time, duration, database name)
//   - Detailed results table with all metrics
//   - Color-coded success rates and performance indicators
//
// Parameters:
//   - report: The complete benchmark report to convert to HTML
//   - filename: Path where the HTML file should be written
func saveHTMLReport(report BenchmarkReport, filename string) {
	html := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head>
	<meta charset="UTF-8">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<title>StoreMy Benchmark Report - %s</title>
	<script src="https://cdn.tailwindcss.com"></script>
	<link rel="preconnect" href="https://fonts.googleapis.com">
	<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
	<link href="https://fonts.googleapis.com/css2?family=Cascadia+Code:wght@400;600;700&display=swap" rel="stylesheet">
	<style>
		body {
			font-family: 'Cascadia Code', monospace;
		}
	</style>
</head>
<body class="bg-gray-100 p-6">
	<div class="max-w-7xl mx-auto bg-white rounded-lg shadow-lg p-8">
		<h1 class="text-4xl font-bold text-gray-800 border-b-4 border-green-500 pb-3 mb-6">
			StoreMy Benchmark Report
		</h1>

		<div class="bg-green-50 rounded-lg p-6 mb-8 grid grid-cols-2 md:grid-cols-4 gap-4">
			<div class="space-y-1">
				<div class="text-sm font-semibold text-gray-600">Start Time</div>
				<div class="text-lg text-green-600 font-bold">%s</div>
			</div>
			<div class="space-y-1">
				<div class="text-sm font-semibold text-gray-600">End Time</div>
				<div class="text-lg text-green-600 font-bold">%s</div>
			</div>
			<div class="space-y-1">
				<div class="text-sm font-semibold text-gray-600">Total Duration</div>
				<div class="text-lg text-green-600 font-bold">%v</div>
			</div>
			<div class="space-y-1">
				<div class="text-sm font-semibold text-gray-600">Database</div>
				<div class="text-lg text-green-600 font-bold">%s</div>
			</div>
		</div>

		<h2 class="text-2xl font-bold text-gray-700 mt-8 mb-4">Benchmark Results</h2>
		<div class="overflow-x-auto">
			<table class="min-w-full border-collapse">
				<thead>
					<tr class="bg-green-500 text-white">
						<th class="px-4 py-3 text-left font-bold">Query Type</th>
						<th class="px-4 py-3 text-left font-bold">Query</th>
						<th class="px-4 py-3 text-left font-bold">Iterations</th>
						<th class="px-4 py-3 text-left font-bold">Concurrent</th>
						<th class="px-4 py-3 text-left font-bold">Avg Time</th>
						<th class="px-4 py-3 text-left font-bold">Min/Max</th>
						<th class="px-4 py-3 text-left font-bold">P95</th>
						<th class="px-4 py-3 text-left font-bold">QPS</th>
						<th class="px-4 py-3 text-left font-bold">Success Rate</th>
					</tr>
				</thead>
				<tbody class="divide-y divide-gray-200">
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
					<tr class="hover:bg-gray-50 transition-colors">
						<td class="px-4 py-3 font-bold text-gray-800">%s</td>
						<td class="px-4 py-3 text-sm text-gray-700 max-w-md truncate">%s</td>
						<td class="px-4 py-3 text-gray-700">%d</td>
						<td class="px-4 py-3 text-gray-700">%d</td>
						<td class="px-4 py-3 text-gray-700">%v</td>
						<td class="px-4 py-3 text-gray-700">%v / %v</td>
						<td class="px-4 py-3 text-gray-700">%v</td>
						<td class="px-4 py-3 text-green-600 font-semibold">%.2f</td>
						<td class="px-4 py-3 text-green-600 font-semibold">%.1f%%</td>
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
				</tbody>
			</table>
		</div>
	</div>
</body>
</html>
`

	if err := os.WriteFile(filename, []byte(html), 0o600); err != nil { // #nosec G703
		log.Printf("Error writing HTML report: %v", err)
		return
	}

	log.Printf("HTML report saved: %s", filename) // #nosec G706
}
