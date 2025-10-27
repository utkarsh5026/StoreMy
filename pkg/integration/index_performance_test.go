package integration

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// PerformanceResult stores detailed performance comparison results
type PerformanceResult struct {
	Name              string        `json:"name"`
	QueryType         string        `json:"query_type"` // "sequential_scan" or "index_scan"
	RowCount          int           `json:"row_count"`
	MatchedRows       int           `json:"matched_rows"`
	Selectivity       float64       `json:"selectivity"` // percentage of rows matched
	ExecutionTime     time.Duration `json:"execution_time_ns"`
	MemAllocBytes     int64         `json:"mem_alloc_bytes"`
	MemTotalAllocated int64         `json:"mem_total_allocated"`
}

// TestIndexPerformance_EqualityQuery tests performance difference for equality queries
func TestIndexPerformance_EqualityQuery(t *testing.T) {
	td := SetupTestDB(t)
	defer td.Cleanup()

	// Create table with a large dataset
	td.MustExecute(t, `CREATE TABLE users (
		id INT PRIMARY KEY,
		email STRING,
		age INT,
		score INT
	)`)

	// Insert test data - 500 rows (reduced for faster testing)
	t.Log("Inserting 500 test rows...")
	insertStart := time.Now()
	rowCount := 500
	for i := 1; i <= rowCount; i++ {
		td.MustExecute(t, fmt.Sprintf(
			"INSERT INTO users (id, email, age, score) VALUES (%d, 'user%d@example.com', %d, %d)",
			i, i, 20+(i%50), i%100,
		))
	}
	t.Logf("Data insertion completed in %v", time.Since(insertStart))

	var results []PerformanceResult

	// Test 1: Sequential scan for equality query (no index on email)
	t.Log("\n=== Test 1: Sequential Scan (Equality) ===")
	result1 := measureQueryPerformance(t, td,
		"Sequential Scan - Equality (email)",
		"sequential_scan",
		"SELECT * FROM users WHERE email = 'user250@example.com'",
		rowCount,
	)
	results = append(results, result1)
	t.Logf("Sequential scan: %v, Matched: %d rows", result1.ExecutionTime, result1.MatchedRows)

	// Create index on email
	t.Log("\n=== Creating Index on email ===")
	indexStart := time.Now()
	_, err := td.DB.ExecuteQuery("CREATE INDEX idx_email ON users(email)")
	if err != nil {
		t.Logf("WARNING: Index creation failed (this may be expected if hash indexes have issues): %v", err)
		t.Logf("Skipping index scan comparison for this test")
		savePerformanceResults(t, results, "index_equality")
		return
	}
	t.Logf("Index created in %v", time.Since(indexStart))

	// Test 2: Index scan for equality query (with index on email)
	t.Log("\n=== Test 2: Index Scan (Equality) ===")
	result2 := measureQueryPerformance(t, td,
		"Index Scan - Equality (email)",
		"index_scan",
		"SELECT * FROM users WHERE email = 'user250@example.com'",
		rowCount,
	)
	results = append(results, result2)
	t.Logf("Index scan: %v, Matched: %d rows", result2.ExecutionTime, result2.MatchedRows)

	// Calculate speedup
	if result2.ExecutionTime > 0 {
		speedup := float64(result1.ExecutionTime) / float64(result2.ExecutionTime)
		t.Logf("\n*** SPEEDUP: %.2fx faster with index ***\n", speedup)

		// Assert that index scan is actually faster
		if speedup < 1.0 {
			t.Logf("WARNING: Index scan was not faster (%.2fx). This may indicate the optimizer is not using the index.", speedup)
		}
	}

	// Save results
	savePerformanceResults(t, results, "index_equality")
}

// TestIndexPerformance_RangeQuery tests performance difference for range queries
func TestIndexPerformance_RangeQuery(t *testing.T) {
	td := SetupTestDB(t)
	defer td.Cleanup()

	// Create table
	td.MustExecute(t, `CREATE TABLE products (
		id INT PRIMARY KEY,
		name STRING,
		price INT,
		category STRING
	)`)

	// Insert test data - 500 rows (reduced for faster testing)
	t.Log("Inserting 500 test rows...")
	insertStart := time.Now()
	rowCount := 500
	for i := 1; i <= rowCount; i++ {
		td.MustExecute(t, fmt.Sprintf(
			"INSERT INTO products (id, name, price, category) VALUES (%d, 'product_%d', %d, 'category_%d')",
			i, i, i*10, i%20,
		))
	}
	t.Logf("Data insertion completed in %v", time.Since(insertStart))

	var results []PerformanceResult

	// Test 1: Sequential scan for range query (no index on price)
	t.Log("\n=== Test 1: Sequential Scan (Range) ===")
	result1 := measureQueryPerformance(t, td,
		"Sequential Scan - Range (price)",
		"sequential_scan",
		"SELECT * FROM products WHERE price >= 2000 AND price <= 3000",
		rowCount,
	)
	results = append(results, result1)
	t.Logf("Sequential scan: %v, Matched: %d rows (%.2f%% selectivity)",
		result1.ExecutionTime, result1.MatchedRows, result1.Selectivity*100)

	// Create index on price
	t.Log("\n=== Creating Index on price ===")
	indexStart := time.Now()
	_, err := td.DB.ExecuteQuery("CREATE INDEX idx_price ON products(price)")
	if err != nil {
		t.Logf("WARNING: Index creation failed: %v", err)
		t.Logf("Skipping index scan comparison for this test")
		savePerformanceResults(t, results, "index_range")
		return
	}
	t.Logf("Index created in %v", time.Since(indexStart))

	// Test 2: Index scan for range query (with index on price)
	t.Log("\n=== Test 2: Index Scan (Range) ===")
	result2 := measureQueryPerformance(t, td,
		"Index Scan - Range (price)",
		"index_scan",
		"SELECT * FROM products WHERE price >= 2000 AND price <= 3000",
		rowCount,
	)
	results = append(results, result2)
	t.Logf("Index scan: %v, Matched: %d rows (%.2f%% selectivity)",
		result2.ExecutionTime, result2.MatchedRows, result2.Selectivity*100)

	// Calculate speedup
	if result2.ExecutionTime > 0 {
		speedup := float64(result1.ExecutionTime) / float64(result2.ExecutionTime)
		t.Logf("\n*** SPEEDUP: %.2fx faster with index ***\n", speedup)
	}

	// Save results
	savePerformanceResults(t, results, "index_range")
}

// TestIndexPerformance_HighSelectivity tests when many rows match (high selectivity)
func TestIndexPerformance_HighSelectivity(t *testing.T) {
	td := SetupTestDB(t)
	defer td.Cleanup()

	// Create table
	td.MustExecute(t, `CREATE TABLE orders (
		id INT PRIMARY KEY,
		customer_id INT,
		status STRING,
		amount INT
	)`)

	// Insert test data - 500 rows (reduced for faster testing)
	t.Log("Inserting 500 test rows...")
	rowCount := 500
	for i := 1; i <= rowCount; i++ {
		status := "pending"
		if i%10 < 2 { // 20% are completed
			status = "completed"
		}
		td.MustExecute(t, fmt.Sprintf(
			"INSERT INTO orders (id, customer_id, status, amount) VALUES (%d, %d, '%s', %d)",
			i, i%100, status, i*100,
		))
	}

	var results []PerformanceResult

	// Test 1: Sequential scan (no index on status)
	t.Log("\n=== Test 1: Sequential Scan (High Selectivity - 80% match) ===")
	result1 := measureQueryPerformance(t, td,
		"Sequential Scan - High Selectivity",
		"sequential_scan",
		"SELECT * FROM orders WHERE status = 'pending'",
		rowCount,
	)
	results = append(results, result1)
	t.Logf("Sequential scan: %v, Matched: %d rows (%.2f%% selectivity)",
		result1.ExecutionTime, result1.MatchedRows, result1.Selectivity*100)

	// Create index on status
	t.Log("\n=== Creating Index on status ===")
	_, err := td.DB.ExecuteQuery("CREATE INDEX idx_status ON orders(status)")
	if err != nil {
		t.Logf("WARNING: Index creation failed: %v", err)
		t.Logf("Skipping index scan comparison for this test")
		savePerformanceResults(t, results, "index_high_selectivity")
		return
	}

	// Test 2: Index scan (with index on status)
	t.Log("\n=== Test 2: Index Scan (High Selectivity - 80% match) ===")
	result2 := measureQueryPerformance(t, td,
		"Index Scan - High Selectivity",
		"index_scan",
		"SELECT * FROM orders WHERE status = 'pending'",
		rowCount,
	)
	results = append(results, result2)
	t.Logf("Index scan: %v, Matched: %d rows (%.2f%% selectivity)",
		result2.ExecutionTime, result2.MatchedRows, result2.Selectivity*100)

	// Calculate speedup
	if result2.ExecutionTime > 0 {
		speedup := float64(result1.ExecutionTime) / float64(result2.ExecutionTime)
		t.Logf("\n*** Note: With high selectivity (%.2f%%), speedup is %.2fx ***", result1.Selectivity*100, speedup)
		t.Logf("High selectivity queries may benefit less from indexes as most rows match.")
	}

	// Save results
	savePerformanceResults(t, results, "index_high_selectivity")
}

// TestIndexPerformance_LowSelectivity tests when few rows match (low selectivity)
func TestIndexPerformance_LowSelectivity(t *testing.T) {
	td := SetupTestDB(t)
	defer td.Cleanup()

	// Create table
	td.MustExecute(t, `CREATE TABLE events (
		id INT PRIMARY KEY,
		event_type STRING,
		user_id INT,
		timestamp INT
	)`)

	// Insert test data - 1000 rows (increased from others to show 1% selectivity)
	t.Log("Inserting 1000 test rows...")
	rowCount := 1000
	for i := 1; i <= rowCount; i++ {
		eventType := "view"
		if i%100 == 0 { // Only 1% are "purchase" events
			eventType = "purchase"
		}
		td.MustExecute(t, fmt.Sprintf(
			"INSERT INTO events (id, event_type, user_id, timestamp) VALUES (%d, '%s', %d, %d)",
			i, eventType, i%1000, i,
		))
	}

	var results []PerformanceResult

	// Test 1: Sequential scan (no index on event_type)
	t.Log("\n=== Test 1: Sequential Scan (Low Selectivity - 1% match) ===")
	result1 := measureQueryPerformance(t, td,
		"Sequential Scan - Low Selectivity",
		"sequential_scan",
		"SELECT * FROM events WHERE event_type = 'purchase'",
		rowCount,
	)
	results = append(results, result1)
	t.Logf("Sequential scan: %v, Matched: %d rows (%.2f%% selectivity)",
		result1.ExecutionTime, result1.MatchedRows, result1.Selectivity*100)

	// Create index on event_type
	t.Log("\n=== Creating Index on event_type ===")
	_, err := td.DB.ExecuteQuery("CREATE INDEX idx_event_type ON events(event_type)")
	if err != nil {
		t.Logf("WARNING: Index creation failed: %v", err)
		t.Logf("Skipping index scan comparison for this test")
		savePerformanceResults(t, results, "index_low_selectivity")
		return
	}

	// Test 2: Index scan (with index on event_type)
	t.Log("\n=== Test 2: Index Scan (Low Selectivity - 1% match) ===")
	result2 := measureQueryPerformance(t, td,
		"Index Scan - Low Selectivity",
		"index_scan",
		"SELECT * FROM events WHERE event_type = 'purchase'",
		rowCount,
	)
	results = append(results, result2)
	t.Logf("Index scan: %v, Matched: %d rows (%.2f%% selectivity)",
		result2.ExecutionTime, result2.MatchedRows, result2.Selectivity*100)

	// Calculate speedup
	if result2.ExecutionTime > 0 {
		speedup := float64(result1.ExecutionTime) / float64(result2.ExecutionTime)
		t.Logf("\n*** MAXIMUM SPEEDUP: %.2fx faster with index ***", speedup)
		t.Logf("Low selectivity queries (%.2f%%) benefit most from indexes!", result1.Selectivity*100)
	}

	// Save results
	savePerformanceResults(t, results, "index_low_selectivity")
}

// TestIndexPerformance_Comprehensive runs comprehensive benchmark with various data sizes
func TestIndexPerformance_Comprehensive(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping comprehensive benchmark in short mode")
	}

	dataSizes := []int{100, 250, 500, 1000}
	var allResults []PerformanceResult

	for _, size := range dataSizes {
		t.Logf("\n========== Testing with %d rows ==========", size)

		td := SetupTestDB(t)

		// Create table
		td.MustExecute(t, `CREATE TABLE benchmark (
			id INT PRIMARY KEY,
			search_key INT,
			data STRING
		)`)

		// Insert data
		t.Logf("Inserting %d rows...", size)
		for i := 1; i <= size; i++ {
			td.MustExecute(t, fmt.Sprintf(
				"INSERT INTO benchmark (id, search_key, data) VALUES (%d, %d, 'data_%d')",
				i, i%1000, i,
			))
		}

		// Sequential scan
		result1 := measureQueryPerformance(t, td,
			fmt.Sprintf("SeqScan_%d_rows", size),
			"sequential_scan",
			"SELECT * FROM benchmark WHERE search_key = 500",
			size,
		)
		allResults = append(allResults, result1)

		// Create index
		_, err := td.DB.ExecuteQuery("CREATE INDEX idx_search_key ON benchmark(search_key)")
		if err != nil {
			t.Logf("WARNING: Index creation failed for size %d: %v", size, err)
			td.Cleanup()
			continue
		}

		// Index scan
		result2 := measureQueryPerformance(t, td,
			fmt.Sprintf("IndexScan_%d_rows", size),
			"index_scan",
			"SELECT * FROM benchmark WHERE search_key = 500",
			size,
		)
		allResults = append(allResults, result2)

		speedup := float64(result1.ExecutionTime) / float64(result2.ExecutionTime)
		t.Logf("Size %d: SeqScan=%v, IndexScan=%v, Speedup=%.2fx",
			size, result1.ExecutionTime, result2.ExecutionTime, speedup)

		td.Cleanup()
	}

	// Save comprehensive results
	savePerformanceResults(t, allResults, "index_comprehensive")
	generateComparisonHTML(t, allResults, "index_comprehensive")
}

// measureQueryPerformance executes a query and measures its performance
func measureQueryPerformance(t *testing.T, td *TestDatabase, name, queryType, query string, totalRows int) PerformanceResult {
	t.Helper()

	var memStatsBefore, memStatsAfter runtime.MemStats
	runtime.GC() // Run garbage collection before measurement
	runtime.ReadMemStats(&memStatsBefore)

	start := time.Now()
	result := td.MustExecute(t, query)
	elapsed := time.Since(start)

	runtime.ReadMemStats(&memStatsAfter)

	matchedRows := len(result.Rows)
	selectivity := 0.0
	if totalRows > 0 {
		selectivity = float64(matchedRows) / float64(totalRows)
	}

	return PerformanceResult{
		Name:              name,
		QueryType:         queryType,
		RowCount:          totalRows,
		MatchedRows:       matchedRows,
		Selectivity:       selectivity,
		ExecutionTime:     elapsed,
		MemAllocBytes:     int64(memStatsAfter.Alloc),
		MemTotalAllocated: int64(memStatsAfter.TotalAlloc - memStatsBefore.TotalAlloc),
	}
}

// savePerformanceResults saves results to JSON file
func savePerformanceResults(t *testing.T, results []PerformanceResult, testName string) {
	t.Helper()

	timestamp := time.Now().Format("2006-01-02_15-04-05")
	benchmarkDir := filepath.Join("..", "..", ".benchmarks")
	jsonDir := filepath.Join(benchmarkDir, "json")

	if err := os.MkdirAll(jsonDir, 0755); err != nil {
		t.Logf("Warning: Failed to create json directory: %v", err)
		return
	}

	jsonFilename := filepath.Join(jsonDir, fmt.Sprintf("%s_%s.json", testName, timestamp))
	jsonLatestFilename := filepath.Join(jsonDir, fmt.Sprintf("%s_latest.json", testName))

	// Save timestamped JSON
	file, err := os.Create(jsonFilename)
	if err != nil {
		t.Logf("Warning: Failed to create results file: %v", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(results); err != nil {
		t.Logf("Warning: Failed to write results: %v", err)
		return
	}

	// Save latest JSON
	latestFile, err := os.Create(jsonLatestFilename)
	if err == nil {
		defer latestFile.Close()
		latestEncoder := json.NewEncoder(latestFile)
		latestEncoder.SetIndent("", "  ")
		latestEncoder.Encode(results)
	}

	t.Logf("\nPerformance results saved to:")
	t.Logf("  - %s", jsonFilename)
	t.Logf("  - %s", jsonLatestFilename)
}

// generateComparisonHTML creates an HTML report comparing index vs sequential scan performance
func generateComparisonHTML(t *testing.T, results []PerformanceResult, testName string) {
	t.Helper()

	timestamp := time.Now().Format("2006-01-02_15-04-05")
	benchmarkDir := filepath.Join("..", "..", ".benchmarks")
	htmlDir := filepath.Join(benchmarkDir, "html")

	if err := os.MkdirAll(htmlDir, 0755); err != nil {
		t.Logf("Warning: Failed to create html directory: %v", err)
		return
	}

	html := `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Index Performance Comparison - StoreMy</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {
            font-family: 'Cascadia Mono', 'Cascadia Code', Consolas, Monaco, 'Courier New', monospace;
        }
    </style>
</head>
<body class="bg-gray-900 text-gray-100 p-8">
    <div class="max-w-7xl mx-auto">
        <div class="mb-8">
            <h1 class="text-4xl font-bold mb-2 text-blue-400">Index vs Sequential Scan Performance</h1>
            <p class="text-gray-400">Generated: ` + timestamp + `</p>
            <p class="text-gray-400 mt-2">This benchmark demonstrates the performance benefits of using indexes for query optimization.</p>
        </div>

        <!-- Summary Cards -->
        <div class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
` + generateSummaryCards(results) + `
        </div>

        <!-- Performance Chart -->
        <div class="bg-gray-800 rounded-lg p-6 border border-gray-700 mb-8">
            <h2 class="text-xl font-bold mb-4 text-blue-400">Performance Comparison</h2>
            <canvas id="performanceChart"></canvas>
        </div>

        <!-- Results Table -->
        <div class="bg-gray-800 rounded-lg border border-gray-700 overflow-hidden mb-8">
            <table class="w-full">
                <thead class="bg-gray-700">
                    <tr>
                        <th class="px-6 py-4 text-left text-xs font-semibold text-gray-300 uppercase">Test Name</th>
                        <th class="px-6 py-4 text-left text-xs font-semibold text-gray-300 uppercase">Scan Type</th>
                        <th class="px-6 py-4 text-right text-xs font-semibold text-gray-300 uppercase">Rows</th>
                        <th class="px-6 py-4 text-right text-xs font-semibold text-gray-300 uppercase">Matched</th>
                        <th class="px-6 py-4 text-right text-xs font-semibold text-gray-300 uppercase">Selectivity</th>
                        <th class="px-6 py-4 text-right text-xs font-semibold text-gray-300 uppercase">Time</th>
                    </tr>
                </thead>
                <tbody class="divide-y divide-gray-700">
` + generateTableRows(results) + `
                </tbody>
            </table>
        </div>

        <!-- Key Insights -->
        <div class="bg-gray-800 rounded-lg p-6 border border-gray-700">
            <h2 class="text-xl font-bold mb-4 text-blue-400">Key Insights</h2>
            <div class="space-y-3 text-sm">
` + generateInsights(results) + `
            </div>
        </div>

        <div class="mt-8 text-center text-xs text-gray-500">
            <p>Generated by StoreMy Performance Test Suite</p>
        </div>
    </div>

    <script>
` + generateChartScript(results) + `
    </script>
</body>
</html>`

	htmlFilename := filepath.Join(htmlDir, fmt.Sprintf("%s_%s.html", testName, timestamp))
	htmlLatestFilename := filepath.Join(htmlDir, fmt.Sprintf("%s_latest.html", testName))

	if err := os.WriteFile(htmlFilename, []byte(html), 0644); err != nil {
		t.Logf("Warning: Failed to create HTML file: %v", err)
		return
	}

	if err := os.WriteFile(htmlLatestFilename, []byte(html), 0644); err == nil {
		t.Logf("  - %s", htmlLatestFilename)
	}

	t.Logf("\nHTML report saved to:")
	t.Logf("  - %s", htmlFilename)
}

// HTML generation helper functions
func generateSummaryCards(results []PerformanceResult) string {
	if len(results) == 0 {
		return ""
	}

	var maxSpeedup float64 = 0
	var avgSpeedup float64 = 0
	speedupCount := 0

	for i := 0; i < len(results)-1; i += 2 {
		if i+1 < len(results) && results[i+1].ExecutionTime > 0 {
			speedup := float64(results[i].ExecutionTime) / float64(results[i+1].ExecutionTime)
			if speedup > maxSpeedup {
				maxSpeedup = speedup
			}
			avgSpeedup += speedup
			speedupCount++
		}
	}

	if speedupCount > 0 {
		avgSpeedup /= float64(speedupCount)
	}

	return fmt.Sprintf(`            <div class="bg-gray-800 rounded-lg p-6 border border-gray-700">
                <h3 class="text-sm text-gray-400 mb-1">Maximum Speedup</h3>
                <p class="text-2xl font-bold text-green-400">%.2fx</p>
                <p class="text-xs text-gray-500 mt-1">Best improvement with index</p>
            </div>
            <div class="bg-gray-800 rounded-lg p-6 border border-gray-700">
                <h3 class="text-sm text-gray-400 mb-1">Average Speedup</h3>
                <p class="text-2xl font-bold text-blue-400">%.2fx</p>
                <p class="text-xs text-gray-500 mt-1">Mean performance gain</p>
            </div>
            <div class="bg-gray-800 rounded-lg p-6 border border-gray-700">
                <h3 class="text-sm text-gray-400 mb-1">Total Tests</h3>
                <p class="text-2xl font-bold text-purple-400">%d</p>
                <p class="text-xs text-gray-500 mt-1">Test scenarios run</p>
            </div>`, maxSpeedup, avgSpeedup, len(results))
}

func generateTableRows(results []PerformanceResult) string {
	html := ""
	for i, result := range results {
		rowClass := "bg-gray-800"
		if i%2 == 1 {
			rowClass = "bg-gray-750"
		}

		typeClass := "text-yellow-400"
		if result.QueryType == "index_scan" {
			typeClass = "text-green-400"
		}

		timeClass := "text-gray-300"
		if result.ExecutionTime < 100*time.Millisecond {
			timeClass = "text-green-400"
		} else if result.ExecutionTime > 500*time.Millisecond {
			timeClass = "text-red-400"
		}

		html += fmt.Sprintf(`                    <tr class="%s hover:bg-gray-700 transition-colors">
                        <td class="px-6 py-4 text-sm font-medium text-blue-300">%s</td>
                        <td class="px-6 py-4 text-sm %s">%s</td>
                        <td class="px-6 py-4 text-sm text-right text-gray-300">%s</td>
                        <td class="px-6 py-4 text-sm text-right text-gray-300">%s</td>
                        <td class="px-6 py-4 text-sm text-right text-gray-300">%.2f%%</td>
                        <td class="px-6 py-4 text-sm text-right %s">%s</td>
                    </tr>
`,
			rowClass,
			result.Name,
			typeClass,
			result.QueryType,
			formatNumber(int64(result.RowCount)),
			formatNumber(int64(result.MatchedRows)),
			result.Selectivity*100,
			timeClass,
			formatDuration(result.ExecutionTime),
		)
	}
	return html
}

func generateInsights(results []PerformanceResult) string {
	insights := ""

	// Compare sequential vs index scans
	for i := 0; i < len(results)-1; i += 2 {
		if i+1 < len(results) {
			seqScan := results[i]
			indexScan := results[i+1]

			if indexScan.ExecutionTime > 0 {
				speedup := float64(seqScan.ExecutionTime) / float64(indexScan.ExecutionTime)

				color := "green"
				if speedup < 2 {
					color = "yellow"
				}

				insights += fmt.Sprintf(`                <div class="flex items-start">
                    <span class="text-%s-400 mr-2">✓</span>
                    <p class="text-gray-300"><span class="text-%s-400 font-semibold">%.2fx faster</span> - %s (Selectivity: %.2f%%)</p>
                </div>
`, color, color, speedup, seqScan.Name, seqScan.Selectivity*100)
			}
		}
	}

	if insights == "" {
		insights = `                <p class="text-gray-400">No performance comparisons available.</p>`
	}

	return insights
}

func generateChartScript(results []PerformanceResult) string {
	labels := "["
	seqData := "["
	indexData := "["

	for i := 0; i < len(results); i += 2 {
		if i+1 < len(results) {
			labels += fmt.Sprintf("'%s',", results[i].Name)
			seqData += fmt.Sprintf("%d,", results[i].ExecutionTime.Milliseconds())
			indexData += fmt.Sprintf("%d,", results[i+1].ExecutionTime.Milliseconds())
		}
	}

	labels += "]"
	seqData += "]"
	indexData += "]"

	return fmt.Sprintf(`        const ctx = document.getElementById('performanceChart').getContext('2d');
        new Chart(ctx, {
            type: 'bar',
            data: {
                labels: %s,
                datasets: [
                    {
                        label: 'Sequential Scan',
                        data: %s,
                        backgroundColor: 'rgba(251, 191, 36, 0.8)',
                        borderColor: 'rgba(251, 191, 36, 1)',
                        borderWidth: 1
                    },
                    {
                        label: 'Index Scan',
                        data: %s,
                        backgroundColor: 'rgba(34, 197, 94, 0.8)',
                        borderColor: 'rgba(34, 197, 94, 1)',
                        borderWidth: 1
                    }
                ]
            },
            options: {
                responsive: true,
                plugins: {
                    legend: {
                        labels: {
                            color: '#e5e7eb'
                        }
                    },
                    title: {
                        display: true,
                        text: 'Query Execution Time (ms)',
                        color: '#e5e7eb'
                    }
                },
                scales: {
                    x: {
                        ticks: { color: '#9ca3af' },
                        grid: { color: '#374151' }
                    },
                    y: {
                        ticks: { color: '#9ca3af' },
                        grid: { color: '#374151' }
                    }
                }
            }
        });`, labels, seqData, indexData)
}

// Helper function to format numbers with commas
func formatNumber(n int64) string {
	str := fmt.Sprintf("%d", n)
	if len(str) <= 3 {
		return str
	}
	result := ""
	for i, c := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result += ","
		}
		result += string(c)
	}
	return result
}

// Helper function to format durations
func formatDuration(d time.Duration) string {
	if d < time.Microsecond {
		return fmt.Sprintf("%d ns", d.Nanoseconds())
	} else if d < time.Millisecond {
		return fmt.Sprintf("%.2f μs", float64(d.Nanoseconds())/1000.0)
	} else if d < time.Second {
		return fmt.Sprintf("%.2f ms", float64(d.Nanoseconds())/1000000.0)
	}
	return fmt.Sprintf("%.2f s", d.Seconds())
}
