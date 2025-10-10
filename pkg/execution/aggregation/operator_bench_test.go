package aggregation

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
	"time"
)

// Helper function to create test data with specified size
func createBenchmarkTuples(count int) ([]*tuple.Tuple, *tuple.TupleDescription) {
	td, _ := tuple.NewTupleDesc(
		[]types.Type{types.StringType, types.IntType},
		[]string{"group", "value"},
	)

	var tuples []*tuple.Tuple
	groups := []string{"A", "B", "C", "D", "E"}

	for i := 0; i < count; i++ {
		tup := tuple.NewTuple(td)
		group := groups[i%len(groups)]
		tup.SetField(0, types.NewStringField(group, len(group)))
		tup.SetField(1, types.NewIntField(int64(i%100)))
		tuples = append(tuples, tup)
	}

	return tuples, td
}

// BenchmarkAggregateOperator_Open benchmarks the Open operation
func BenchmarkAggregateOperator_Open(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(string(rune(size)), func(b *testing.B) {
			tuples, td := createBenchmarkTuples(size)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				source := newMockIterator(tuples, td)
				agg, _ := NewAggregateOperator(source, 1, 0, Sum)
				b.StartTimer()

				agg.Open()

				b.StopTimer()
				agg.Close()
				b.StartTimer()
			}
		})
	}
}

// BenchmarkAggregateOperator_Iteration benchmarks result iteration
func BenchmarkAggregateOperator_Iteration(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(string(rune(size)), func(b *testing.B) {
			tuples, td := createBenchmarkTuples(size)
			source := newMockIterator(tuples, td)
			agg, _ := NewAggregateOperator(source, 1, 0, Sum)
			agg.Open()
			defer agg.Close()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				agg.Rewind()
				for {
					hasNext, _ := agg.HasNext()
					if !hasNext {
						break
					}
					agg.Next()
				}
			}
		})
	}
}

// BenchmarkAggregateOperator_WithGroupBy benchmarks grouped aggregation
func BenchmarkAggregateOperator_WithGroupBy(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(string(rune(size)), func(b *testing.B) {
			tuples, td := createBenchmarkTuples(size)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				source := newMockIterator(tuples, td)
				agg, _ := NewAggregateOperator(source, 1, 0, Sum)
				b.StartTimer()

				agg.Open()

				b.StopTimer()
				agg.Close()
				b.StartTimer()
			}
		})
	}
}

// BenchmarkAggregateOperator_NoGroupBy benchmarks aggregation without grouping
func BenchmarkAggregateOperator_NoGroupBy(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(string(rune(size)), func(b *testing.B) {
			tuples, td := createBenchmarkTuples(size)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				source := newMockIterator(tuples, td)
				agg, _ := NewAggregateOperator(source, 1, NoGrouping, Sum)
				b.StartTimer()

				agg.Open()

				b.StopTimer()
				agg.Close()
				b.StartTimer()
			}
		})
	}
}

// BenchmarkAggregateOperator_DifferentOps benchmarks different aggregate operations
func BenchmarkAggregateOperator_DifferentOps(b *testing.B) {
	tuples, td := createBenchmarkTuples(1000)
	ops := []AggregateOp{Sum, Avg, Min, Max, Count}

	for _, op := range ops {
		b.Run(op.String(), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				source := newMockIterator(tuples, td)
				agg, _ := NewAggregateOperator(source, 1, NoGrouping, op)
				b.StartTimer()

				agg.Open()

				b.StopTimer()
				agg.Close()
				b.StartTimer()
			}
		})
	}
}

// BenchmarkAggregateOperator_FullLifecycle benchmarks complete operation lifecycle
func BenchmarkAggregateOperator_FullLifecycle(b *testing.B) {
	tuples, td := createBenchmarkTuples(1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source := newMockIterator(tuples, td)
		agg, _ := NewAggregateOperator(source, 1, 0, Sum)

		agg.Open()

		for {
			hasNext, _ := agg.HasNext()
			if !hasNext {
				break
			}
			agg.Next()
		}

		agg.Close()
	}
}

// BenchmarkResult stores detailed benchmark results
type BenchmarkResult struct {
	Name              string        `json:"name"`
	Operations        int           `json:"operations"`
	TotalTime         time.Duration `json:"total_time_ns"`
	NsPerOp           int64         `json:"ns_per_op"`
	BytesPerOp        int64         `json:"bytes_per_op"`
	AllocsPerOp       int64         `json:"allocs_per_op"`
	MemAllocBytes     int64         `json:"mem_alloc_bytes"`
	MemTotalAllocated int64         `json:"mem_total_allocated"`
}

// TestBenchmarkAndSaveResults runs benchmarks and saves detailed results to a file
func TestBenchmarkAndSaveResults(t *testing.T) {
	results := []BenchmarkResult{}

	// Benchmark configurations
	benchmarks := []struct {
		name     string
		size     int
		groupBy  int
		op       AggregateOp
		runCount int
	}{
		{"Open_100", 100, 0, Sum, 1000},
		{"Open_1000", 1000, 0, Sum, 500},
		{"Open_10000", 10000, 0, Sum, 100},
		{"WithGroupBy_1000", 1000, 0, Sum, 500},
		{"NoGroupBy_1000", 1000, NoGrouping, Sum, 500},
		{"Sum_1000", 1000, NoGrouping, Sum, 500},
		{"Avg_1000", 1000, NoGrouping, Avg, 500},
		{"Min_1000", 1000, NoGrouping, Min, 500},
		{"Max_1000", 1000, NoGrouping, Max, 500},
		{"Count_1000", 1000, NoGrouping, Count, 500},
	}

	for _, bench := range benchmarks {
		tuples, td := createBenchmarkTuples(bench.size)

		start := time.Now()
		var memStatsBefore, memStatsAfter runtime.MemStats

		runtime.ReadMemStats(&memStatsBefore)

		for i := 0; i < bench.runCount; i++ {
			source := newMockIterator(tuples, td)
			agg, _ := NewAggregateOperator(source, 1, bench.groupBy, bench.op)
			agg.Open()
			agg.Close()
		}

		runtime.ReadMemStats(&memStatsAfter)
		elapsed := time.Since(start)

		result := BenchmarkResult{
			Name:              bench.name,
			Operations:        bench.runCount,
			TotalTime:         elapsed,
			NsPerOp:           elapsed.Nanoseconds() / int64(bench.runCount),
			BytesPerOp:        int64(memStatsAfter.TotalAlloc-memStatsBefore.TotalAlloc) / int64(bench.runCount),
			AllocsPerOp:       int64(memStatsAfter.Mallocs-memStatsBefore.Mallocs) / int64(bench.runCount),
			MemAllocBytes:     int64(memStatsAfter.Alloc),
			MemTotalAllocated: int64(memStatsAfter.TotalAlloc),
		}
		results = append(results, result)

		fmt.Printf("%s: %d ops, %v total, %d ns/op, %d B/op, %d allocs/op\n",
			result.Name, result.Operations, result.TotalTime,
			result.NsPerOp, result.BytesPerOp, result.AllocsPerOp)
	}

	// Save results to JSON and HTML files with timestamp
	timestamp := time.Now().Format("2006-01-02_15-04-05")
	benchmarkDir := filepath.Join("..", "..", "..", ".benchmarks")
	jsonDir := filepath.Join(benchmarkDir, "json")
	htmlDir := filepath.Join(benchmarkDir, "html")

	// Create directories if they don't exist
	if err := os.MkdirAll(jsonDir, 0755); err != nil {
		t.Fatalf("Failed to create json directory: %v", err)
	}
	if err := os.MkdirAll(htmlDir, 0755); err != nil {
		t.Fatalf("Failed to create html directory: %v", err)
	}

	// Save JSON files
	jsonFilename := filepath.Join(jsonDir, fmt.Sprintf("aggregation_operator_%s.json", timestamp))
	jsonLatestFilename := filepath.Join(jsonDir, "aggregation_operator_latest.json")

	// Save timestamped JSON
	file, err := os.Create(jsonFilename)
	if err != nil {
		t.Fatalf("Failed to create results file: %v", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(results); err != nil {
		t.Fatalf("Failed to write results: %v", err)
	}

	// Save latest JSON
	latestFile, err := os.Create(jsonLatestFilename)
	if err != nil {
		t.Logf("Warning: Failed to create latest file: %v", err)
	} else {
		defer latestFile.Close()
		latestEncoder := json.NewEncoder(latestFile)
		latestEncoder.SetIndent("", "  ")
		latestEncoder.Encode(results)
	}

	// Save HTML files
	htmlFilename := filepath.Join(htmlDir, fmt.Sprintf("aggregation_operator_%s.html", timestamp))
	htmlLatestFilename := filepath.Join(htmlDir, "aggregation_operator_latest.html")

	htmlContent := generateBenchmarkHTML(results, timestamp)

	// Save timestamped HTML
	if err := os.WriteFile(htmlFilename, []byte(htmlContent), 0644); err != nil {
		t.Logf("Warning: Failed to create HTML file: %v", err)
	}

	// Save latest HTML
	if err := os.WriteFile(htmlLatestFilename, []byte(htmlContent), 0644); err != nil {
		t.Logf("Warning: Failed to create latest HTML file: %v", err)
	}

	fmt.Printf("\nBenchmark results saved to:\n")
	fmt.Printf("  JSON:\n")
	fmt.Printf("    - %s\n", jsonFilename)
	fmt.Printf("    - %s\n", jsonLatestFilename)
	fmt.Printf("  HTML:\n")
	fmt.Printf("    - %s\n", htmlFilename)
	fmt.Printf("    - %s\n", htmlLatestFilename)
}

// generateBenchmarkHTML creates an HTML report with Tailwind CSS styling
func generateBenchmarkHTML(results []BenchmarkResult, timestamp string) string {
	html := `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Aggregation Operator Benchmark Results</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        body {
            font-family: 'Cascadia Mono', 'Cascadia Code', Consolas, Monaco, 'Courier New', monospace;
        }
    </style>
</head>
<body class="bg-gray-900 text-gray-100 p-8">
    <div class="max-w-7xl mx-auto">
        <div class="mb-8">
            <h1 class="text-4xl font-bold mb-2 text-blue-400">Aggregation Operator Benchmarks</h1>
            <p class="text-gray-400">Generated: ` + timestamp + `</p>
        </div>

        <!-- Summary Cards -->
        <div class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
            <div class="bg-gray-800 rounded-lg p-6 border border-gray-700">
                <h3 class="text-sm text-gray-400 mb-1">Fastest Operation</h3>
                <p class="text-2xl font-bold text-green-400">` + getFastestOp(results) + `</p>
            </div>
            <div class="bg-gray-800 rounded-lg p-6 border border-gray-700">
                <h3 class="text-sm text-gray-400 mb-1">Total Tests</h3>
                <p class="text-2xl font-bold text-blue-400">` + fmt.Sprintf("%d", len(results)) + `</p>
            </div>
            <div class="bg-gray-800 rounded-lg p-6 border border-gray-700">
                <h3 class="text-sm text-gray-400 mb-1">Avg Memory/Op</h3>
                <p class="text-2xl font-bold text-purple-400">` + fmt.Sprintf("%d B", getAvgMemory(results)) + `</p>
            </div>
        </div>

        <!-- Results Table -->
        <div class="bg-gray-800 rounded-lg border border-gray-700 overflow-hidden">
            <table class="w-full">
                <thead class="bg-gray-700">
                    <tr>
                        <th class="px-6 py-4 text-left text-xs font-semibold text-gray-300 uppercase tracking-wider">Benchmark</th>
                        <th class="px-6 py-4 text-right text-xs font-semibold text-gray-300 uppercase tracking-wider">Operations</th>
                        <th class="px-6 py-4 text-right text-xs font-semibold text-gray-300 uppercase tracking-wider">Time/Op</th>
                        <th class="px-6 py-4 text-right text-xs font-semibold text-gray-300 uppercase tracking-wider">Bytes/Op</th>
                        <th class="px-6 py-4 text-right text-xs font-semibold text-gray-300 uppercase tracking-wider">Allocs/Op</th>
                        <th class="px-6 py-4 text-right text-xs font-semibold text-gray-300 uppercase tracking-wider">Total Time</th>
                    </tr>
                </thead>
                <tbody class="divide-y divide-gray-700">
`

	for i, result := range results {
		rowClass := ""
		if i%2 == 0 {
			rowClass = "bg-gray-800"
		} else {
			rowClass = "bg-gray-750"
		}

		timeClass := "text-gray-300"
		if result.NsPerOp < 60000 {
			timeClass = "text-green-400 font-semibold"
		} else if result.NsPerOp > 80000 {
			timeClass = "text-red-400"
		}

		html += fmt.Sprintf(`                    <tr class="%s hover:bg-gray-700 transition-colors">
                        <td class="px-6 py-4 text-sm font-medium text-blue-300">%s</td>
                        <td class="px-6 py-4 text-sm text-right text-gray-300">%s</td>
                        <td class="px-6 py-4 text-sm text-right %s">%s ns</td>
                        <td class="px-6 py-4 text-sm text-right text-gray-300">%s B</td>
                        <td class="px-6 py-4 text-sm text-right text-gray-300">%d</td>
                        <td class="px-6 py-4 text-sm text-right text-gray-400">%s</td>
                    </tr>
`,
			rowClass,
			result.Name,
			formatNumber(int64(result.Operations)),
			timeClass,
			formatNumber(result.NsPerOp),
			formatNumber(result.BytesPerOp),
			result.AllocsPerOp,
			formatDuration(result.TotalTime),
		)
	}

	html += `                </tbody>
            </table>
        </div>

        <!-- Performance Insights -->
        <div class="mt-8 bg-gray-800 rounded-lg p-6 border border-gray-700">
            <h2 class="text-xl font-bold mb-4 text-blue-400">Performance Insights</h2>
            <div class="space-y-2 text-sm text-gray-300">
` + generateInsights(results) + `
            </div>
        </div>

        <div class="mt-8 text-center text-xs text-gray-500">
            <p>Generated by StoreMy Benchmark Suite</p>
        </div>
    </div>
</body>
</html>`

	return html
}

// Helper functions for HTML generation
func getFastestOp(results []BenchmarkResult) string {
	if len(results) == 0 {
		return "N/A"
	}
	fastest := results[0]
	for _, r := range results {
		if r.NsPerOp < fastest.NsPerOp {
			fastest = r
		}
	}
	return fmt.Sprintf("%s (%d ns)", fastest.Name, fastest.NsPerOp)
}

func getAvgMemory(results []BenchmarkResult) int64 {
	if len(results) == 0 {
		return 0
	}
	total := int64(0)
	for _, r := range results {
		total += r.BytesPerOp
	}
	return total / int64(len(results))
}

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

func generateInsights(results []BenchmarkResult) string {
	insights := ""

	// Find fastest and slowest
	if len(results) > 0 {
		fastest := results[0]
		slowest := results[0]
		for _, r := range results {
			if r.NsPerOp < fastest.NsPerOp {
				fastest = r
			}
			if r.NsPerOp > slowest.NsPerOp {
				slowest = r
			}
		}

		insights += fmt.Sprintf("                <p>✓ <span class=\"text-green-400\">%s</span> is the fastest operation at %s ns/op</p>\n",
			fastest.Name, formatNumber(fastest.NsPerOp))
		insights += fmt.Sprintf("                <p>✗ <span class=\"text-red-400\">%s</span> is the slowest operation at %s ns/op</p>\n",
			slowest.Name, formatNumber(slowest.NsPerOp))

		if slowest.NsPerOp > 0 {
			speedup := float64(slowest.NsPerOp) / float64(fastest.NsPerOp)
			insights += fmt.Sprintf("                <p>→ Performance difference: %.2fx faster</p>\n", speedup)
		}
	}

	// Check scaling
	for i := 0; i < len(results)-1; i++ {
		if results[i].Name[:4] == "Open" && results[i+1].Name[:4] == "Open" {
			insights += fmt.Sprintf("                <p>→ Scaling from %s to %s shows %s performance characteristics</p>\n",
				results[i].Name, results[i+1].Name, getScalingType(results[i], results[i+1]))
		}
	}

	return insights
}

func getScalingType(r1, r2 BenchmarkResult) string {
	ratio := float64(r2.NsPerOp) / float64(r1.NsPerOp)
	if ratio < 8 {
		return "sub-linear"
	} else if ratio < 12 {
		return "linear"
	} else {
		return "super-linear"
	}
}
