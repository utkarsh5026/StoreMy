package query

import (
	"fmt"
	"path/filepath"
	"runtime"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log/wal"
	"storemy/pkg/memory"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// benchmarkSetup holds resources for benchmark tests
type benchmarkSetup struct {
	heapFile *heap.HeapFile
	wal      *wal.WAL
	store    *memory.PageStore
	tx       *transaction.TransactionContext
	td       *tuple.TupleDescription
	tempDir  string
}

// setupScanBenchmark creates test data for benchmarking
func setupScanBenchmark(b *testing.B, numTuples int) *benchmarkSetup {
	tempDir := b.TempDir()
	filePath := filepath.Join(tempDir, "benchmark.dat")

	td, err := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.IntType, types.StringType},
		[]string{"id", "value", "description"},
	)
	if err != nil {
		b.Fatalf("Failed to create tuple description: %v", err)
	}

	heapFile, err := heap.NewHeapFile(primitives.Filepath(filePath), td)
	if err != nil {
		b.Fatalf("Failed to create heap file: %v", err)
	}

	walPath := filepath.Join(tempDir, "bench.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		b.Fatalf("Failed to create WAL: %v", err)
	}

	store := memory.NewPageStore(wal)
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())

	setup := &benchmarkSetup{
		heapFile: heapFile,
		wal:      wal,
		store:    store,
		tx:       tx,
		td:       td,
		tempDir:  tempDir,
	}

	// Insert test data
	setup.insertBenchmarkData(b, numTuples)

	return setup
}

// insertBenchmarkData inserts test tuples
func (s *benchmarkSetup) insertBenchmarkData(b *testing.B, numTuples int) {
	tuplesPerPage := 10 // Approximate tuples per page

	for i := 0; i < numTuples; i++ {
		tup := tuple.NewTuple(s.td)
		tup.SetField(0, types.NewIntField(int64(i)))
		tup.SetField(1, types.NewIntField(int64(i*100)))
		tup.SetField(2, types.NewStringField(fmt.Sprintf("description_%d", i), 128))

		pageNum := i / tuplesPerPage
		pageID := page.NewPageDescriptor(s.heapFile.GetID(), primitives.PageNumber(pageNum))

		pg, err := s.store.GetPage(s.tx, s.heapFile, pageID, transaction.ReadWrite)
		if err != nil {
			b.Fatalf("Failed to get page: %v", err)
		}

		heapPage := pg.(*heap.HeapPage)
		err = heapPage.AddTuple(tup)
		if err != nil {
			// Page full, continue to next page
			continue
		}

		err = s.heapFile.WritePage(heapPage)
		if err != nil {
			b.Fatalf("Failed to write page: %v", err)
		}
	}

	// Flush to ensure data is written
	s.store.FlushAllPages()
}

// cleanup closes all resources
func (s *benchmarkSetup) cleanup() {
	if s.store != nil {
		s.store.Close()
	}
	if s.wal != nil {
		s.wal.Close()
	}
	if s.heapFile != nil {
		s.heapFile.Close()
	}
}

// BenchmarkSequentialScan benchmarks the standard sequential scan
func BenchmarkSequentialScan(b *testing.B) {
	sizes := []int{100, 1000, 5000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("tuples_%d", size), func(b *testing.B) {
			setup := setupScanBenchmark(b, size)
			defer setup.cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Create new transaction for each iteration
				tx := transaction.NewTransactionContext(primitives.NewTransactionID())

				seqScan, err := NewSeqScan(tx, setup.heapFile.GetID(), setup.heapFile, setup.store)
				if err != nil {
					b.Fatalf("Failed to create sequential scan: %v", err)
				}

				err = seqScan.Open()
				if err != nil {
					b.Fatalf("Failed to open sequential scan: %v", err)
				}

				count := 0
				for {
					hasNext, err := seqScan.HasNext()
					if err != nil {
						b.Fatalf("HasNext failed: %v", err)
					}
					if !hasNext {
						break
					}

					_, err = seqScan.Next()
					if err != nil {
						b.Fatalf("Next failed: %v", err)
					}
					count++
				}

				seqScan.Close()
			}
		})
	}
}

// BenchmarkParallelScan benchmarks the parallel sequential scan
func BenchmarkParallelScan(b *testing.B) {
	sizes := []int{100, 1000, 5000, 10000}
	workerCounts := []int{2, 4, 8}

	for _, size := range sizes {
		for _, workers := range workerCounts {
			b.Run(fmt.Sprintf("tuples_%d_workers_%d", size, workers), func(b *testing.B) {
				setup := setupScanBenchmark(b, size)
				defer setup.cleanup()

				config := ParallelSeqScanConfig{
					NumWorkers:     workers,
					ResultChanSize: 1000,
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					tx := transaction.NewTransactionContext(primitives.NewTransactionID())

					parallelScan, err := NewParallelSeqScan(
						tx,
						setup.heapFile.GetID(),
						setup.heapFile,
						setup.store,
						config,
					)
					if err != nil {
						b.Fatalf("Failed to create parallel scan: %v", err)
					}

					err = parallelScan.Open()
					if err != nil {
						b.Fatalf("Failed to open parallel scan: %v", err)
					}

					count := 0
					for {
						hasNext, err := parallelScan.HasNext()
						if err != nil {
							b.Fatalf("HasNext failed: %v", err)
						}
						if !hasNext {
							break
						}

						_, err = parallelScan.Next()
						if err != nil {
							b.Fatalf("Next failed: %v", err)
						}
						count++
					}

					parallelScan.Close()
				}
			})
		}
	}
}

// BenchmarkScanComparison provides direct comparison between sequential and parallel
func BenchmarkScanComparison(b *testing.B) {
	// Use a large dataset to see parallel benefits
	numTuples := 10000
	setup := setupScanBenchmark(b, numTuples)
	defer setup.cleanup()

	b.Run("Sequential", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx := transaction.NewTransactionContext(primitives.NewTransactionID())

			seqScan, _ := NewSeqScan(tx, setup.heapFile.GetID(), setup.heapFile, setup.store)
			seqScan.Open()

			count := 0
			for {
				hasNext, _ := seqScan.HasNext()
				if !hasNext {
					break
				}
				seqScan.Next()
				count++
			}

			seqScan.Close()
		}
	})

	b.Run("Parallel_2_workers", func(b *testing.B) {
		config := ParallelSeqScanConfig{NumWorkers: 2, ResultChanSize: 1000}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx := transaction.NewTransactionContext(primitives.NewTransactionID())

			parallelScan, _ := NewParallelSeqScan(tx, setup.heapFile.GetID(), setup.heapFile, setup.store, config)
			parallelScan.Open()

			count := 0
			for {
				hasNext, _ := parallelScan.HasNext()
				if !hasNext {
					break
				}
				parallelScan.Next()
				count++
			}

			parallelScan.Close()
		}
	})

	b.Run("Parallel_4_workers", func(b *testing.B) {
		config := ParallelSeqScanConfig{NumWorkers: 4, ResultChanSize: 1000}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx := transaction.NewTransactionContext(primitives.NewTransactionID())

			parallelScan, _ := NewParallelSeqScan(tx, setup.heapFile.GetID(), setup.heapFile, setup.store, config)
			parallelScan.Open()

			count := 0
			for {
				hasNext, _ := parallelScan.HasNext()
				if !hasNext {
					break
				}
				parallelScan.Next()
				count++
			}

			parallelScan.Close()
		}
	})

	cpuCount := runtime.NumCPU()
	b.Run(fmt.Sprintf("Parallel_%d_workers_match_cpu", cpuCount), func(b *testing.B) {
		config := ParallelSeqScanConfig{NumWorkers: cpuCount, ResultChanSize: 1000}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx := transaction.NewTransactionContext(primitives.NewTransactionID())

			parallelScan, _ := NewParallelSeqScan(tx, setup.heapFile.GetID(), setup.heapFile, setup.store, config)
			parallelScan.Open()

			count := 0
			for {
				hasNext, _ := parallelScan.HasNext()
				if !hasNext {
					break
				}
				parallelScan.Next()
				count++
			}

			parallelScan.Close()
		}
	})
}

// BenchmarkScanWithFilter benchmarks scan with filtering
func BenchmarkScanWithFilter(b *testing.B) {
	numTuples := 10000
	setup := setupScanBenchmark(b, numTuples)
	defer setup.cleanup()

	// Filter: id > 5000
	pred := NewPredicate(0, primitives.GreaterThan, types.NewIntField(5000))

	b.Run("Sequential_with_filter", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx := transaction.NewTransactionContext(primitives.NewTransactionID())

			seqScan, _ := NewSeqScan(tx, setup.heapFile.GetID(), setup.heapFile, setup.store)
			filter, _ := NewFilter(pred, seqScan)
			filter.Open()

			count := 0
			for {
				hasNext, _ := filter.HasNext()
				if !hasNext {
					break
				}
				filter.Next()
				count++
			}

			filter.Close()
		}
	})

	b.Run("Parallel_with_filter", func(b *testing.B) {
		config := ParallelSeqScanConfig{NumWorkers: 4, ResultChanSize: 1000}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx := transaction.NewTransactionContext(primitives.NewTransactionID())

			parallelScan, _ := NewParallelSeqScan(tx, setup.heapFile.GetID(), setup.heapFile, setup.store, config)
			filter, _ := NewFilter(pred, parallelScan)
			filter.Open()

			count := 0
			for {
				hasNext, _ := filter.HasNext()
				if !hasNext {
					break
				}
				filter.Next()
				count++
			}

			filter.Close()
		}
	})
}

// BenchmarkMemoryPressure tests memory usage under different scan types
func BenchmarkMemoryPressure(b *testing.B) {
	numTuples := 20000 // Large dataset
	setup := setupScanBenchmark(b, numTuples)
	defer setup.cleanup()

	b.Run("Sequential_memory", func(b *testing.B) {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		allocsBefore := m.TotalAlloc

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx := transaction.NewTransactionContext(primitives.NewTransactionID())
			seqScan, _ := NewSeqScan(tx, setup.heapFile.GetID(), setup.heapFile, setup.store)
			seqScan.Open()

			for {
				hasNext, _ := seqScan.HasNext()
				if !hasNext {
					break
				}
				seqScan.Next()
			}

			seqScan.Close()
		}
		b.StopTimer()

		runtime.ReadMemStats(&m)
		allocsAfter := m.TotalAlloc
		b.ReportMetric(float64(allocsAfter-allocsBefore)/float64(b.N), "bytes/op")
	})

	b.Run("Parallel_memory", func(b *testing.B) {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		allocsBefore := m.TotalAlloc

		config := ParallelSeqScanConfig{NumWorkers: 4, ResultChanSize: 1000}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx := transaction.NewTransactionContext(primitives.NewTransactionID())
			parallelScan, _ := NewParallelSeqScan(tx, setup.heapFile.GetID(), setup.heapFile, setup.store, config)
			parallelScan.Open()

			for {
				hasNext, _ := parallelScan.HasNext()
				if !hasNext {
					break
				}
				parallelScan.Next()
			}

			parallelScan.Close()
		}
		b.StopTimer()

		runtime.ReadMemStats(&m)
		allocsAfter := m.TotalAlloc
		b.ReportMetric(float64(allocsAfter-allocsBefore)/float64(b.N), "bytes/op")
	})
}
