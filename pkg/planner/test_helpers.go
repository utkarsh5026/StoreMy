package planner

import (
	"os"
	"path/filepath"
	"storemy/pkg/catalog/catalogmanager"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log/wal"
	"storemy/pkg/memory"
	"storemy/pkg/parser/statements"
	"storemy/pkg/registry"
	"storemy/pkg/storage/index"
	"testing"
)

// Global transaction registry for tests (will be set by createTestContextWithCleanup)
var testTxRegistry *transaction.TransactionRegistry

func createTransactionContext(t *testing.T) TxContext {
	t.Helper()

	if testTxRegistry == nil {
		t.Fatal("Test transaction registry not initialized. Call createTestContextWithCleanup first.")
	}

	ctx, err := testTxRegistry.Begin()
	if err != nil {
		t.Fatalf("Error creating transaction Context: %v", err)
	}

	return ctx
}

// cleanupTable registers cleanup for table files to ensure proper resource cleanup on Windows
func cleanupTable(t *testing.T, catalogMgr *catalogmanager.CatalogManager, tableName string, txID interface{}) {
	t.Helper()
	t.Cleanup(func() {
		// Tables will be cleaned up when catalogMgr.ClearCache() is called
		// No need to individually close files here
	})
}

// Helper function to create a test database context with cleanup registration
func createTestContextWithCleanup(t *testing.T, dataDir string) *registry.DatabaseContext {
	tmpDir, err := os.MkdirTemp("", "test_wal_*")
	if err != nil {
		if t != nil {
			t.Fatalf("failed to create temp dir for WAL: %v", err)
		}
		panic(err)
	}

	walPath := filepath.Join(tmpDir, "test.wal")
	wal, err := wal.NewWAL(walPath, 8192)
	if err != nil {
		os.RemoveAll(tmpDir)
		if t != nil {
			t.Fatalf("failed to create WAL: %v", err)
		}
		panic(err)
	}

	pageStore := memory.NewPageStore(wal)
	catalogMgr := catalogmanager.NewCatalogManager(pageStore, dataDir)

	// Create transaction registry and store it globally for test transactions
	txRegistry := transaction.NewTransactionRegistry(wal)
	testTxRegistry = txRegistry

	tx, err := txRegistry.Begin()
	if err != nil {
		if t != nil {
			t.Fatalf("failed to begin init transaction: %v", err)
		}
		panic(err)
	}

	if initErr := catalogMgr.Initialize(tx); initErr != nil {
		if t != nil {
			t.Fatalf("failed to initialize catalog: %v", initErr)
		}
		panic(initErr)
	}

	// Verify catalog is accessible
	if t != nil {
		testTx, _ := txRegistry.Begin()
		if !catalogMgr.TableExists(testTx, "CATALOG_TABLES") {
			t.Logf("WARNING: CATALOG_TABLES does not exist after initialization")
		} else {
			t.Logf("SUCCESS: CATALOG_TABLES exists after initialization")
		}
	}

	if t != nil {
		t.Cleanup(func() {
			// Reset global registry
			testTxRegistry = nil

			if catalogMgr != nil {
				catalogMgr.ClearCache()
			}
			if wal != nil {
				wal.Close()
			}
			os.RemoveAll(tmpDir)
		})
	}

	return registry.NewDatabaseContext(
		pageStore,
		catalogMgr,
		wal,
		dataDir,
	)
}

// setupTestDataDir creates a temporary data directory for tests and returns its absolute path
func setupTestDataDir(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("Failed to create data directory: %v", err)
	}

	return dataDir
}

// createTestIndex creates an index for testing purposes
func createTestIndex(t *testing.T, ctx DbContext, transCtx TxContext, tableName, indexName, columnName string, indexType index.IndexType) {
	t.Helper()
	stmt := statements.NewCreateIndexStatement(indexName, tableName, columnName, indexType, false)
	plan := NewCreateIndexPlan(stmt, ctx, transCtx)
	_, err := plan.Execute()
	if err != nil {
		t.Fatalf("Failed to create test index: %v", err)
	}
}
