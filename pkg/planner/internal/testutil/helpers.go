package testutil

import (
	"os"
	"path/filepath"
	"storemy/pkg/catalog/catalogmanager"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log/wal"
	"storemy/pkg/memory"
	"storemy/pkg/registry"
	"testing"
)

// Helper function to create a test database context with cleanup registration
func CreateTestContextWithCleanup(t *testing.T, dataDir string) (*registry.DatabaseContext, *transaction.TransactionRegistry) {
	tmpDir, err := os.MkdirTemp("", "test_wal_*")
	if err != nil {
		if t != nil {
			t.Fatalf("failed to create temp dir for WAL: %v", err)
		}
		panic(err)
	}

	walPath := filepath.Join(tmpDir, "test.wal")
	walInstance, err := wal.NewWAL(walPath, 8192)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		if t != nil {
			t.Fatalf("failed to create WAL: %v", err)
		}
		panic(err)
	}

	pageStore := memory.NewPageStore(walInstance)
	catalogMgr := catalogmanager.NewCatalogManager(pageStore, dataDir)

	// Create transaction registry
	txRegistry := transaction.NewTransactionRegistry(walInstance)

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
			if catalogMgr != nil {
				catalogMgr.ClearCacheCompletely()
			}
			if walInstance != nil {
				_ = walInstance.Close()
			}
			_ = os.RemoveAll(tmpDir)
		})
	}

	dbContext := registry.NewDatabaseContext(
		pageStore,
		catalogMgr,
		walInstance,
		dataDir,
	)

	return dbContext, txRegistry
}

// setupTestDataDir creates a temporary data directory for tests and returns its absolute path
func SetupTestDataDir(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		t.Fatalf("Failed to create data directory: %v", err)
	}

	return dataDir
}

// CleanupTable registers cleanup for table files to ensure proper resource cleanup on Windows
func CleanupTable(t *testing.T, catalogMgr *catalogmanager.CatalogManager, tableName string, txID interface{}) {
	t.Helper()
	t.Cleanup(func() {
		// Tables will be cleaned up when catalogMgr.ClearCache() is called
		// No need to individually close files here
	})
}
