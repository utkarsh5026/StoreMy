package planner

import (
	"os"
	"path/filepath"
	"storemy/pkg/catalog"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log"
	"storemy/pkg/memory"
	"storemy/pkg/registry"
	"testing"
)

func createTransactionContext(t *testing.T) TransactionCtx {
	t.Helper()

	rg := transaction.NewTransactionRegistry(nil)
	ctx, err := rg.Begin()

	if err != nil {
		t.Fatalf("Error creating transaction Context")
	}

	return ctx
}

// cleanupTable registers cleanup for table files to ensure proper resource cleanup on Windows
func cleanupTable(t *testing.T, tableManager *memory.TableManager, tableName string) {
	t.Helper()
	t.Cleanup(func() {
		tableID, err := tableManager.GetTableID(tableName)
		if err == nil {
			if info, err := tableManager.GetTableInfo(tableID); err == nil {
				info.File.Close()
			}
		}
	})
}

// Helper function to create a test database context with cleanup registration
func createTestContextWithCleanup(t *testing.T, dataDir string) *registry.DatabaseContext {
	tm := memory.NewTableManager()

	tmpDir, err := os.MkdirTemp("", "test_wal_*")
	if err != nil {
		if t != nil {
			t.Fatalf("failed to create temp dir for WAL: %v", err)
		}
		panic(err)
	}

	walPath := filepath.Join(tmpDir, "test.wal")
	wal, err := log.NewWAL(walPath, 8192)
	if err != nil {
		os.RemoveAll(tmpDir)
		if t != nil {
			t.Fatalf("failed to create WAL: %v", err)
		}
		panic(err)
	}

	pageStore := memory.NewPageStore(tm, wal)
	catalogMgr := catalog.NewCatalogManager(pageStore, tm)
	txRegistry := transaction.NewTransactionRegistry(wal)
	tx, err := txRegistry.Begin()
	if err == nil {
		_ = catalogMgr.Initialize(tx, dataDir)
	}

	if t != nil {
		t.Cleanup(func() {
			if tm != nil {
				tm.Clear()
			}
			if wal != nil {
				wal.Close()
			}
			os.RemoveAll(tmpDir)
		})
	}

	return registry.NewDatabaseContext(
		tm,
		pageStore,
		catalogMgr.GetSystemCatalog(),
		catalogMgr,
		wal,
		dataDir,
	)
}

// Add this helper function near the top of the file after imports
func setupTestDataDir(t *testing.T) string {
	t.Helper()
	dataDir := t.TempDir()
	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working directory: %v", err)
	}
	if err := os.Chdir(dataDir); err != nil {
		t.Fatalf("Failed to change to temp directory: %v", err)
	}
	t.Cleanup(func() { os.Chdir(oldDir) })

	if err := os.Mkdir("data", 0755); err != nil {
		t.Fatalf("Failed to create data directory: %v", err)
	}

	return dataDir
}
