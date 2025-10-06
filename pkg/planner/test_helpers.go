package planner

import (
	"os"
	"path/filepath"
	"storemy/pkg/log"
	"storemy/pkg/memory"
	"storemy/pkg/registry"
	"testing"
)

// cleanupTable registers cleanup for table files to ensure proper resource cleanup on Windows
func cleanupTable(t *testing.T, tableManager *memory.TableManager, tableName string) {
	t.Helper()
	t.Cleanup(func() {
		tableID, err := tableManager.GetTableID(tableName)
		if err == nil {
			if dbFile, err := tableManager.GetDbFile(tableID); err == nil {
				dbFile.Close()
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

	if t != nil {
		t.Cleanup(func() {
			if wal != nil {
				wal.Close()
			}
			os.RemoveAll(tmpDir)
		})
	}

	pageStore := memory.NewPageStore(tm, wal)
	return registry.NewDatabaseContext(
		tm,
		pageStore,
		nil,
		wal,
		dataDir,
	)
}
