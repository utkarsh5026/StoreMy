package planner

import (
	"os"
	"path/filepath"
	"storemy/pkg/log"
	"storemy/pkg/memory"
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

func createWal(t *testing.T) *log.WAL {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "wal_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	logPath := filepath.Join(tmpDir, "test.wal")
	wal, err := log.NewWAL(logPath, 4096)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	t.Cleanup(func() {
		wal.Close()
		os.RemoveAll(tmpDir)
	})

	return wal
}
