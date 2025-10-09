package catalog

import (
	"storemy/pkg/concurrency/transaction"
	"testing"
	"time"
)

// mockDB is a mock database for testing
type mockDB struct{}

func (m *mockDB) BeginTransaction() (*transaction.TransactionContext, error) {
	return nil, nil
}

func (m *mockDB) CommitTransaction(tx *transaction.TransactionContext) error {
	return nil
}

func TestStatisticsManager_RecordModification(t *testing.T) {
	catalog := &SystemCatalog{} // Mock catalog
	db := &mockDB{}
	sm := NewStatisticsManager(catalog, db)

	tableID := 1

	// Initially no modifications
	if sm.modificationCount[tableID] != 0 {
		t.Errorf("Expected 0 modifications initially, got %d", sm.modificationCount[tableID])
	}

	// Record one modification
	sm.RecordModification(tableID)
	if sm.modificationCount[tableID] != 1 {
		t.Errorf("Expected 1 modification, got %d", sm.modificationCount[tableID])
	}

	// Record multiple modifications
	for i := 0; i < 99; i++ {
		sm.RecordModification(tableID)
	}
	if sm.modificationCount[tableID] != 100 {
		t.Errorf("Expected 100 modifications, got %d", sm.modificationCount[tableID])
	}
}

func TestStatisticsManager_ShouldUpdateStatistics_Threshold(t *testing.T) {
	catalog := &SystemCatalog{}
	db := &mockDB{}
	sm := NewStatisticsManager(catalog, db)
	sm.SetUpdateThreshold(10) // Low threshold for testing

	tableID := 1

	// Should not update with 0 modifications (but true because never updated)
	if !sm.ShouldUpdateStatistics(tableID) {
		t.Error("Expected true for never-updated table")
	}

	// Mark as updated
	sm.mu.Lock()
	sm.lastUpdate[tableID] = time.Now()
	sm.modificationCount[tableID] = 0
	sm.mu.Unlock()

	// Should not update with low count
	for i := 0; i < 9; i++ {
		sm.RecordModification(tableID)
	}
	if sm.ShouldUpdateStatistics(tableID) {
		t.Error("Should not update with only 9 modifications (threshold is 10)")
	}

	// Should update when threshold reached
	sm.RecordModification(tableID) // 10th modification
	if !sm.ShouldUpdateStatistics(tableID) {
		t.Error("Should update when threshold (10) is reached")
	}
}

func TestStatisticsManager_ShouldUpdateStatistics_TimeInterval(t *testing.T) {
	catalog := &SystemCatalog{}
	db := &mockDB{}
	sm := NewStatisticsManager(catalog, db)
	sm.SetUpdateInterval(100 * time.Millisecond) // Short interval for testing

	tableID := 1

	// Mark as recently updated with low modification count
	sm.mu.Lock()
	sm.lastUpdate[tableID] = time.Now()
	sm.modificationCount[tableID] = 5 // Below threshold
	sm.mu.Unlock()

	// Should not update immediately
	if sm.ShouldUpdateStatistics(tableID) {
		t.Error("Should not update immediately after recent update")
	}

	// Wait for interval to pass
	time.Sleep(150 * time.Millisecond)

	// Should update now due to time interval
	if !sm.ShouldUpdateStatistics(tableID) {
		t.Error("Should update after time interval has passed")
	}
}

func TestStatisticsManager_SetThresholdAndInterval(t *testing.T) {
	catalog := &SystemCatalog{}
	db := &mockDB{}
	sm := NewStatisticsManager(catalog, db)

	// Test setting threshold
	sm.SetUpdateThreshold(500)
	if sm.updateThreshold != 500 {
		t.Errorf("Expected threshold 500, got %d", sm.updateThreshold)
	}

	// Test setting interval
	newInterval := 10 * time.Minute
	sm.SetUpdateInterval(newInterval)
	if sm.updateInterval != newInterval {
		t.Errorf("Expected interval %v, got %v", newInterval, sm.updateInterval)
	}
}

func TestStatisticsManager_ConcurrentAccess(t *testing.T) {
	catalog := &SystemCatalog{}
	db := &mockDB{}
	sm := NewStatisticsManager(catalog, db)

	tableID := 1
	goroutines := 100
	modificationsPerGoroutine := 10

	// Concurrently record modifications
	done := make(chan bool, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < modificationsPerGoroutine; j++ {
				sm.RecordModification(tableID)
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < goroutines; i++ {
		<-done
	}

	// Verify count
	expected := goroutines * modificationsPerGoroutine
	sm.mu.RLock()
	actual := sm.modificationCount[tableID]
	sm.mu.RUnlock()

	if actual != expected {
		t.Errorf("Expected %d modifications, got %d (possible race condition)", expected, actual)
	}
}

func TestStatisticsManager_MultipleTablesTracking(t *testing.T) {
	catalog := &SystemCatalog{}
	db := &mockDB{}
	sm := NewStatisticsManager(catalog, db)

	// Record modifications for multiple tables
	sm.RecordModification(1)
	sm.RecordModification(1)
	sm.RecordModification(2)
	sm.RecordModification(3)
	sm.RecordModification(2)

	// Verify counts
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.modificationCount[1] != 2 {
		t.Errorf("Table 1: expected 2 modifications, got %d", sm.modificationCount[1])
	}
	if sm.modificationCount[2] != 2 {
		t.Errorf("Table 2: expected 2 modifications, got %d", sm.modificationCount[2])
	}
	if sm.modificationCount[3] != 1 {
		t.Errorf("Table 3: expected 1 modification, got %d", sm.modificationCount[3])
	}
}
