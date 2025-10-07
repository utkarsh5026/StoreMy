package transaction

import (
	"path/filepath"
	"storemy/pkg/log"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"sync"
	"testing"
	"time"
)

// mockPageID implements tuple.PageID for testing
type mockPageID struct {
	tableID int
	pageNo  int
}

func (m *mockPageID) GetTableID() int  { return m.tableID }
func (m *mockPageID) PageNo() int      { return m.pageNo }
func (m *mockPageID) Serialize() []int { return []int{m.tableID, m.pageNo} }
func (m *mockPageID) String() string   { return "mockPageID" }
func (m *mockPageID) HashCode() int    { return m.tableID*31 + m.pageNo }
func (m *mockPageID) Equals(other tuple.PageID) bool {
	if other == nil {
		return false
	}
	return m.tableID == other.GetTableID() && m.pageNo == other.PageNo()
}

// Helper function to create a temporary WAL for testing
func createTestWAL(t *testing.T) (*log.WAL, string) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "test.wal")
	wal, err := log.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create test WAL: %v", err)
	}
	return wal, walPath
}

// TestTransactionStatus_String tests the string representation of transaction statuses
func TestTransactionStatus_String(t *testing.T) {
	tests := []struct {
		status   TransactionStatus
		expected string
	}{
		{TxActive, "ACTIVE"},
		{TxCommitting, "COMMITTING"},
		{TxAborting, "ABORTING"},
		{TxCommitted, "COMMITTED"},
		{TxAborted, "ABORTED"},
		{TransactionStatus(999), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.status.String()
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

// TestNewTransactionContext tests creating a new transaction context
func TestNewTransactionContext(t *testing.T) {
	tid := primitives.NewTransactionID()
	ctx := NewTransactionContext(tid)

	if ctx == nil {
		t.Fatal("NewTransactionContext returned nil")
	}

	if ctx.ID != tid {
		t.Errorf("Expected transaction ID %v, got %v", tid, ctx.ID)
	}

	if ctx.status != TxActive {
		t.Errorf("Expected status TxActive, got %v", ctx.status)
	}

	if ctx.lockedPages == nil {
		t.Error("lockedPages map is nil")
	}

	if ctx.dirtyPages == nil {
		t.Error("dirtyPages map is nil")
	}

	if ctx.waitingFor == nil {
		t.Error("waitingFor slice is nil")
	}

	if ctx.begunInWAL {
		t.Error("Expected begunInWAL to be false")
	}

	if ctx.firstLSN != 0 || ctx.lastLSN != 0 || ctx.undoNextLSN != 0 {
		t.Error("Expected LSN values to be 0")
	}

	if ctx.startTime.IsZero() {
		t.Error("Expected startTime to be set")
	}

	if !ctx.endTime.IsZero() {
		t.Error("Expected endTime to be zero")
	}
}

// TestTransactionContext_IsActive tests the IsActive method
func TestTransactionContext_IsActive(t *testing.T) {
	tid := primitives.NewTransactionID()
	ctx := NewTransactionContext(tid)

	if !ctx.IsActive() {
		t.Error("Expected new transaction to be active")
	}

	ctx.SetStatus(TxCommitted)
	if ctx.IsActive() {
		t.Error("Expected committed transaction to not be active")
	}

	ctx.SetStatus(TxActive)
	if !ctx.IsActive() {
		t.Error("Expected transaction to be active after setting status")
	}
}

// TestTransactionContext_SetStatus tests the SetStatus method
func TestTransactionContext_SetStatus(t *testing.T) {
	tid := primitives.NewTransactionID()
	ctx := NewTransactionContext(tid)

	statuses := []TransactionStatus{
		TxActive,
		TxCommitting,
		TxCommitted,
		TxAborting,
		TxAborted,
	}

	for _, status := range statuses {
		ctx.SetStatus(status)
		if ctx.GetStatus() != status {
			t.Errorf("Expected status %v, got %v", status, ctx.GetStatus())
		}

		// Check that endTime is set for terminal states
		if status == TxCommitted || status == TxAborted {
			if ctx.endTime.IsZero() {
				t.Errorf("Expected endTime to be set for status %v", status)
			}
		}
	}
}

// TestTransactionContext_RecordPageAccess tests page access recording
func TestTransactionContext_RecordPageAccess(t *testing.T) {
	tid := primitives.NewTransactionID()
	ctx := NewTransactionContext(tid)

	pid1 := &mockPageID{tableID: 1, pageNo: 1}
	pid2 := &mockPageID{tableID: 1, pageNo: 2}

	// Record read-only access
	ctx.RecordPageAccess(pid1, ReadOnly)
	lockedPages := ctx.GetLockedPages()
	if len(lockedPages) != 1 {
		t.Errorf("Expected 1 locked page, got %d", len(lockedPages))
	}
	if ctx.pagesRead != 1 {
		t.Errorf("Expected pagesRead to be 1, got %d", ctx.pagesRead)
	}

	// Record read-write access
	ctx.RecordPageAccess(pid2, ReadWrite)
	lockedPages = ctx.GetLockedPages()
	if len(lockedPages) != 2 {
		t.Errorf("Expected 2 locked pages, got %d", len(lockedPages))
	}

	// Upgrade read-only to read-write
	ctx.RecordPageAccess(pid1, ReadWrite)
	lockedPages = ctx.GetLockedPages()
	if len(lockedPages) != 2 {
		t.Errorf("Expected 2 locked pages after upgrade, got %d", len(lockedPages))
	}

	// Try to downgrade read-write to read-only (should not change)
	initialPagesRead := ctx.pagesRead
	ctx.RecordPageAccess(pid2, ReadOnly)
	if ctx.lockedPages[pid2] != ReadWrite {
		t.Error("Expected page to remain read-write")
	}
	if ctx.pagesRead != initialPagesRead {
		t.Error("pagesRead should not increase when re-accessing with lower permission")
	}
}

// TestTransactionContext_MarkPageDirty tests marking pages as dirty
func TestTransactionContext_MarkPageDirty(t *testing.T) {
	tid := primitives.NewTransactionID()
	ctx := NewTransactionContext(tid)

	pid1 := &mockPageID{tableID: 1, pageNo: 1}
	pid2 := &mockPageID{tableID: 1, pageNo: 2}

	// Mark first page dirty
	ctx.MarkPageDirty(pid1)
	dirtyPages := ctx.GetDirtyPages()
	if len(dirtyPages) != 1 {
		t.Errorf("Expected 1 dirty page, got %d", len(dirtyPages))
	}
	if ctx.pagesWritten != 1 {
		t.Errorf("Expected pagesWritten to be 1, got %d", ctx.pagesWritten)
	}

	// Mark same page dirty again (should not increase counter)
	ctx.MarkPageDirty(pid1)
	dirtyPages = ctx.GetDirtyPages()
	if len(dirtyPages) != 1 {
		t.Errorf("Expected 1 dirty page after remarking, got %d", len(dirtyPages))
	}
	if ctx.pagesWritten != 1 {
		t.Errorf("Expected pagesWritten to remain 1, got %d", ctx.pagesWritten)
	}

	// Mark second page dirty
	ctx.MarkPageDirty(pid2)
	dirtyPages = ctx.GetDirtyPages()
	if len(dirtyPages) != 2 {
		t.Errorf("Expected 2 dirty pages, got %d", len(dirtyPages))
	}
	if ctx.pagesWritten != 2 {
		t.Errorf("Expected pagesWritten to be 2, got %d", ctx.pagesWritten)
	}
}

// TestTransactionContext_WaitingFor tests the waiting for functionality
func TestTransactionContext_WaitingFor(t *testing.T) {
	tid := primitives.NewTransactionID()
	ctx := NewTransactionContext(tid)

	pid1 := &mockPageID{tableID: 1, pageNo: 1}
	pid2 := &mockPageID{tableID: 1, pageNo: 2}
	pid3 := &mockPageID{tableID: 1, pageNo: 3}

	// Add pages to waiting list
	ctx.AddWaitingFor(pid1)
	ctx.AddWaitingFor(pid2)
	ctx.AddWaitingFor(pid3)

	waiting := ctx.GetWaitingFor()
	if len(waiting) != 3 {
		t.Errorf("Expected 3 waiting pages, got %d", len(waiting))
	}

	// Remove a page
	ctx.RemoveWaitingFor(pid2)
	waiting = ctx.GetWaitingFor()
	if len(waiting) != 2 {
		t.Errorf("Expected 2 waiting pages after removal, got %d", len(waiting))
	}

	// Verify pid2 is not in the list
	for _, pid := range waiting {
		if pid.Equals(pid2) {
			t.Error("Expected pid2 to be removed from waiting list")
		}
	}

	// Remove non-existent page (should not crash)
	ctx.RemoveWaitingFor(&mockPageID{tableID: 99, pageNo: 99})
	waiting = ctx.GetWaitingFor()
	if len(waiting) != 2 {
		t.Errorf("Expected 2 waiting pages after removing non-existent, got %d", len(waiting))
	}
}

// TestTransactionContext_LSNTracking tests LSN tracking
func TestTransactionContext_LSNTracking(t *testing.T) {
	tid := primitives.NewTransactionID()
	ctx := NewTransactionContext(tid)

	if ctx.GetFirstLSN() != 0 {
		t.Error("Expected initial firstLSN to be 0")
	}
	if ctx.GetLastLSN() != 0 {
		t.Error("Expected initial lastLSN to be 0")
	}

	// Update LSN
	ctx.UpdateLSN(primitives.LSN(100))
	if ctx.GetFirstLSN() != 100 {
		t.Errorf("Expected firstLSN to be 100, got %d", ctx.GetFirstLSN())
	}
	if ctx.GetLastLSN() != 100 {
		t.Errorf("Expected lastLSN to be 100, got %d", ctx.GetLastLSN())
	}

	// Update LSN again
	ctx.UpdateLSN(primitives.LSN(200))
	if ctx.GetFirstLSN() != 100 {
		t.Errorf("Expected firstLSN to remain 100, got %d", ctx.GetFirstLSN())
	}
	if ctx.GetLastLSN() != 200 {
		t.Errorf("Expected lastLSN to be 200, got %d", ctx.GetLastLSN())
	}
}

// TestTransactionContext_EnsureBegunInWAL tests WAL begin record
func TestTransactionContext_EnsureBegunInWAL(t *testing.T) {
	wal, _ := createTestWAL(t)
	defer wal.Close()

	tid := primitives.NewTransactionID()
	ctx := NewTransactionContext(tid)

	if ctx.begunInWAL {
		t.Error("Expected begunInWAL to be false initially")
	}

	// First call should write BEGIN record
	err := ctx.EnsureBegunInWAL(wal)
	if err != nil {
		t.Fatalf("Failed to ensure begun in WAL: %v", err)
	}

	if !ctx.begunInWAL {
		t.Error("Expected begunInWAL to be true after ensuring")
	}

	firstLSN := ctx.GetFirstLSN()
	lastLSN := ctx.GetLastLSN()

	// FirstLSN can be 0 (primitives.FirstLSN), which is valid
	if firstLSN != lastLSN {
		t.Error("Expected firstLSN and lastLSN to be equal after begin")
	}

	// Second call should be idempotent
	err = ctx.EnsureBegunInWAL(wal)
	if err != nil {
		t.Fatalf("Second EnsureBegunInWAL call failed: %v", err)
	}

	if ctx.GetFirstLSN() != firstLSN || ctx.GetLastLSN() != lastLSN {
		t.Error("Expected LSNs to remain unchanged on second call")
	}
}

// TestTransactionContext_Statistics tests statistics tracking
func TestTransactionContext_Statistics(t *testing.T) {
	tid := primitives.NewTransactionID()
	ctx := NewTransactionContext(tid)

	pid := &mockPageID{tableID: 1, pageNo: 1}

	// Record various operations
	ctx.RecordPageAccess(pid, ReadOnly)
	ctx.MarkPageDirty(pid)
	ctx.RecordTupleRead()
	ctx.RecordTupleRead()
	ctx.RecordTupleWrite()
	ctx.RecordTupleDelete()

	stats := ctx.GetStatistics()

	if stats.PagesRead != 1 {
		t.Errorf("Expected pages_read to be 1, got %d", stats.PagesRead)
	}
	if stats.PagesWritten != 1 {
		t.Errorf("Expected pages_written to be 1, got %d", stats.PagesWritten)
	}
	if stats.TuplesRead != 2 {
		t.Errorf("Expected tuples_read to be 2, got %d", stats.TuplesRead)
	}
	if stats.TuplesWritten != 1 {
		t.Errorf("Expected tuples_written to be 1, got %d", stats.TuplesWritten)
	}
	if stats.TuplesDeleted != 1 {
		t.Errorf("Expected tuples_deleted to be 1, got %d", stats.TuplesDeleted)
	}
	if stats.LockedPages != 1 {
		t.Errorf("Expected locked_pages to be 1, got %d", stats.LockedPages)
	}
	if stats.LockedPages != 1 {
		t.Errorf("Expected dirty_pages to be 1, got %d", stats.LockedPages)
	}
}

// TestTransactionContext_Duration tests duration calculation
func TestTransactionContext_Duration(t *testing.T) {
	tid := primitives.NewTransactionID()
	ctx := NewTransactionContext(tid)

	// Duration for active transaction
	time.Sleep(10 * time.Millisecond)
	duration := ctx.Duration()
	if duration < 10*time.Millisecond {
		t.Errorf("Expected duration >= 10ms, got %v", duration)
	}

	// Duration for completed transaction
	ctx.SetStatus(TxCommitted)
	duration1 := ctx.Duration()
	time.Sleep(10 * time.Millisecond)
	duration2 := ctx.Duration()

	// Duration should not increase after transaction ends
	if duration2 != duration1 {
		t.Error("Expected duration to remain constant after transaction ends")
	}
}

// TestTransactionContext_String tests string representation
func TestTransactionContext_String(t *testing.T) {
	tid := primitives.NewTransactionID()
	ctx := NewTransactionContext(tid)

	pid := &mockPageID{tableID: 1, pageNo: 1}
	ctx.RecordPageAccess(pid, ReadWrite)
	ctx.MarkPageDirty(pid)

	str := ctx.String()
	if str == "" {
		t.Error("Expected non-empty string representation")
	}

	// String should contain transaction ID and status
	if len(str) < 10 {
		t.Error("String representation seems too short")
	}
}

// TestTransactionContext_Concurrency tests concurrent access to transaction context
func TestTransactionContext_Concurrency(t *testing.T) {
	tid := primitives.NewTransactionID()
	ctx := NewTransactionContext(tid)

	const goroutines = 10
	const operationsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	// Concurrent operations
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				pid := &mockPageID{tableID: id, pageNo: j}
				ctx.RecordPageAccess(pid, ReadWrite)
				ctx.MarkPageDirty(pid)
				ctx.RecordTupleRead()
				ctx.RecordTupleWrite()
				_ = ctx.GetStatus()
				_ = ctx.GetDirtyPages()
				_ = ctx.GetLockedPages()
			}
		}(i)
	}

	wg.Wait()

	// Verify final state
	stats := ctx.GetStatistics()
	expectedTuples := goroutines * operationsPerGoroutine

	if stats.TuplesRead != expectedTuples {
		t.Errorf("Expected tuples_read to be %d, got %d", expectedTuples, stats.TuplesRead)
	}
	if stats.TuplesWritten != expectedTuples {
		t.Errorf("Expected tuples_written to be %d, got %d", expectedTuples, stats.TuplesWritten)
	}
}

// TestNewTransactionRegistry tests creating a new registry
func TestNewTransactionRegistry(t *testing.T) {
	wal, _ := createTestWAL(t)
	defer wal.Close()

	registry := NewTransactionRegistry(wal)

	if registry == nil {
		t.Fatal("NewTransactionRegistry returned nil")
	}

	if registry.contexts == nil {
		t.Error("contexts map is nil")
	}

	if registry.wal != wal {
		t.Error("WAL not set correctly")
	}

	if registry.Count() != 0 {
		t.Errorf("Expected empty registry, got count %d", registry.Count())
	}
}

// TestTransactionRegistry_Begin tests beginning a transaction
func TestTransactionRegistry_Begin(t *testing.T) {
	wal, _ := createTestWAL(t)
	defer wal.Close()

	registry := NewTransactionRegistry(wal)

	ctx, err := registry.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	if ctx == nil {
		t.Fatal("Begin returned nil context")
	}

	if !ctx.IsActive() {
		t.Error("Expected new transaction to be active")
	}

	if registry.Count() != 1 {
		t.Errorf("Expected registry count to be 1, got %d", registry.Count())
	}

	// Begin another transaction
	ctx2, err := registry.Begin()
	if err != nil {
		t.Fatalf("Failed to begin second transaction: %v", err)
	}

	if ctx2.ID.Equals(ctx.ID) {
		t.Error("Expected different transaction IDs")
	}

	if registry.Count() != 2 {
		t.Errorf("Expected registry count to be 2, got %d", registry.Count())
	}
}

// TestTransactionRegistry_Get tests retrieving a transaction
func TestTransactionRegistry_Get(t *testing.T) {
	wal, _ := createTestWAL(t)
	defer wal.Close()

	registry := NewTransactionRegistry(wal)

	ctx, _ := registry.Begin()

	// Get existing transaction
	retrieved, err := registry.Get(ctx.ID)
	if err != nil {
		t.Fatalf("Failed to get transaction: %v", err)
	}

	if retrieved != ctx {
		t.Error("Expected to retrieve the same context")
	}

	// Get non-existent transaction
	fakeTID := primitives.NewTransactionID()
	_, err = registry.Get(fakeTID)
	if err == nil {
		t.Error("Expected error when getting non-existent transaction")
	}
}

// TestTransactionRegistry_GetOrCreate tests get or create functionality
func TestTransactionRegistry_GetOrCreate(t *testing.T) {
	wal, _ := createTestWAL(t)
	defer wal.Close()

	registry := NewTransactionRegistry(wal)

	tid := primitives.NewTransactionID()

	// First call should create
	ctx1 := registry.GetOrCreate(tid)
	if ctx1 == nil {
		t.Fatal("GetOrCreate returned nil")
	}

	if registry.Count() != 1 {
		t.Errorf("Expected count to be 1, got %d", registry.Count())
	}

	// Second call should return existing
	ctx2 := registry.GetOrCreate(tid)
	if ctx2 != ctx1 {
		t.Error("Expected to get the same context")
	}

	if registry.Count() != 1 {
		t.Errorf("Expected count to remain 1, got %d", registry.Count())
	}

	// Different TID should create new
	tid2 := primitives.NewTransactionID()
	ctx3 := registry.GetOrCreate(tid2)
	if ctx3 == ctx1 {
		t.Error("Expected different context for different TID")
	}

	if registry.Count() != 2 {
		t.Errorf("Expected count to be 2, got %d", registry.Count())
	}
}

// TestTransactionRegistry_Remove tests removing a transaction
func TestTransactionRegistry_Remove(t *testing.T) {
	wal, _ := createTestWAL(t)
	defer wal.Close()

	registry := NewTransactionRegistry(wal)

	ctx, _ := registry.Begin()
	initialCount := registry.Count()

	registry.Remove(ctx.ID)

	if registry.Count() != initialCount-1 {
		t.Errorf("Expected count to decrease by 1")
	}

	// Verify transaction is really gone
	_, err := registry.Get(ctx.ID)
	if err == nil {
		t.Error("Expected error when getting removed transaction")
	}

	// Removing non-existent transaction should not crash
	registry.Remove(primitives.NewTransactionID())
}

// TestTransactionRegistry_GetActive tests getting active transactions
func TestTransactionRegistry_GetActive(t *testing.T) {
	wal, _ := createTestWAL(t)
	defer wal.Close()

	registry := NewTransactionRegistry(wal)

	// Create several transactions with different statuses
	ctx1, _ := registry.Begin()
	ctx2, _ := registry.Begin()
	ctx3, _ := registry.Begin()
	ctx4, _ := registry.Begin()

	ctx2.SetStatus(TxCommitted)
	ctx4.SetStatus(TxAborted)

	active := registry.GetActive()

	if len(active) != 2 {
		t.Errorf("Expected 2 active transactions, got %d", len(active))
	}

	// Verify only active transactions are returned
	for _, ctx := range active {
		if !ctx.IsActive() {
			t.Error("GetActive returned non-active transaction")
		}
		if ctx != ctx1 && ctx != ctx3 {
			t.Error("GetActive returned unexpected transaction")
		}
	}
}

// TestTransactionRegistry_GetAllTransactionIDs tests getting all transaction IDs
func TestTransactionRegistry_GetAllTransactionIDs(t *testing.T) {
	wal, _ := createTestWAL(t)
	defer wal.Close()

	registry := NewTransactionRegistry(wal)

	expectedCount := 5
	createdIDs := make(map[*primitives.TransactionID]bool)

	for i := 0; i < expectedCount; i++ {
		ctx, _ := registry.Begin()
		createdIDs[ctx.ID] = true
	}

	allIDs := registry.GetAllTransactionIDs()

	if len(allIDs) != expectedCount {
		t.Errorf("Expected %d transaction IDs, got %d", expectedCount, len(allIDs))
	}

	// Verify all IDs are present
	for _, tid := range allIDs {
		if !createdIDs[tid] {
			t.Error("GetAllTransactionIDs returned unexpected ID")
		}
	}
}

// TestTransactionRegistry_Concurrency tests concurrent registry operations
func TestTransactionRegistry_Concurrency(t *testing.T) {
	wal, _ := createTestWAL(t)
	defer wal.Close()

	registry := NewTransactionRegistry(wal)

	const goroutines = 20
	const transactionsPerGoroutine = 10

	var wg sync.WaitGroup
	wg.Add(goroutines)

	// Concurrent transaction creation and operations
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < transactionsPerGoroutine; j++ {
				ctx, err := registry.Begin()
				if err != nil {
					t.Errorf("Failed to begin transaction: %v", err)
					continue
				}

				// Perform some operations
				pid := &mockPageID{tableID: 1, pageNo: j}
				ctx.RecordPageAccess(pid, ReadWrite)
				ctx.MarkPageDirty(pid)

				// Sometimes commit, sometimes abort, sometimes leave active
				switch j % 3 {
				case 0:
					ctx.SetStatus(TxCommitted)
					registry.Remove(ctx.ID)
				case 1:
					ctx.SetStatus(TxAborted)
					registry.Remove(ctx.ID)
				}
			}
		}()
	}

	wg.Wait()

	// Verify registry is in consistent state
	count := registry.Count()
	allIDs := registry.GetAllTransactionIDs()
	if len(allIDs) != count {
		t.Errorf("Count mismatch: Count()=%d, len(GetAllTransactionIDs())=%d", count, len(allIDs))
	}

	active := registry.GetActive()
	for _, ctx := range active {
		if !ctx.IsActive() {
			t.Error("GetActive returned non-active transaction after concurrent operations")
		}
	}
}

// TestTransactionRegistry_MultipleBeginCommitRemove tests the full lifecycle
func TestTransactionRegistry_MultipleBeginCommitRemove(t *testing.T) {
	wal, _ := createTestWAL(t)
	defer wal.Close()

	registry := NewTransactionRegistry(wal)

	transactions := make([]*TransactionContext, 0, 10)

	// Begin multiple transactions
	for i := 0; i < 10; i++ {
		ctx, err := registry.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction %d: %v", i, err)
		}
		transactions = append(transactions, ctx)
	}

	if registry.Count() != 10 {
		t.Errorf("Expected 10 transactions, got %d", registry.Count())
	}

	// Commit half of them
	for i := 0; i < 5; i++ {
		transactions[i].SetStatus(TxCommitted)
		registry.Remove(transactions[i].ID)
	}

	if registry.Count() != 5 {
		t.Errorf("Expected 5 transactions after commits, got %d", registry.Count())
	}

	// Abort the rest
	for i := 5; i < 10; i++ {
		transactions[i].SetStatus(TxAborted)
		registry.Remove(transactions[i].ID)
	}

	if registry.Count() != 0 {
		t.Errorf("Expected 0 transactions after all removed, got %d", registry.Count())
	}

	active := registry.GetActive()
	if len(active) != 0 {
		t.Errorf("Expected no active transactions, got %d", len(active))
	}
}

// TestTransactionContext_DirtyPagesIndependence tests that GetDirtyPages returns a copy
func TestTransactionContext_DirtyPagesIndependence(t *testing.T) {
	tid := primitives.NewTransactionID()
	ctx := NewTransactionContext(tid)

	pid1 := &mockPageID{tableID: 1, pageNo: 1}
	ctx.MarkPageDirty(pid1)

	dirtyPages1 := ctx.GetDirtyPages()
	dirtyPages2 := ctx.GetDirtyPages()

	// Verify they are different slices
	if len(dirtyPages1) != len(dirtyPages2) {
		t.Error("Expected same length for dirty pages copies")
	}

	// Add another dirty page
	pid2 := &mockPageID{tableID: 1, pageNo: 2}
	ctx.MarkPageDirty(pid2)

	dirtyPages3 := ctx.GetDirtyPages()
	if len(dirtyPages3) != 2 {
		t.Errorf("Expected 2 dirty pages, got %d", len(dirtyPages3))
	}

	// Original slices should be unchanged
	if len(dirtyPages1) != 1 {
		t.Error("Original dirty pages slice was modified")
	}
}

// TestTransactionContext_LockedPagesIndependence tests that GetLockedPages returns a copy
func TestTransactionContext_LockedPagesIndependence(t *testing.T) {
	tid := primitives.NewTransactionID()
	ctx := NewTransactionContext(tid)

	pid1 := &mockPageID{tableID: 1, pageNo: 1}
	ctx.RecordPageAccess(pid1, ReadOnly)

	lockedPages1 := ctx.GetLockedPages()

	// Add another locked page
	pid2 := &mockPageID{tableID: 1, pageNo: 2}
	ctx.RecordPageAccess(pid2, ReadWrite)

	lockedPages2 := ctx.GetLockedPages()

	if len(lockedPages2) != 2 {
		t.Errorf("Expected 2 locked pages, got %d", len(lockedPages2))
	}

	// Original slice should be unchanged
	if len(lockedPages1) != 1 {
		t.Error("Original locked pages slice was modified")
	}
}

// TestTransactionContext_WaitingForIndependence tests that GetWaitingFor returns a copy
func TestTransactionContext_WaitingForIndependence(t *testing.T) {
	tid := primitives.NewTransactionID()
	ctx := NewTransactionContext(tid)

	pid1 := &mockPageID{tableID: 1, pageNo: 1}
	ctx.AddWaitingFor(pid1)

	waiting1 := ctx.GetWaitingFor()

	// Add another waiting page
	pid2 := &mockPageID{tableID: 1, pageNo: 2}
	ctx.AddWaitingFor(pid2)

	waiting2 := ctx.GetWaitingFor()

	if len(waiting2) != 2 {
		t.Errorf("Expected 2 waiting pages, got %d", len(waiting2))
	}

	// Original slice should be unchanged
	if len(waiting1) != 1 {
		t.Error("Original waiting pages slice was modified")
	}
}
