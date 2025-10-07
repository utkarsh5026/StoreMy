package transaction

import (
	"fmt"
	"maps"
	"slices"
	"storemy/pkg/log"
	"storemy/pkg/primitives"
	"sync"
	"time"
)

// TransactionStatus represents the current state of a transaction
type TransactionStatus int

const (
	TxActive TransactionStatus = iota
	TxCommitting
	TxAborting
	TxCommitted
	TxAborted
)

// Permissions represents the access level for database operations
type Permissions int

const (
	ReadOnly Permissions = iota
	ReadWrite
)

func (ts TransactionStatus) String() string {
	switch ts {
	case TxActive:
		return "ACTIVE"
	case TxCommitting:
		return "COMMITTING"
	case TxAborting:
		return "ABORTING"
	case TxCommitted:
		return "COMMITTED"
	case TxAborted:
		return "ABORTED"
	default:
		return "UNKNOWN"
	}
}

type TransactionStats struct {
	PagesRead     int
	PagesWritten  int
	TuplesRead    int
	TuplesWritten int
	TuplesDeleted int
	LockedPages   int
	DirtyPages    int
}

// TransactionContext encapsulates all state for a single transaction.
// This is the single source of truth for everything a transaction has done.
type TransactionContext struct {
	// Identity
	ID *primitives.TransactionID

	// Lifecycle state
	status    TransactionStatus
	startTime time.Time
	endTime   time.Time
	mutex     sync.RWMutex

	// Page access tracking
	// Maps PageID to the permission level requested (ReadOnly or ReadWrite)
	lockedPages map[primitives.PageID]Permissions
	// Set of pages this transaction has modified
	dirtyPages map[primitives.PageID]bool

	// Write-Ahead Logging state
	// First log record written by this transaction
	firstLSN primitives.LSN
	// Most recent log record written by this transaction
	lastLSN primitives.LSN
	// For rollback: next LSN to undo (used during abort)
	undoNextLSN primitives.LSN
	// Has a BEGIN record been written to WAL?
	begunInWAL bool

	// Deadlock detection
	// Pages this transaction is currently waiting to acquire
	waitingFor []primitives.PageID

	// Statistics
	pagesRead     int
	pagesWritten  int
	tuplesRead    int
	tuplesWritten int
	tuplesDeleted int
}

func NewTransactionContext(tid *primitives.TransactionID) *TransactionContext {
	return &TransactionContext{
		ID:          tid,
		status:      TxActive,
		startTime:   time.Now(),
		lockedPages: make(map[primitives.PageID]Permissions),
		dirtyPages:  make(map[primitives.PageID]bool),
		waitingFor:  make([]primitives.PageID, 0),
		firstLSN:    0,
		lastLSN:     0,
		undoNextLSN: 0,
		begunInWAL:  false,
	}
}

// IsActive returns true if the transaction is still active
func (tc *TransactionContext) IsActive() bool {
	tc.mutex.RLock()
	defer tc.mutex.RUnlock()
	return tc.status == TxActive
}

func (tc *TransactionContext) GetStatus() TransactionStatus {
	tc.mutex.RLock()
	defer tc.mutex.RUnlock()
	return tc.status
}

// SetStatus updates the transaction status
func (tc *TransactionContext) SetStatus(status TransactionStatus) {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()
	tc.status = status
	if status == TxCommitted || status == TxAborted {
		tc.endTime = time.Now()
	}
}

// RecordPageAccess records that this transaction has accessed a page
func (tc *TransactionContext) RecordPageAccess(pid primitives.PageID, perm Permissions) {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	if existing, exists := tc.lockedPages[pid]; exists {
		if existing == ReadWrite {
			return
		}
	}

	tc.lockedPages[pid] = perm
	if perm == ReadOnly {
		tc.pagesRead++
	}
}

// MarkPageDirty marks a page as dirty (modified) by this transaction
func (tc *TransactionContext) MarkPageDirty(pid primitives.PageID) {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	if !tc.dirtyPages[pid] {
		tc.dirtyPages[pid] = true
		tc.pagesWritten++
	}
}

// GetDirtyPages returns a copy of all dirty pages
func (tc *TransactionContext) GetDirtyPages() []primitives.PageID {
	tc.mutex.RLock()
	defer tc.mutex.RUnlock()
	return slices.Collect(maps.Keys(tc.dirtyPages))
}

// GetLockedPages returns a copy of all locked pages
func (tc *TransactionContext) GetLockedPages() []primitives.PageID {
	tc.mutex.RLock()
	defer tc.mutex.RUnlock()
	return slices.Collect(maps.Keys(tc.lockedPages))
}

// AddWaitingFor records that this transaction is waiting for a page
func (tc *TransactionContext) AddWaitingFor(pid primitives.PageID) {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()
	tc.waitingFor = append(tc.waitingFor, pid)
}

// RemoveWaitingFor removes a page from the waiting list
func (tc *TransactionContext) RemoveWaitingFor(pid primitives.PageID) {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	filtered := make([]primitives.PageID, 0, len(tc.waitingFor))
	for _, p := range tc.waitingFor {
		if !p.Equals(pid) {
			filtered = append(filtered, p)
		}
	}
	tc.waitingFor = filtered
}

func (tc *TransactionContext) GetWaitingFor() []primitives.PageID {
	tc.mutex.RLock()
	defer tc.mutex.RUnlock()

	waiting := make([]primitives.PageID, len(tc.waitingFor))
	copy(waiting, tc.waitingFor)
	return waiting
}

// EnsureBegunInWAL ensures a BEGIN record has been written
func (tc *TransactionContext) EnsureBegunInWAL(wal *log.WAL) error {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	if tc.begunInWAL {
		return nil
	}

	lsn, err := wal.LogBegin(tc.ID)
	if err != nil {
		return err
	}

	tc.firstLSN = lsn
	tc.lastLSN = lsn
	tc.begunInWAL = true
	return nil
}

// UpdateLSN updates the LSN chain for this transaction
func (tc *TransactionContext) UpdateLSN(lsn primitives.LSN) {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	if tc.firstLSN == 0 {
		tc.firstLSN = lsn
	}
	tc.lastLSN = lsn
}

// GetLastLSN returns the last LSN for this transaction
func (tc *TransactionContext) GetLastLSN() primitives.LSN {
	tc.mutex.RLock()
	defer tc.mutex.RUnlock()
	return tc.lastLSN
}

// GetFirstLSN returns the first LSN for this transaction
func (tc *TransactionContext) GetFirstLSN() primitives.LSN {
	tc.mutex.RLock()
	defer tc.mutex.RUnlock()
	return tc.firstLSN
}

// Statistics methods

// RecordTupleRead increments the tuples read counter
func (tc *TransactionContext) RecordTupleRead() {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()
	tc.tuplesRead++
}

// RecordTupleWrite increments the tuples written counter
func (tc *TransactionContext) RecordTupleWrite() {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()
	tc.tuplesWritten++
}

// RecordTupleDelete increments the tuples deleted counter
func (tc *TransactionContext) RecordTupleDelete() {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()
	tc.tuplesDeleted++
}

func (tc *TransactionContext) GetPagePermission(pageId primitives.PageID) (perm Permissions, exists bool) {
	tc.mutex.RLock()
	defer tc.mutex.RUnlock()
	perm, exists = tc.lockedPages[pageId]
	return
}

// GetStatistics returns a snapshot of transaction statistics
func (tc *TransactionContext) GetStatistics() TransactionStats {
	tc.mutex.RLock()
	defer tc.mutex.RUnlock()

	return TransactionStats{
		PagesRead:     tc.pagesRead,
		PagesWritten:  tc.pagesWritten,
		TuplesRead:    tc.tuplesRead,
		TuplesWritten: tc.tuplesWritten,
		TuplesDeleted: tc.tuplesDeleted,
		LockedPages:   len(tc.lockedPages),
		DirtyPages:    len(tc.dirtyPages),
	}
}

// Duration returns how long the transaction has been running
func (tc *TransactionContext) Duration() time.Duration {
	tc.mutex.RLock()
	defer tc.mutex.RUnlock()

	endTime := tc.endTime
	if endTime.IsZero() {
		endTime = time.Now()
	}
	return endTime.Sub(tc.startTime)
}

// String returns a string representation of the transaction context
func (tc *TransactionContext) String() string {
	tc.mutex.RLock()
	defer tc.mutex.RUnlock()

	return fmt.Sprintf("Transaction %s [Status=%s, Duration=%v, Dirty=%d, Locked=%d]",
		tc.ID.String(), tc.status.String(), tc.Duration(),
		len(tc.dirtyPages), len(tc.lockedPages))
}
