package transaction

import (
	"fmt"
	"maps"
	"slices"
	"storemy/pkg/log"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
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
	lockedPages map[tuple.PageID]Permissions
	// Set of pages this transaction has modified
	dirtyPages map[tuple.PageID]bool

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
	waitingFor []tuple.PageID

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
		lockedPages: make(map[tuple.PageID]Permissions),
		dirtyPages:  make(map[tuple.PageID]bool),
		waitingFor:  make([]tuple.PageID, 0),
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
func (tc *TransactionContext) RecordPageAccess(pid tuple.PageID, perm Permissions) {
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
func (tc *TransactionContext) MarkPageDirty(pid tuple.PageID) {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	if !tc.dirtyPages[pid] {
		tc.dirtyPages[pid] = true
		tc.pagesWritten++
	}
}

// GetDirtyPages returns a copy of all dirty pages
func (tc *TransactionContext) GetDirtyPages() []tuple.PageID {
	tc.mutex.RLock()
	defer tc.mutex.RUnlock()
	return slices.Collect(maps.Keys(tc.dirtyPages))
}

// GetLockedPages returns a copy of all locked pages
func (tc *TransactionContext) GetLockedPages() []tuple.PageID {
	tc.mutex.RLock()
	defer tc.mutex.RUnlock()
	return slices.Collect(maps.Keys(tc.lockedPages))
}

// AddWaitingFor records that this transaction is waiting for a page
func (tc *TransactionContext) AddWaitingFor(pid tuple.PageID) {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()
	tc.waitingFor = append(tc.waitingFor, pid)
}

// RemoveWaitingFor removes a page from the waiting list
func (tc *TransactionContext) RemoveWaitingFor(pid tuple.PageID) {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	filtered := make([]tuple.PageID, 0, len(tc.waitingFor))
	for _, p := range tc.waitingFor {
		if !p.Equals(pid) {
			filtered = append(filtered, p)
		}
	}
	tc.waitingFor = filtered
}

func (tc *TransactionContext) GetWaitingFor() []tuple.PageID {
	tc.mutex.RLock()
	defer tc.mutex.RUnlock()

	waiting := make([]tuple.PageID, len(tc.waitingFor))
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

// GetStatistics returns a snapshot of transaction statistics
func (tc *TransactionContext) GetStatistics() map[string]int {
	tc.mutex.RLock()
	defer tc.mutex.RUnlock()

	return map[string]int{
		"pages_read":     tc.pagesRead,
		"pages_written":  tc.pagesWritten,
		"tuples_read":    tc.tuplesRead,
		"tuples_written": tc.tuplesWritten,
		"tuples_deleted": tc.tuplesDeleted,
		"locked_pages":   len(tc.lockedPages),
		"dirty_pages":    len(tc.dirtyPages),
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

// TransactionRegistry manages all active transaction contexts
// This is the single global registry that replaces scattered transaction maps
type TransactionRegistry struct {
	contexts map[*primitives.TransactionID]*TransactionContext
	mutex    sync.RWMutex
	wal      *log.WAL
}

// NewTransactionRegistry creates a new transaction registry
func NewTransactionRegistry(wal *log.WAL) *TransactionRegistry {
	return &TransactionRegistry{
		contexts: make(map[*primitives.TransactionID]*TransactionContext),
		wal:      wal,
	}
}

// Begin creates a new transaction context and registers it
func (tr *TransactionRegistry) Begin() (*TransactionContext, error) {
	tid := primitives.NewTransactionID()
	ctx := NewTransactionContext(tid)

	tr.mutex.Lock()
	tr.contexts[tid] = ctx
	tr.mutex.Unlock()

	return ctx, nil
}

// Get retrieves a transaction context by ID
func (tr *TransactionRegistry) Get(tid *primitives.TransactionID) (*TransactionContext, error) {
	tr.mutex.RLock()
	defer tr.mutex.RUnlock()

	ctx, exists := tr.contexts[tid]
	if !exists {
		return nil, fmt.Errorf("transaction %s not found", tid.String())
	}
	return ctx, nil
}

// GetOrCreate gets an existing context or creates a new one
func (tr *TransactionRegistry) GetOrCreate(tid *primitives.TransactionID) *TransactionContext {
	tr.mutex.Lock()
	defer tr.mutex.Unlock()

	ctx, exists := tr.contexts[tid]
	if exists {
		return ctx
	}

	ctx = NewTransactionContext(tid)
	tr.contexts[tid] = ctx
	return ctx
}

// Remove removes a transaction context from the registry
func (tr *TransactionRegistry) Remove(tid *primitives.TransactionID) {
	tr.mutex.Lock()
	defer tr.mutex.Unlock()
	delete(tr.contexts, tid)
}

// GetActive returns all active transaction contexts
func (tr *TransactionRegistry) GetActive() []*TransactionContext {
	tr.mutex.RLock()
	defer tr.mutex.RUnlock()

	active := make([]*TransactionContext, 0)
	for _, ctx := range tr.contexts {
		if ctx.IsActive() {
			active = append(active, ctx)
		}
	}
	return active
}

// Count returns the number of registered transactions
func (tr *TransactionRegistry) Count() int {
	tr.mutex.RLock()
	defer tr.mutex.RUnlock()
	return len(tr.contexts)
}

// GetAllTransactionIDs returns all registered transaction IDs
func (tr *TransactionRegistry) GetAllTransactionIDs() []*primitives.TransactionID {
	tr.mutex.RLock()
	defer tr.mutex.RUnlock()

	tids := make([]*primitives.TransactionID, 0, len(tr.contexts))
	for tid := range tr.contexts {
		tids = append(tids, tid)
	}
	return tids
}
