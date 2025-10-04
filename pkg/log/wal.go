package log

import (
	"fmt"
	"io"
	"maps"
	"os"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/tuple"
	"sync"
)

const (
	FirstLSN LSN = 0
)

// WAL manages the write-ahead log
type WAL struct {
	file       *os.File
	activeTxns map[*transaction.TransactionID]*TransactionLogInfo
	dirtyPages map[tuple.PageID]LSN
	mutex      sync.RWMutex
	flushCond  *sync.Cond
	writer     *LogWriter
}

// NewWAL creates a new WAL instance
func NewWAL(logPath string, bufferSize int) (*WAL, error) {
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %v", err)
	}

	pos, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to seek to end of WAL: %v", err)
	}

	writer := NewLogWriter(file, bufferSize, LSN(pos), LSN(pos))

	w := &WAL{
		file:       file,
		writer:     writer,
		activeTxns: make(map[*transaction.TransactionID]*TransactionLogInfo),
		dirtyPages: make(map[tuple.PageID]LSN),
	}

	w.flushCond = sync.NewCond(&w.mutex)
	return w, nil
}

func (w *WAL) LogBegin(tid *transaction.TransactionID) (LSN, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	record := NewLogRecord(BeginRecord, tid, nil, nil, nil, FirstLSN)
	lsn, err := w.writeRecord(record)
	if err != nil {
		return 0, err
	}

	w.activeTxns[tid] = &TransactionLogInfo{
		FirstLSN: lsn,
		LastLSN:  lsn,
	}
	return lsn, nil
}

// CRITICAL: This must FORCE the log to disk before returning (durability guarantee)
// After this returns successfully, the transaction is durable even if system crashes
func (w *WAL) LogCommit(tid *transaction.TransactionID) (LSN, error) {
	w.mutex.Lock()

	txnInfo, err := w.getTransactionInfo(tid)
	if err != nil {
		w.mutex.Unlock()
		return 0, err
	}

	record := NewLogRecord(CommitRecord, tid, nil, nil, nil, txnInfo.LastLSN)

	lsn, err := w.writeRecord(record)
	if err != nil {
		w.mutex.Unlock()
		return 0, err
	}

	txnInfo.LastLSN = lsn
	w.mutex.Unlock()

	if err := w.Force(lsn); err != nil {
		return 0, fmt.Errorf("failed to force commit record to disk: %v", err)
	}

	w.mutex.Lock()
	delete(w.activeTxns, tid)
	w.mutex.Unlock()
	return lsn, nil
}

// LogAbort logs a transaction abort
// This initiates the undo process for the transaction
func (w *WAL) LogAbort(tid *transaction.TransactionID) (LSN, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	txnInfo, err := w.getTransactionInfo(tid)
	if err != nil {
		return 0, err
	}

	record := NewLogRecord(AbortRecord, tid, nil, nil, nil, txnInfo.LastLSN)
	lsn, err := w.writeRecord(record)
	if err != nil {
		return 0, err
	}

	txnInfo.LastLSN = lsn
	txnInfo.UndoNextLSN = txnInfo.LastLSN
	return lsn, nil
}

// Close closes the WAL gracefully
// Flushes any remaining buffered data and closes the file
func (w *WAL) Close() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if err := w.writer.Close(); err != nil {
		return fmt.Errorf("failed to close WAL writer: %v", err)
	}

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %v", err)
	}

	return nil
}

// LogUpdate logs a page update with before and after images
// This is called BEFORE the page is actually modified in memory
func (w *WAL) LogUpdate(tid *transaction.TransactionID, pageID tuple.PageID, beforeImage, afterImage []byte) (LSN, error) {
	return w.logDataOperation(UpdateRecord, tid, pageID, beforeImage, afterImage)
}

// LogInsert logs a tuple insertion
// Only needs after image - there's nothing to undo to (tuple didn't exist)
// During recovery, we REDO the insert by applying the after image
func (w *WAL) LogInsert(tid *transaction.TransactionID, pageID tuple.PageID, afterImage []byte) (LSN, error) {
	return w.logDataOperation(InsertRecord, tid, pageID, nil, afterImage)
}

// LogDelete logs a tuple deletion
// Only needs before image - this is what we restore during UNDO
// During recovery, REDO means "ensure tuple is deleted" (no-op if already gone)
func (w *WAL) LogDelete(tid *transaction.TransactionID, pageID tuple.PageID, beforeImage []byte) (LSN, error) {
	return w.logDataOperation(DeleteRecord, tid, pageID, beforeImage, nil)
}

// GetDirtyPages returns a copy of the dirty page table
// Used during checkpointing
func (w *WAL) GetDirtyPages() map[tuple.PageID]LSN {
	w.mutex.RLock()
	defer w.mutex.RUnlock()

	pages := make(map[tuple.PageID]LSN, len(w.dirtyPages))
	maps.Copy(pages, w.dirtyPages)
	return pages
}

// GetActiveTransactions returns a list of active transaction IDs
// Used during checkpointing
func (w *WAL) GetActiveTransactions() []*transaction.TransactionID {
	w.mutex.RLock()
	defer w.mutex.RUnlock()

	txns := make([]*transaction.TransactionID, 0, len(w.activeTxns))
	for tid := range w.activeTxns {
		txns = append(txns, tid)
	}
	return txns
}

// GetLastLSN returns the last LSN for a transaction
// Used for building PrevLSN chains
func (w *WAL) GetLastLSN(tid *transaction.TransactionID) (LSN, error) {
	w.mutex.RLock()
	defer w.mutex.RUnlock()

	txnInfo, exists := w.activeTxns[tid]
	if !exists {
		return 0, fmt.Errorf("transaction %v not found", tid)
	}

	return txnInfo.LastLSN, nil
}

func (w *WAL) writeRecord(record *LogRecord) (LSN, error) {
	data, err := SerializeLogRecord(record)
	if err != nil {
		return 0, err
	}

	return w.writer.Write(data)
}

// Force ensures all log records up to the given LSN are on disk
// This is called during commit to ensure durability
func (w *WAL) Force(lsn LSN) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	return w.writer.Force(lsn)
}

func (w *WAL) getTransactionInfo(tid *transaction.TransactionID) (*TransactionLogInfo, error) {
	txnInfo, exists := w.activeTxns[tid]
	if !exists {
		return nil, fmt.Errorf("transaction %v not found in active transactions", tid)
	}
	return txnInfo, nil
}

// logDataOperation is a helper for logging data operations (insert, update, delete)
func (w *WAL) logDataOperation(recordType LogRecordType, tid *transaction.TransactionID, pageID tuple.PageID, beforeImage, afterImage []byte) (LSN, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	txnInfo, err := w.getTransactionInfo(tid)
	if err != nil {
		return FirstLSN, err
	}

	record := NewLogRecord(recordType, tid, pageID, beforeImage, afterImage, txnInfo.LastLSN)

	lsn, err := w.writeRecord(record)
	if err != nil {
		return 0, err
	}

	txnInfo.LastLSN = lsn

	if _, exists := w.dirtyPages[pageID]; !exists {
		w.dirtyPages[pageID] = lsn
	}

	return lsn, nil
}
