package wal

import (
	"fmt"
	"io"
	"maps"
	"os"
	"storemy/pkg/log/record"
	"storemy/pkg/primitives"
	"sync"
)

const (
	FirstLSN primitives.LSN = 0
)

// WAL manages the write-ahead log
type WAL struct {
	file       *os.File
	activeTxns map[*primitives.TransactionID]*record.TransactionLogInfo
	dirtyPages map[primitives.PageID]primitives.LSN
	mutex      sync.RWMutex
	flushCond  *sync.Cond
	writer     *LogWriter
}

// NewWAL creates a new WAL instance
func NewWAL(logPath string, bufferSize int) (*WAL, error) {
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %v", err)
	}

	pos, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to seek to end of WAL: %v", err)
	}

	writer := NewLogWriter(file, bufferSize, primitives.LSN(pos), primitives.LSN(pos))

	w := &WAL{
		file:       file,
		writer:     writer,
		activeTxns: make(map[*primitives.TransactionID]*record.TransactionLogInfo),
		dirtyPages: make(map[primitives.PageID]primitives.LSN),
	}

	w.flushCond = sync.NewCond(&w.mutex)
	return w, nil
}

func (w *WAL) LogBegin(tid *primitives.TransactionID) (primitives.LSN, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	lsn, err := w.logTransactionOperation(record.BeginRecord, tid, FirstLSN)
	if err != nil {
		return 0, err
	}

	w.activeTxns[tid] = &record.TransactionLogInfo{
		FirstLSN: lsn,
		LastLSN:  lsn,
	}
	return lsn, nil
}

// CRITICAL: This must FORCE the log to disk before returning (durability guarantee)
// After this returns successfully, the transaction is durable even if system crashes
func (w *WAL) LogCommit(tid *primitives.TransactionID) (primitives.LSN, error) {
	w.mutex.Lock()

	txnInfo, err := w.getTransactionInfo(tid)
	if err != nil {
		w.mutex.Unlock()
		return 0, err
	}

	lsn, err := w.logTransactionOperation(record.CommitRecord, tid, txnInfo.LastLSN)
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
func (w *WAL) LogAbort(tid *primitives.TransactionID) (primitives.LSN, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	txnInfo, err := w.getTransactionInfo(tid)
	if err != nil {
		return 0, err
	}

	lsn, err := w.logTransactionOperation(record.AbortRecord, tid, txnInfo.LastLSN)
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
func (w *WAL) LogUpdate(tid *primitives.TransactionID, pageID primitives.PageID, beforeImage, afterImage []byte) (primitives.LSN, error) {
	return w.logDataOperation(record.UpdateRecord, tid, pageID, beforeImage, afterImage)
}

// LogInsert logs a tuple insertion
// Only needs after image - there's nothing to undo to (tuple didn't exist)
// During recovery, we REDO the insert by applying the after image
func (w *WAL) LogInsert(tid *primitives.TransactionID, pageID primitives.PageID, afterImage []byte) (primitives.LSN, error) {
	return w.logDataOperation(record.InsertRecord, tid, pageID, nil, afterImage)
}

// LogDelete logs a tuple deletion
// Only needs before image - this is what we restore during UNDO
// During recovery, REDO means "ensure tuple is deleted" (no-op if already gone)
func (w *WAL) LogDelete(tid *primitives.TransactionID, pageID primitives.PageID, beforeImage []byte) (primitives.LSN, error) {
	return w.logDataOperation(record.DeleteRecord, tid, pageID, beforeImage, nil)
}

// GetDirtyPages returns a copy of the dirty page table
// Used during checkpointing
func (w *WAL) GetDirtyPages() map[primitives.PageID]primitives.LSN {
	w.mutex.RLock()
	defer w.mutex.RUnlock()

	pages := make(map[primitives.PageID]primitives.LSN, len(w.dirtyPages))
	maps.Copy(pages, w.dirtyPages)
	return pages
}

// GetActiveTransactions returns a list of active transaction IDs
// Used during checkpointing
func (w *WAL) GetActiveTransactions() []*primitives.TransactionID {
	w.mutex.RLock()
	defer w.mutex.RUnlock()

	txns := make([]*primitives.TransactionID, 0, len(w.activeTxns))
	for tid := range w.activeTxns {
		txns = append(txns, tid)
	}
	return txns
}

// GetLastLSN returns the last primitives.LSN for a transaction
// Used for building PrevLSN chains
func (w *WAL) GetLastLSN(tid *primitives.TransactionID) (primitives.LSN, error) {
	w.mutex.RLock()
	defer w.mutex.RUnlock()

	txnInfo, exists := w.activeTxns[tid]
	if !exists {
		return 0, fmt.Errorf("transaction %v not found", tid)
	}

	return txnInfo.LastLSN, nil
}

func (w *WAL) writeRecord(rec *record.LogRecord) (primitives.LSN, error) {
	data, err := record.SerializeLogRecord(rec)
	if err != nil {
		return 0, err
	}

	return w.writer.Write(data)
}

// Force ensures all log records up to the given primitives.LSN are on disk
// This is called during commit to ensure durability
func (w *WAL) Force(lsn primitives.LSN) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	return w.writer.Force(lsn)
}

func (w *WAL) getTransactionInfo(tid *primitives.TransactionID) (*record.TransactionLogInfo, error) {
	txnInfo, exists := w.activeTxns[tid]
	if !exists {
		return nil, fmt.Errorf("transaction %v not found in active transactions", tid)
	}
	return txnInfo, nil
}

// logDataOperation is a helper for logging data operations (insert, update, delete)
func (w *WAL) logDataOperation(recordType record.LogRecordType, tid *primitives.TransactionID, pageID primitives.PageID, beforeImage, afterImage []byte) (primitives.LSN, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	txnInfo, err := w.getTransactionInfo(tid)
	if err != nil {
		return FirstLSN, err
	}

	rec := record.NewLogRecord(recordType, tid, pageID, beforeImage, afterImage, txnInfo.LastLSN)

	lsn, err := w.writeRecord(rec)
	if err != nil {
		return 0, err
	}

	txnInfo.LastLSN = lsn

	if _, exists := w.dirtyPages[pageID]; !exists {
		w.dirtyPages[pageID] = lsn
	}

	return lsn, nil
}

func (w *WAL) logTransactionOperation(recordType record.LogRecordType, tid *primitives.TransactionID, prevLSN primitives.LSN) (primitives.LSN, error) {
	rec := record.NewLogRecord(recordType, tid, nil, nil, nil, prevLSN)
	return w.writeRecord(rec)
}
