package main

import (
	"fmt"
	"os"
	"storemy/pkg/log"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"time"
)

const ExampleLogFile = "examples_wal.log"

// This package demonstrates how Write-Ahead Logging (WAL) works in database transactions
// by creating REAL log records that you can visualize with the logreader tool.

// Example1_SimpleInsert demonstrates a basic INSERT operation and writes real log records
func Example1_SimpleInsert(writer *log.LogWriter) error {
	fmt.Println("=== Example 1: Simple INSERT Transaction ===\n")
	fmt.Println("Scenario: User inserts a new record into a table")
	fmt.Println("SQL: INSERT INTO users (id, name) VALUES (1, 'Alice')\n")

	tid := primitives.NewTransactionID()
	pageID := heap.NewHeapPageID(1, 0) // Table 1, Page 0

	// Step 1: BEGIN transaction
	beginRecord := log.NewLogRecord(log.BeginRecord, tid, primitives.PageID(nil), nil, nil, 0)
	beginRecord.LSN = writer.CurrentLSN()

	serialized, err := log.SerializeLogRecord(beginRecord)
	if err != nil {
		return fmt.Errorf("failed to serialize BEGIN: %w", err)
	}

	lsn, err := writer.Write(serialized)
	if err != nil {
		return fmt.Errorf("failed to write BEGIN: %w", err)
	}

	fmt.Printf("1. BEGIN Transaction\n")
	fmt.Printf("   LSN: %d | TID: %s\n\n", lsn, tid.String())

	// Step 2: INSERT record
	afterImage := []byte("id=1, name=Alice") // New data
	insertRecord := log.NewLogRecord(log.InsertRecord, tid, pageID, nil, afterImage, lsn)
	insertRecord.LSN = writer.CurrentLSN()

	serialized, err = log.SerializeLogRecord(insertRecord)
	if err != nil {
		return fmt.Errorf("failed to serialize INSERT: %w", err)
	}

	insertLSN, err := writer.Write(serialized)
	if err != nil {
		return fmt.Errorf("failed to write INSERT: %w", err)
	}

	fmt.Printf("2. INSERT Record\n")
	fmt.Printf("   LSN: %d | TID: %s\n", insertLSN, tid.String())
	fmt.Printf("   Page: Table=%d, Page=%d\n", pageID.GetTableID(), pageID.PageNo())
	fmt.Printf("   After Image: %s\n\n", string(afterImage))

	// Step 3: COMMIT transaction
	commitRecord := log.NewLogRecord(log.CommitRecord, tid, primitives.PageID(nil), nil, nil, insertLSN)
	commitRecord.LSN = writer.CurrentLSN()

	serialized, err = log.SerializeLogRecord(commitRecord)
	if err != nil {
		return fmt.Errorf("failed to serialize COMMIT: %w", err)
	}

	commitLSN, err := writer.Write(serialized)
	if err != nil {
		return fmt.Errorf("failed to write COMMIT: %w", err)
	}

	// Force to disk (durability!)
	if err := writer.Force(commitLSN); err != nil {
		return fmt.Errorf("failed to force COMMIT: %w", err)
	}

	fmt.Printf("3. COMMIT Transaction\n")
	fmt.Printf("   LSN: %d | TID: %s\n\n", commitLSN, tid.String())

	fmt.Println("✓ Transaction committed successfully!")
	fmt.Println("✓ Log records written to disk for crash recovery.\n")
	return nil
}

// Example2_UpdateOperation demonstrates an UPDATE operation with before/after images
func Example2_UpdateOperation(writer *log.LogWriter) error {
	fmt.Println("=== Example 2: UPDATE Transaction ===\n")
	fmt.Println("Scenario: User updates an existing record")
	fmt.Println("SQL: UPDATE users SET name='Alice Smith' WHERE id=1\n")

	tid := primitives.NewTransactionID()
	pageID := heap.NewHeapPageID(1, 0)

	// Step 1: BEGIN
	beginRecord := log.NewLogRecord(log.BeginRecord, tid, primitives.PageID(nil), nil, nil, 0)
	beginRecord.LSN = writer.CurrentLSN()
	serialized, _ := log.SerializeLogRecord(beginRecord)
	beginLSN, _ := writer.Write(serialized)

	fmt.Printf("1. BEGIN Transaction (LSN: %d, TID: %s)\n\n", beginLSN, tid.String())

	// Step 2: UPDATE record
	beforeImage := []byte("id=1, name=Alice")       // Old data
	afterImage := []byte("id=1, name=Alice Smith")  // New data

	updateRecord := log.NewLogRecord(log.UpdateRecord, tid, pageID, beforeImage, afterImage, beginLSN)
	updateRecord.LSN = writer.CurrentLSN()
	serialized, _ = log.SerializeLogRecord(updateRecord)
	updateLSN, _ := writer.Write(serialized)

	fmt.Printf("2. UPDATE Record\n")
	fmt.Printf("   LSN: %d | TID: %s\n", updateLSN, tid.String())
	fmt.Printf("   Page: Table=%d, Page=%d\n", pageID.GetTableID(), pageID.PageNo())
	fmt.Printf("   Before Image: %s (for UNDO)\n", string(beforeImage))
	fmt.Printf("   After Image:  %s (for REDO)\n\n", string(afterImage))

	// Step 3: COMMIT
	commitRecord := log.NewLogRecord(log.CommitRecord, tid, primitives.PageID(nil), nil, nil, updateLSN)
	commitRecord.LSN = writer.CurrentLSN()
	serialized, _ = log.SerializeLogRecord(commitRecord)
	commitLSN, _ := writer.Write(serialized)
	writer.Force(commitLSN)

	fmt.Printf("3. COMMIT Transaction (LSN: %d, TID: %s)\n\n", commitLSN, tid.String())

	fmt.Println("Key Points:")
	fmt.Println("- Before Image: Used for UNDO (rollback)")
	fmt.Println("- After Image: Used for REDO (crash recovery)")
	fmt.Println("- Both images ensure transaction atomicity!\n")
	return nil
}

// Example3_AbortedTransaction demonstrates a transaction rollback with CLR records
func Example3_AbortedTransaction(writer *log.LogWriter) error {
	fmt.Println("=== Example 3: ABORTED Transaction (Rollback) ===\n")
	fmt.Println("Scenario: Transaction fails and needs to be rolled back")
	fmt.Println("SQL: BEGIN; UPDATE users SET balance=balance-100 WHERE id=1; -- Error! ABORT\n")

	tid := primitives.NewTransactionID()
	pageID := heap.NewHeapPageID(1, 0)

	// Step 1: BEGIN
	beginRecord := log.NewLogRecord(log.BeginRecord, tid, primitives.PageID(nil), nil, nil, 0)
	beginRecord.LSN = writer.CurrentLSN()
	serialized, _ := log.SerializeLogRecord(beginRecord)
	beginLSN, _ := writer.Write(serialized)

	fmt.Printf("1. BEGIN Transaction (LSN: %d, TID: %s)\n\n", beginLSN, tid.String())

	// Step 2: UPDATE record
	beforeImage := []byte("id=1, balance=500")
	afterImage := []byte("id=1, balance=400")

	updateRecord := log.NewLogRecord(log.UpdateRecord, tid, pageID, beforeImage, afterImage, beginLSN)
	updateRecord.LSN = writer.CurrentLSN()
	serialized, _ = log.SerializeLogRecord(updateRecord)
	updateLSN, _ := writer.Write(serialized)

	fmt.Printf("2. UPDATE Record (LSN: %d)\n", updateLSN)
	fmt.Printf("   Before: %s\n", string(beforeImage))
	fmt.Printf("   After:  %s\n\n", string(afterImage))

	// Step 3: Error occurs - ABORT
	fmt.Printf("3. ERROR! Transaction must be aborted\n\n")

	// Step 4: Generate CLR (Compensation Log Record) to undo changes
	clrRecord := &log.LogRecord{
		LSN:         writer.CurrentLSN(),
		Type:        log.CLRRecord,
		TID:         tid,
		PrevLSN:     updateLSN,
		PageID:      pageID,
		UndoNextLSN: beginLSN, // Next record to undo
		AfterImage:  beforeImage, // Restore to before state
		Timestamp:   time.Now(),
	}

	serialized, _ = log.SerializeLogRecord(clrRecord)
	clrLSN, _ := writer.Write(serialized)

	fmt.Printf("4. CLR Record (Compensation Log Record)\n")
	fmt.Printf("   LSN: %d | TID: %s\n", clrLSN, tid.String())
	fmt.Printf("   Undo Next LSN: %d\n", clrRecord.UndoNextLSN)
	fmt.Printf("   Action: Restoring data to: %s\n\n", string(beforeImage))

	// Step 5: ABORT
	abortRecord := log.NewLogRecord(log.AbortRecord, tid, primitives.PageID(nil), nil, nil, clrLSN)
	abortRecord.LSN = writer.CurrentLSN()
	serialized, _ = log.SerializeLogRecord(abortRecord)
	abortLSN, _ := writer.Write(serialized)
	writer.Force(abortLSN)

	fmt.Printf("5. ABORT Transaction (LSN: %d, TID: %s)\n\n", abortLSN, tid.String())

	fmt.Println("✓ Transaction rolled back successfully!")
	fmt.Println("✓ CLR records ensure we don't undo the same operation twice during recovery.\n")
	return nil
}

// Example4_MultipleOperations demonstrates a transaction with multiple operations
func Example4_MultipleOperations(writer *log.LogWriter) error {
	fmt.Println("=== Example 4: Transaction with Multiple Operations ===\n")
	fmt.Println("Scenario: Money transfer between two accounts")
	fmt.Println("SQL: BEGIN;")
	fmt.Println("     UPDATE accounts SET balance=balance-100 WHERE id=1;")
	fmt.Println("     UPDATE accounts SET balance=balance+100 WHERE id=2;")
	fmt.Println("     COMMIT;\n")

	tid := primitives.NewTransactionID()

	// Step 1: BEGIN
	beginRecord := log.NewLogRecord(log.BeginRecord, tid, primitives.PageID(nil), nil, nil, 0)
	beginRecord.LSN = writer.CurrentLSN()
	serialized, _ := log.SerializeLogRecord(beginRecord)
	beginLSN, _ := writer.Write(serialized)

	fmt.Printf("1. BEGIN Transaction (LSN: %d, TID: %s)\n\n", beginLSN, tid.String())

	// Step 2: Debit from account 1
	pageID1 := heap.NewHeapPageID(2, 0)
	update1 := log.NewLogRecord(
		log.UpdateRecord, tid, pageID1,
		[]byte("id=1, balance=1000"),
		[]byte("id=1, balance=900"),
		beginLSN,
	)
	update1.LSN = writer.CurrentLSN()
	serialized, _ = log.SerializeLogRecord(update1)
	update1LSN, _ := writer.Write(serialized)

	fmt.Printf("2. UPDATE Record - Debit Account 1 (LSN: %d)\n", update1LSN)
	fmt.Printf("   Page: Table=2, Page=0\n")
	fmt.Printf("   Before: id=1, balance=1000\n")
	fmt.Printf("   After:  id=1, balance=900\n\n")

	// Step 3: Credit to account 2
	pageID2 := heap.NewHeapPageID(2, 1)
	update2 := log.NewLogRecord(
		log.UpdateRecord, tid, pageID2,
		[]byte("id=2, balance=500"),
		[]byte("id=2, balance=600"),
		update1LSN,
	)
	update2.LSN = writer.CurrentLSN()
	serialized, _ = log.SerializeLogRecord(update2)
	update2LSN, _ := writer.Write(serialized)

	fmt.Printf("3. UPDATE Record - Credit Account 2 (LSN: %d)\n", update2LSN)
	fmt.Printf("   Page: Table=2, Page=1\n")
	fmt.Printf("   Before: id=2, balance=500\n")
	fmt.Printf("   After:  id=2, balance=600\n\n")

	// Step 4: COMMIT
	commitRecord := log.NewLogRecord(log.CommitRecord, tid, primitives.PageID(nil), nil, nil, update2LSN)
	commitRecord.LSN = writer.CurrentLSN()
	serialized, _ = log.SerializeLogRecord(commitRecord)
	commitLSN, _ := writer.Write(serialized)
	writer.Force(commitLSN)

	fmt.Printf("4. COMMIT Transaction (LSN: %d, TID: %s)\n\n", commitLSN, tid.String())

	fmt.Println("Key Points:")
	fmt.Printf("- PrevLSN chain: %d → %d → %d → %d\n", beginLSN, update1LSN, update2LSN, commitLSN)
	fmt.Println("- All operations are atomic - either all succeed or all fail")
	fmt.Println("- If crash happens before COMMIT, all operations are rolled back!\n")
	return nil
}

// Example5_Checkpoint demonstrates checkpoint operations
func Example5_Checkpoint(writer *log.LogWriter) error {
	fmt.Println("=== Example 5: Checkpoint Operation ===\n")
	fmt.Println("Scenario: System creates a checkpoint to speed up recovery\n")

	fmt.Printf("1. Active Transactions: T1, T2, T3\n\n")

	// Checkpoint begin
	ckptBegin := &log.LogRecord{
		LSN:       writer.CurrentLSN(),
		Type:      log.CheckpointBegin,
		Timestamp: time.Now(),
	}
	serialized, _ := log.SerializeLogRecord(ckptBegin)
	ckptBeginLSN, _ := writer.Write(serialized)

	fmt.Printf("2. CHECKPOINT BEGIN (LSN: %d)\n", ckptBeginLSN)
	fmt.Printf("   Recording state of all active transactions...\n\n")

	// Checkpoint end
	ckptEnd := &log.LogRecord{
		LSN:       writer.CurrentLSN(),
		Type:      log.CheckpointEnd,
		PrevLSN:   ckptBeginLSN,
		Timestamp: time.Now(),
	}
	serialized, _ = log.SerializeLogRecord(ckptEnd)
	ckptEndLSN, _ := writer.Write(serialized)
	writer.Force(ckptEndLSN)

	fmt.Printf("3. CHECKPOINT END (LSN: %d)\n", ckptEndLSN)
	fmt.Printf("   Active Transactions: [T1, T2, T3]\n")
	fmt.Printf("   Dirty Pages flushed to disk\n\n")

	fmt.Println("Purpose of Checkpoints:")
	fmt.Println("- Mark a point where all dirty pages are written to disk")
	fmt.Println("- Recovery only needs to start from last checkpoint, not from beginning")
	fmt.Println("- Significantly reduces recovery time after crashes\n")
	return nil
}

// RunAllExamples runs all the examples in sequence and writes to a real log file
func RunAllExamples() error {
	// Create a new log file
	file, err := os.Create(ExampleLogFile)
	if err != nil {
		return fmt.Errorf("failed to create log file: %w", err)
	}
	defer file.Close()

	// Create log writer
	writer := log.NewLogWriter(file, 4096, 0, 0)
	defer writer.Close()

	fmt.Println("╔════════════════════════════════════════════════════════════╗")
	fmt.Println("║  Write-Ahead Logging (WAL) Examples for StoreMy Database  ║")
	fmt.Println("╔════════════════════════════════════════════════════════════╗\n")

	examples := []struct {
		name string
		fn   func(*log.LogWriter) error
	}{
		{"Simple INSERT", Example1_SimpleInsert},
		{"UPDATE Operation", Example2_UpdateOperation},
		{"Aborted Transaction", Example3_AbortedTransaction},
		{"Multiple Operations", Example4_MultipleOperations},
		{"Checkpoint", Example5_Checkpoint},
	}

	for i, example := range examples {
		if err := example.fn(writer); err != nil {
			return fmt.Errorf("example %s failed: %w", example.name, err)
		}
		if i < len(examples)-1 {
			fmt.Println("─────────────────────────────────────────────────────────────\n")
		}
	}

	fmt.Println("═════════════════════════════════════════════════════════════")
	fmt.Printf("\n✓ All log records written to: %s\n", ExampleLogFile)
	fmt.Println("\nNow visualize the logs with the interactive viewer:")
	fmt.Printf("  go run pkg/debug/logreader.go %s\n\n", ExampleLogFile)
	fmt.Println("Navigation:")
	fmt.Println("  ↑/↓ or j/k  - Move between records")
	fmt.Println("  Enter/Space - View detailed information")
	fmt.Println("  Esc         - Back to list view")
	fmt.Println("  q           - Quit")
	fmt.Println("═════════════════════════════════════════════════════════════")

	return nil
}
