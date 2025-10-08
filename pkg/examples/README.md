# Write-Ahead Logging (WAL) Examples

This package contains interactive examples that demonstrate how Write-Ahead Logging works in the StoreMy database system.

## What is Write-Ahead Logging?

Write-Ahead Logging (WAL) is a fundamental technique used in database systems to ensure:
- **Atomicity**: Transactions are all-or-nothing
- **Durability**: Committed data survives crashes
- **Consistency**: Database stays in a valid state
- **Recovery**: System can recover after crashes

### The Golden Rule: **Write to log BEFORE modifying data!**

## Quick Start

### 1. Run the Examples to Generate Real Log Files

```bash
# Run all examples - this creates REAL log records in examples_wal.log
cd pkg/examples
go run .

# Or from project root
make examples
```

This will:
- Execute 5 different transaction examples
- Write actual log records to `examples_wal.log`
- Show you what's happening in real-time

### 2. Visualize the Logs with Interactive Viewer

```bash
# From pkg/examples directory
go run pkg/debug/logreader.go examples_wal.log

# Or use absolute path
go run pkg/debug/logreader.go pkg/examples/examples_wal.log
```

**Navigation:**
- `↑/↓` or `j/k` - Move between records
- `Enter/Space` - View detailed information
- `Esc` - Back to list view
- `q` - Quit

## Examples Overview

### Example 1: Simple INSERT Transaction
**What it shows:** Basic transaction flow with BEGIN → INSERT → COMMIT

**Key Concepts:**
- Each operation gets a unique LSN (Log Sequence Number)
- INSERT records have "after image" (new data)
- Logs are written before data hits disk

### Example 2: UPDATE Operation
**What it shows:** Updating existing records with before/after images

**Key Concepts:**
- UPDATE records have both "before image" and "after image"
- Before image: Used for UNDO (rollback)
- After image: Used for REDO (crash recovery)

### Example 3: Aborted Transaction
**What it shows:** Transaction rollback using CLR (Compensation Log Records)

**Key Concepts:**
- When transactions abort, changes must be undone
- CLR records log the undo operations
- CLR records are never undone themselves (idempotent)

### Example 4: Multiple Operations
**What it shows:** Complex transaction with multiple operations (money transfer)

**Key Concepts:**
- Transactions can contain multiple operations
- All operations are linked via PrevLSN (forms a chain)
- Atomicity: Either ALL operations succeed or ALL fail

### Example 5: Checkpoint
**What it shows:** How checkpoints speed up recovery

**Key Concepts:**
- Checkpoints mark "safe points" in the log
- All dirty pages flushed to disk at checkpoint
- Recovery starts from last checkpoint, not from beginning


## Understanding Log Records

Each log record contains:

| Field | Description |
|-------|-------------|
| **LSN** | Log Sequence Number - unique identifier |
| **Type** | BEGIN, COMMIT, ABORT, INSERT, UPDATE, DELETE, CLR, CHECKPOINT |
| **TID** | Transaction ID - which transaction made this change |
| **PrevLSN** | Previous LSN for this transaction (forms chain) |
| **PageID** | Which page was modified (Table + Page Number) |
| **BeforeImage** | Data before modification (for UNDO) |
| **AfterImage** | Data after modification (for REDO) |
| **Timestamp** | When this operation occurred |

## Log Record Types

### Transaction Control
- **BEGIN**: Start of a new transaction
- **COMMIT**: Transaction completed successfully
- **ABORT**: Transaction failed and rolled back

### Data Modifications
- **INSERT**: New record added (only after image)
- **UPDATE**: Existing record modified (before + after image)
- **DELETE**: Record removed (only before image)

### Recovery Records
- **CLR**: Compensation Log Record (records an undo operation)
- **CHECKPOINT_BEGIN**: Start recording checkpoint
- **CHECKPOINT_END**: Checkpoint complete

## Seeing It In Action

### What You'll See in the Log Viewer

After running the examples and opening `examples_wal.log` in the log reader, you'll see:

1. **List View**: All 18 log records from the 5 examples
   - Color-coded record types (BEGIN, INSERT, UPDATE, COMMIT, ABORT, CLR, CHECKPOINT)
   - LSN values showing sequential ordering
   - Transaction IDs grouping related operations
   - Timestamps showing when each operation occurred

2. **Detail View**: Click any record to see:
   - Full record metadata (LSN, PrevLSN, TID, Timestamp)
   - Page information (Table ID, Page Number)
   - Before/After image sizes
   - CLR-specific fields (UndoNextLSN)

### Example: Viewing a Money Transfer

When you navigate to Example 4 in the log viewer, you'll see the complete transaction chain:
- **LSN 463**: BEGIN (starts the transaction)
- **LSN 492**: UPDATE (debit account 1, PrevLSN→463)
- **LSN 572**: UPDATE (credit account 2, PrevLSN→492)
- **LSN 651**: COMMIT (finalizes transaction, PrevLSN→572)

Notice how PrevLSN creates a **backward chain** through the transaction!

## Visual Guide to Log Chain

```
Transaction T1:
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ LSN: 100    │     │ LSN: 101    │     │ LSN: 102    │     │ LSN: 103    │
│ Type: BEGIN │────▶│ Type: INSERT│────▶│ Type: UPDATE│────▶│ Type: COMMIT│
│ PrevLSN: 0  │     │ PrevLSN: 100│     │ PrevLSN: 101│     │ PrevLSN: 102│
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

The PrevLSN creates a **backward chain** linking all operations in a transaction.
This chain is crucial for recovery!

## Recovery Example Walkthrough

**Scenario:** System crashes during a transaction

```
Log State Before Crash:
LSN 1: BEGIN T1
LSN 2: INSERT T1 (data=Alice)
LSN 3: COMMIT T1
LSN 4: BEGIN T2
LSN 5: UPDATE T2 (data: Bob→Bobby)
💥 CRASH!
```

**Recovery Process:**

1. **Analysis:**
   - T1: Committed (has COMMIT record)
   - T2: Uncommitted (no COMMIT record)

2. **REDO:**
   - Replay LSN 2: Re-insert Alice (even if already on disk)
   - Ensures T1 is durable

3. **UNDO:**
   - Rollback LSN 5: Restore Bob
   - Write CLR record
   - Write ABORT T2

**Result:** Database consistent! T1's changes preserved, T2's changes removed.

## Tips for Understanding WAL

1. **Think Sequential:** Logs are append-only. New records always go at the end.

2. **LSN is King:** Everything is ordered by LSN. Lower LSN = happened earlier.

3. **Before/After Images:**
   - Before = What to restore if we UNDO
   - After = What to replay if we REDO

4. **CLR Records:** Special records that log undo operations. They ensure we don't undo twice.

5. **Checkpoints:** Not required for correctness, but critical for performance.

## Further Reading

- **ARIES Algorithm**: The recovery algorithm used by most databases
- **WAL Protocol**: Ensures logs are written before data pages
- **Log Shipping**: Using WAL for replication and backups

## Testing Your Understanding

Try to answer these questions after running the examples:

1. Why do we need both before and after images?
2. What happens if we crash right after COMMIT is written?
3. Why can't we undo a CLR record?
4. How does PrevLSN help during recovery?
5. What's the difference between REDO and UNDO?

Run the examples and use the log reader to explore these concepts!
