package memory

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/primitives"
)

// CommitTransaction finalizes all changes made by a transaction and makes them durable.
// This implements the commit phase of the 2-Phase Commit protocol:
//  1. Log COMMIT record to WAL
//  2. Update before-images for all dirty pages (for next transaction)
//  3. Flush all dirty pages to disk (FORCE policy)
//  4. Release all locks held by transaction
//
// After commit completes:
//   - All changes are durable (survive crash)
//   - Other transactions can see the changes
//   - Transaction resources are released
//
// Parameters:
//   - ctx: Transaction context containing dirty pages and lock information
func (p *PageStore) CommitTransaction(ctx TxContext) error {
	return p.finalizeTransaction(ctx, CommitOperation)
}

// AbortTransaction undoes all changes made by a transaction and releases its resources.
// This implements the rollback mechanism:
//  1. Log ABORT record to WAL
//  2. Restore before-images for all dirty pages (UNDO)
//  3. Remove uncommitted pages from cache
//  4. Release all locks held by transaction
//
// After abort completes:
//   - All changes are discarded (not visible to any transaction)
//   - Database state is as if transaction never executed
//   - Transaction resources are released
//
// Parameters:
//   - ctx: Transaction context containing dirty pages and lock information
func (p *PageStore) AbortTransaction(ctx TxContext) error {
	return p.finalizeTransaction(ctx, AbortOperation)
}

// finalizeTransaction is the unified handler for COMMIT and ABORT operations.
// It implements the common transaction finalization protocol:
//  1. Validate transaction context
//  2. Check for dirty pages (no-op if read-only transaction)
//  3. Log operation to WAL
//  4. Execute operation-specific logic (commit or abort)
//  5. Release all locks
//
// This centralizes lock management and WAL logging for transaction termination.
//
// Parameters:
//   - ctx: Transaction context (must not be nil)
//   - operation: CommitOperation or AbortOperation
//
// Returns an error if any step fails. The transaction may be in an inconsistent
func (p *PageStore) finalizeTransaction(ctx *transaction.TransactionContext, operation OperationType) error {
	if ctx == nil || ctx.ID == nil {
		return fmt.Errorf("transaction context cannot be nil")
	}

	dirtyPageIDs := ctx.GetDirtyPages()
	if len(dirtyPageIDs) == 0 {
		p.lockManager.UnlockAllPages(ctx.ID)
		return nil
	}

	if err := p.logOperation(operation, ctx.ID, nil, nil); err != nil {
		return err
	}

	var err error
	switch operation {
	case CommitOperation:
		err = p.handleCommit(dirtyPageIDs)

	case AbortOperation:
		err = p.handleAbort(dirtyPageIDs)

	default:
		return fmt.Errorf("unknown operation: %s", operation.String())
	}

	if err != nil {
		return err
	}

	p.lockManager.UnlockAllPages(ctx.ID)
	return nil
}

// handleCommit executes the commit phase for dirty pages:
//  1. Update before-images (for next transaction's rollback)
//  2. Flush all dirty pages to disk (FORCE policy)
//
// This ensures durability - after commit returns, changes survive crashes.
//
// Parameters:
//   - dirtyPageIDs: List of pages modified by the committing transaction
//
// Returns an error if any page flush fails. This is a serious error that may
// leave the database in an inconsistent state requiring recovery.
func (p *PageStore) handleCommit(dirtyPageIDs []primitives.PageID) error {
	p.mutex.Lock()
	for _, pid := range dirtyPageIDs {
		if page, exists := p.cache.Get(pid); exists {
			page.SetBeforeImage()
			p.cache.Put(pid, page)
		}
	}
	p.mutex.Unlock()

	for _, pid := range dirtyPageIDs {
		dbFile, err := p.getDbFileForPage(pid)
		if err != nil {
			return fmt.Errorf("commit failed: unable to get dbFile for page %v: %v", pid, err)
		}
		if err := p.flushPage(dbFile, pid); err != nil {
			return fmt.Errorf("commit failed: unable to flush page %v: %v", pid, err)
		}
	}
	return nil
}

// handleAbort executes the rollback phase for dirty pages:
//  1. Restore before-images for all modified pages (UNDO)
//  2. Remove pages without before-images (newly allocated)
//
// This ensures atomicity - after abort returns, no trace of the transaction remains.
//
// Parameters:
//   - dirtyPageIDs: List of pages modified by the aborting transaction
//
// Returns an error only in exceptional cases (should not normally fail).
//
// Thread-safe: Acquires lock for entire operation to ensure atomic rollback.
func (p *PageStore) handleAbort(dirtyPageIDs []primitives.PageID) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, pid := range dirtyPageIDs {
		page, exists := p.cache.Get(pid)
		if !exists {
			continue
		}

		if beforeImage := page.GetBeforeImage(); beforeImage != nil {
			p.cache.Put(pid, beforeImage)
		} else {
			p.cache.Remove(pid)
		}
	}
	return nil
}
