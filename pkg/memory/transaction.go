package memory

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/primitives"
)

// CommitTransaction finalizes all changes made by a transaction and makes them durable.
// This implements the commit phase with Write-Ahead Logging protocol.
func (p *PageStore) CommitTransaction(ctx *transaction.TransactionContext) error {
	return p.finalizeTransaction(ctx, CommitOperation)
}

// AbortTransaction undoes all changes made by a transaction and releases its resources.
// This implements the rollback mechanism with Write-Ahead Logging protocol.
func (p *PageStore) AbortTransaction(ctx *transaction.TransactionContext) error {
	return p.finalizeTransaction(ctx, AbortOperation)
}

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
