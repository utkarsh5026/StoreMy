package planner

import (
	"fmt"
	"os"
	"storemy/pkg/parser/statements"
)

// DropIndexPlan represents the execution plan for DROP INDEX statement.
// It handles index removal from both the catalog and disk storage.
//
// Execution flow:
//  1. Validates index exists (respects IF EXISTS clause)
//  2. Removes index metadata from CATALOG_INDEXES
//  3. Deletes physical index file from disk
//  4. Returns DDL result with success message
type DropIndexPlan struct {
	Statement      *statements.DropIndexStatement
	ctx            DbContext
	transactionCtx TransactionCtx
}

// NewDropIndexPlan creates a new DROP INDEX plan instance.
func NewDropIndexPlan(
	stmt *statements.DropIndexStatement,
	ctx DbContext,
	transactionCtx TransactionCtx,
) *DropIndexPlan {
	return &DropIndexPlan{
		Statement:      stmt,
		ctx:            ctx,
		transactionCtx: transactionCtx,
	}
}

// Execute performs the DROP INDEX operation within the current transaction.
//
// Steps:
//  1. Validates index exists (respects IF EXISTS clause)
//  2. Retrieves index metadata (including file path)
//  3. Removes index entry from CATALOG_INDEXES
//  4. Deletes physical index file from disk
//  5. Returns success result
func (p *DropIndexPlan) Execute() (any, error) {
	cm := p.ctx.CatalogManager()
	tid := p.transactionCtx.ID
	idxName, tableName := p.Statement.IndexName, p.Statement.TableName

	if !cm.IndexExists(tid, idxName) {
		if p.Statement.IfExists {
			return &DDLResult{
				Success: true,
				Message: fmt.Sprintf("Index %s does not exist (IF EXISTS)", idxName),
			}, nil
		}
		return nil, fmt.Errorf("index %s does not exist", idxName)
	}

	idx, err := cm.GetIndexByName(tid, idxName)
	if err != nil {
		return nil, fmt.Errorf("failed to get index metadata: %v", err)
	}

	if tableName != "" {
		tn, err := cm.GetTableName(tid, idx.TableID)
		if err != nil {
			return nil, fmt.Errorf("failed to verify table name: %v", err)
		}
		if tn != tableName {
			return nil, fmt.Errorf("index %s does not belong to table %s",
				p.Statement.IndexName, tableName)
		}
	}

	filePath, err := cm.DropIndex(p.transactionCtx, p.Statement.IndexName)
	if err != nil {
		return nil, fmt.Errorf("failed to drop index from catalog: %v", err)
	}

	if err := p.deleteIndexFile(filePath); err != nil {
		fmt.Printf("Warning: failed to delete index file %s: %v\n", filePath, err)
	}

	return &DDLResult{
		Success: true,
		Message: fmt.Sprintf("Index %s dropped successfully", p.Statement.IndexName),
	}, nil
}

// deleteIndexFile removes the physical index file from disk.
// Returns error if file deletion fails, but this is typically non-critical.
func (p *DropIndexPlan) deleteIndexFile(filePath string) error {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// File doesn't exist, which is okay (maybe already deleted)
		return nil
	}

	if err := os.Remove(filePath); err != nil {
		return fmt.Errorf("failed to remove file %s: %v", filePath, err)
	}

	return nil
}
