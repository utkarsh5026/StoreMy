package planner

import (
	"fmt"
	"os"
	"storemy/pkg/parser/statements"
)

type DropTablePlan struct {
	Statement      *statements.DropStatement
	ctx            DbContext
	transactionCtx TransactionCtx
}

func NewDropTablePlan(
	stmt *statements.DropStatement,
	ctx DbContext,
	transactionCtx TransactionCtx,
) *DropTablePlan {
	return &DropTablePlan{
		Statement:      stmt,
		ctx:            ctx,
		transactionCtx: transactionCtx,
	}
}

// Execute performs the DROP TABLE operation within the current transaction.
//
// Execution steps:
//  1. Validates table exists (respects IF EXISTS clause)
//  2. Drops all indexes associated with the table (including primary key indexes)
//  3. Removes table entry from catalog
//  4. CatalogManager handles cleanup of data files and cache
func (p *DropTablePlan) Execute() (any, error) {
	catalogMgr := p.ctx.CatalogManager()
	if !catalogMgr.TableExists(p.transactionCtx, p.Statement.TableName) {
		if p.Statement.IfExists {
			return &DDLResult{
				Success: true,
				Message: fmt.Sprintf("Table %s does not exist (IF EXISTS)", p.Statement.TableName),
			}, nil
		}
		return nil, fmt.Errorf("table %s does not exist", p.Statement.TableName)
	}

	// Get table ID for index lookup
	tableID, err := catalogMgr.GetTableID(p.transactionCtx, p.Statement.TableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table ID: %v", err)
	}

	// Drop all indexes associated with this table first
	if err := p.dropTableIndexes(tableID); err != nil {
		return nil, fmt.Errorf("failed to drop table indexes: %v", err)
	}

	// Now drop the table itself
	err = catalogMgr.DropTable(p.transactionCtx, p.Statement.TableName)
	if err != nil {
		return nil, fmt.Errorf("failed to drop table: %v", err)
	}

	return &DDLResult{
		Success: true,
		Message: fmt.Sprintf("Table %s dropped successfully", p.Statement.TableName),
	}, nil
}

// dropTableIndexes drops all indexes associated with a table.
// This includes any primary key indexes that were automatically created.
func (p *DropTablePlan) dropTableIndexes(tableID int) error {
	catalogMgr := p.ctx.CatalogManager()

	// Get all indexes for this table
	indexes, err := catalogMgr.GetIndexesByTable(p.transactionCtx, tableID)
	if err != nil {
		// If no indexes found or error, log but don't fail the drop
		fmt.Printf("Warning: failed to get indexes for table: %v\n", err)
		return nil
	}

	// Drop each index
	var droppedCount int
	for _, indexMeta := range indexes {
		filePath, err := catalogMgr.DropIndex(p.transactionCtx, indexMeta.IndexName)
		if err != nil {
			fmt.Printf("Warning: failed to drop index %s: %v\n", indexMeta.IndexName, err)
			continue
		}

		// Delete the physical index file
		if err := os.Remove(filePath); err != nil {
			// Log but don't fail - file might already be deleted
			fmt.Printf("Warning: failed to delete index file %s: %v\n", filePath, err)
		}

		droppedCount++
	}

	if droppedCount > 0 {
		fmt.Printf("Dropped %d index(es) associated with the table\n", droppedCount)
	}

	return nil
}
