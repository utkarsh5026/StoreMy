package planner

import (
	"fmt"
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
//  2. Removes table entry from catalog
//  3. CatalogManager handles cleanup of data files and cache
func (p *DropTablePlan) Execute() (any, error) {
	catalogMgr := p.ctx.CatalogManager()
	if !catalogMgr.TableExists(p.transactionCtx.ID, p.Statement.TableName) {
		if p.Statement.IfExists {
			return &DDLResult{
				Success: true,
				Message: fmt.Sprintf("Table %s does not exist (IF EXISTS)", p.Statement.TableName),
			}, nil
		}
		return nil, fmt.Errorf("table %s does not exist", p.Statement.TableName)
	}

	err := catalogMgr.DropTable(p.transactionCtx, p.Statement.TableName)
	if err != nil {
		return nil, fmt.Errorf("failed to drop table: %v", err)
	}

	return &DDLResult{
		Success: true,
		Message: fmt.Sprintf("Table %s dropped successfully", p.Statement.TableName),
	}, nil
}
