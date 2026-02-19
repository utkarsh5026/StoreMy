package indexops

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/parser/statements"
	"storemy/pkg/planner/internal/shared"
	"storemy/pkg/registry"
)

// DropIndexPlan represents the execution plan for DROP INDEX statement.
// It handles index removal from both the catalog and disk storage.
//
// Execution flow:
//  1. Validates index exists (respects IF EXISTS clause)
//  2. Removes index metadata from CATALOG_INDEXES
//  3. Deletes physical index file from disk
//  4. Returns DDL result with success message
//
// Transaction Semantics:
//   - Catalog changes are transactional (rollback on failure)
//   - File deletion is best-effort (warns if fails but doesn't rollback)
//   - Uses page-level locks via CatalogManager for CATALOG_INDEXES access
//
// Example:
//
//	DROP INDEX idx_users_email;                    -- Fails if not exists
//	DROP INDEX IF EXISTS idx_users_email;           -- Succeeds even if missing
//	DROP INDEX idx_users_email ON users;            -- Validates table ownership
type DropIndexPlan struct {
	Statement *statements.DropIndexStatement  // Parsed DROP INDEX statement
	ctx       *registry.DatabaseContext       // Database context for catalog/index access
	tx        *transaction.TransactionContext // Current transaction for catalog operations
}

// NewDropIndexPlan creates a new DROP INDEX plan instance.
//
// Parameters:
//   - stmt: Parsed DROP INDEX statement containing index name and IF EXISTS flag
//   - ctx: Database context providing access to CatalogManager and IndexManager
//   - *transaction.TransactionContext: Active transaction for catalog modifications
//
// Returns:
//
//	Plan ready for execution via Execute() method
func NewDropIndexPlan(
	stmt *statements.DropIndexStatement,
	ctx *registry.DatabaseContext, tx *transaction.TransactionContext,
) *DropIndexPlan {
	return &DropIndexPlan{
		Statement: stmt,
		ctx:       ctx,
		tx:        tx,
	}
}

// Execute performs the DROP INDEX operation within the current transaction.
//
// Steps:
//  1. Validates index deletion (index exists, table ownership if specified)
//  2. Handles IF EXISTS clause
//  3. Removes index entry from CATALOG_INDEXES table
//  4. Deletes physical index file from disk
//
// Returns:
//   - DDLResult with success message on completion
//   - Error if index doesn't exist (without IF EXISTS) or validation fails
func (p *DropIndexPlan) Execute() (shared.Result, error) {
	idxName := p.Statement.IndexName
	tableName := p.Statement.TableName

	catalogOps := p.ctx.CatalogManager().NewIndexOps(p.tx)

	// Step 1: Validate via catalog (single consolidated call)
	_, err := catalogOps.ValidateIndexDeletion(idxName, tableName)
	if err != nil {
		// Handle IF EXISTS for non-existent index
		if p.Statement.IfExists {
			return shared.NewDDLResult(true, fmt.Sprintf("Index %s does not exist (IF EXISTS)", idxName)), nil
		}
		return nil, err
	}

	// Step 2: Delete index from system (catalog + physical file)
	indexOpsCoordinator := NewIndexOps(p.tx, p.ctx.CatalogManager(), p.ctx.IndexManager())
	if err := indexOpsCoordinator.DeleteIndexFromSystem(idxName); err != nil {
		return nil, fmt.Errorf("failed to drop index: %w", err)
	}

	return shared.NewDDLResult(true, fmt.Sprintf("Index %s dropped successfully", idxName)), nil
}
