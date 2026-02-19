package indexops

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/parser/statements"
	"storemy/pkg/planner/internal/shared"
	"storemy/pkg/registry"
)

// CreateIndexPlan represents the execution plan for CREATE INDEX statement.
// It coordinates index creation, population, and catalog registration.
//
// Execution flow:
//  1. Validates table and column existence
//  2. Checks IF NOT EXISTS clause
//  3. Creates index file on disk (hash or btree)
//  4. Registers index metadata in CATALOG_INDEXES
//  5. Populates index with existing table data
//  6. Returns DDL result with success message
type CreateIndexPlan struct {
	Statement *statements.CreateIndexStatement
	ctx       *registry.DatabaseContext
	tx        *transaction.TransactionContext
}

// NewCreateIndexPlan creates a new CREATE INDEX plan instance.
func NewCreateIndexPlan(
	stmt *statements.CreateIndexStatement,
	ctx *registry.DatabaseContext,
	tx *transaction.TransactionContext,
) *CreateIndexPlan {
	return &CreateIndexPlan{
		Statement: stmt,
		ctx:       ctx,
		tx:        tx,
	}
}

// Execute performs the CREATE INDEX operation within the current transaction.
//
// Steps:
//  1. Validates index creation (table exists, column exists, index name unique)
//  2. Handles IF NOT EXISTS clause
//  3. Creates physical index file
//  4. Registers index in catalog (CATALOG_INDEXES)
//  5. Populates index with existing data from the table
//  6. Returns success result
func (p *CreateIndexPlan) Execute() (shared.Result, error) {
	tableName, indexName, colName := p.Statement.TableName, p.Statement.IndexName, p.Statement.ColumnName
	idxType := p.Statement.IndexType

	catalogOps := p.ctx.CatalogManager().NewIndexOps(p.tx)

	// Step 1: Validate via catalog (single consolidated call)
	validation, err := catalogOps.ValidateIndexCreation(indexName, tableName, colName)
	if err != nil {
		// Handle IF NOT EXISTS for already-existing index
		if p.Statement.IfNotExists && fmt.Sprintf("%v", err) == fmt.Sprintf("index %s already exists", indexName) {
			return &shared.DDLResult{
				Success: true,
				Message: fmt.Sprintf("Index %s already exists (IF NOT EXISTS)", indexName),
			}, nil
		}
		return nil, err
	}

	filePath := GenerateIndexFilePath(p.ctx, tableName, indexName)
	physicalID, err := p.ctx.IndexManager().CreatePhysicalIndex(filePath, validation.ColumnType, idxType)
	if err != nil {
		return nil, fmt.Errorf("failed to create physical index: %w", err)
	}

	// Step 3: Register in catalog using actual file ID
	_, err = catalogOps.CreateIndex(physicalID, indexName, tableName, colName, idxType)
	if err != nil {
		// Rollback: Delete physical file (best-effort, ignore error)
		_ = p.ctx.IndexManager().DeletePhysicalIndex(filePath)
		return nil, fmt.Errorf("failed to register index in catalog: %w", err)
	}

	// Step 4: Populate the index with existing data
	idxConfig := IndexCreationConfig{
		Ctx:         p.ctx,
		Tx:          p.tx,
		IndexID:     physicalID,
		IndexName:   indexName,
		IndexType:   idxType,
		FilePath:    filePath,
		TableID:     validation.TableID,
		TableName:   tableName,
		ColumnIndex: validation.ColumnIndex,
		ColumnName:  colName,
		ColumnType:  validation.ColumnType,
	}

	if err := PopulateIndexWithData(&idxConfig); err != nil {
		// Rollback: Remove from catalog and delete physical file (best-effort, ignore errors)
		_, _ = catalogOps.DropIndex(indexName)
		_ = p.ctx.IndexManager().DeletePhysicalIndex(filePath)
		return nil, fmt.Errorf("failed to populate index: %w", err)
	}

	return &shared.DDLResult{
		Success: true,
		Message: fmt.Sprintf("Index %s created successfully on %s(%s) using %s",
			indexName, tableName, colName, idxType),
	}, nil
}
