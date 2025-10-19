package planner

import (
	"fmt"
	"os"
	"storemy/pkg/parser/statements"
	"storemy/pkg/types"
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
	ctx       DbContext
	tx        TransactionCtx
}

// NewCreateIndexPlan creates a new CREATE INDEX plan instance.
func NewCreateIndexPlan(
	stmt *statements.CreateIndexStatement,
	ctx DbContext,
	tx TransactionCtx,
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
//  1. Validates table exists
//  2. Validates column exists in the table
//  3. Checks if index already exists (respects IF NOT EXISTS)
//  4. Determines index type and creates appropriate index file
//  5. Registers index in catalog (CATALOG_INDEXES)
//  6. Populates index with existing data from the table
//  7. Returns success result
func (p *CreateIndexPlan) Execute() (Result, error) {
	cm := p.ctx.CatalogManager()
	tableName, indexName, colName := p.Statement.TableName, p.Statement.IndexName, p.Statement.ColumnName
	idxType := p.Statement.IndexType

	if !cm.TableExists(p.tx, tableName) {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	tableID, err := cm.GetTableID(p.tx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table ID: %v", err)
	}

	tsch, err := cm.GetTableSchema(p.tx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %v", err)
	}

	columnIndex := tsch.GetFieldIndex(colName)
	if columnIndex < 0 {
		return nil, fmt.Errorf("column %s does not exist in table %s",
			colName, tableName)
	}

	if cm.IndexExists(p.tx, p.Statement.IndexName) {
		if p.Statement.IfNotExists {
			return &DDLResult{
				Success: true,
				Message: fmt.Sprintf("Index %s already exists (IF NOT EXISTS)", p.Statement.IndexName),
			}, nil
		}
		return nil, fmt.Errorf("index %s already exists", p.Statement.IndexName)
	}

	indexID, filePath, err := cm.CreateIndex(
		p.tx,
		indexName,
		tableName,
		colName,
		idxType,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create index in catalog: %v", err)
	}

	columnType := tsch.Columns[columnIndex].FieldType

	if err := p.createIndex(filePath, columnType); err != nil {
		return nil, err
	}

	if err := p.populateIndex(
		filePath,
		columnType,
		columnIndex,
		indexID,
		tableID); err != nil {
		return nil, err
	}

	return &DDLResult{
		Success: true,
		Message: fmt.Sprintf("Index %s created successfully on %s(%s) using %s",
			p.Statement.IndexName,
			tableName,
			colName,
			idxType),
	}, nil
}

func (p *CreateIndexPlan) createIndex(indexFilePath string, indexColType types.Type) error {
	cm := p.ctx.CatalogManager()
	im := p.ctx.IndexManager()
	if err := im.CreatePhysicalIndex(indexFilePath, indexColType, p.Statement.IndexType); err != nil {
		if _, dropErr := cm.DropIndex(p.tx, p.Statement.IndexName); dropErr != nil {
			fmt.Printf("Warning: failed to cleanup catalog entry after index creation failure: %v\n", dropErr)
		}
		return fmt.Errorf("failed to create physical index: %v", err)
	}
	return nil
}

func (p *CreateIndexPlan) populateIndex(indexFilePath string, indexColType types.Type, indexCol, indexID, tableID int) error {
	cm := p.ctx.CatalogManager()
	im := p.ctx.IndexManager()
	tableFile, err := cm.GetTableFile(tableID)
	if err != nil {
		os.Remove(indexFilePath)
		if _, dropErr := cm.DropIndex(p.tx, p.Statement.IndexName); dropErr != nil {
			fmt.Printf("Warning: failed to cleanup catalog entry after file access failure: %v\n", dropErr)
		}
		return fmt.Errorf("failed to get table file: %v", err)
	}

	if err := im.PopulateIndex(p.tx, indexFilePath, indexID, tableFile, indexCol, indexColType, p.Statement.IndexType); err != nil {
		os.Remove(indexFilePath)
		if _, dropErr := cm.DropIndex(p.tx, p.Statement.IndexName); dropErr != nil {
			fmt.Printf("Warning: failed to cleanup catalog entry after population failure: %v\n", dropErr)
		}
		return fmt.Errorf("failed to populate index: %v", err)
	}
	return nil
}
