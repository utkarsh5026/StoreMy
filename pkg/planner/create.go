package planner

import (
	"fmt"
	"os"

	"storemy/pkg/catalog/schema"
	"storemy/pkg/parser/statements"
	"storemy/pkg/storage/index"
)

type CreateTablePlan struct {
	Statement      *statements.CreateStatement
	ctx            DbContext
	transactionCtx TransactionCtx
}

func NewCreateTablePlan(
	stmt *statements.CreateStatement,
	ctx DbContext,
	transactionCtx TransactionCtx,
) *CreateTablePlan {
	return &CreateTablePlan{
		Statement:      stmt,
		ctx:            ctx,
		transactionCtx: transactionCtx,
	}
}

// Execute performs the CREATE TABLE operation within the current transaction.
//
// Execution steps:
//  1. Validates table doesn't already exist (respects IF NOT EXISTS clause)
//  2. Transforms parsed field definitions into schema metadata
//  3. Creates table entry in catalog with schema
//  4. CatalogManager handles file path generation and persistence
//  5. If primary key is specified, creates a BTree index on the primary key column
func (p *CreateTablePlan) Execute() (Result, error) {
	cm := p.ctx.CatalogManager()
	if cm.TableExists(p.transactionCtx, p.Statement.TableName) {
		if p.Statement.IfNotExists {
			return &DDLResult{
				Success: true,
				Message: fmt.Sprintf("Table %s already exists (IF NOT EXISTS)", p.Statement.TableName),
			}, nil
		}
		return nil, fmt.Errorf("table %s already exists", p.Statement.TableName)
	}

	tableSchema, err := p.makeTableSchema()
	if err != nil {
		return nil, err
	}

	tableID, err := cm.CreateTable(p.transactionCtx, tableSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %v", err)
	}

	var indexMessage string
	if p.Statement.PrimaryKey != "" {
		if err := p.createPrimaryKeyIndex(tableID, tableSchema); err != nil {
			fmt.Printf("Warning: failed to create primary key index: %v\n", err)
			indexMessage = fmt.Sprintf(" (primary key index creation failed: %v)", err)
		} else {
			indexMessage = fmt.Sprintf(" with BTree index on primary key %s", p.Statement.PrimaryKey)
		}
	}

	return &DDLResult{
		Success: true,
		Message: fmt.Sprintf("Table %s created successfully%s", p.Statement.TableName, indexMessage),
	}, nil
}

func (p *CreateTablePlan) makeTableSchema() (*schema.Schema, error) {
	columns := make([]schema.ColumnMetadata, len(p.Statement.Fields))
	for i, field := range p.Statement.Fields {
		isPrimary := field.Name == p.Statement.PrimaryKey
		columns[i] = schema.ColumnMetadata{
			Name:      field.Name,
			FieldType: field.Type,
			Position:  i,
			IsPrimary: isPrimary,
			IsAutoInc: field.AutoIncrement,
			TableID:   0, // Will be set by CatalogManager
		}
	}

	tableSchema, err := schema.NewSchema(0, p.Statement.TableName, columns)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema: %v", err)
	}
	tableSchema.PrimaryKey = p.Statement.PrimaryKey
	return tableSchema, nil
}

// createPrimaryKeyIndex creates a BTree index on the primary key column.
// This is automatically called during table creation if a primary key is specified.
//
// Steps:
//  1. Finds the column index of the primary key
//  2. Determines the data type of the primary key column
//  3. Generates index name and file path
//  4. Creates index metadata in catalog
//  5. Creates physical BTree index file
//  6. Populates index with existing table data (if any)
func (p *CreateTablePlan) createPrimaryKeyIndex(tableID int, tableSchema *schema.Schema) error {
	cm := p.ctx.CatalogManager()

	pkColumnIndex := tableSchema.GetFieldIndex(p.Statement.PrimaryKey)
	if pkColumnIndex < 0 {
		return fmt.Errorf("primary key column %s not found in schema", p.Statement.PrimaryKey)
	}

	pkColumn := tableSchema.Columns[pkColumnIndex]
	indexName := fmt.Sprintf("pk_%s_%s", p.Statement.TableName, p.Statement.PrimaryKey)

	if cm.IndexExists(p.transactionCtx, indexName) {
		return nil
	}

	indexID, filePath, err := cm.CreateIndex(
		p.transactionCtx,
		indexName,
		p.Statement.TableName,
		p.Statement.PrimaryKey,
		index.BTreeIndex,
	)
	if err != nil {
		return fmt.Errorf("failed to register index in catalog: %v", err)
	}

	indexMgr := p.ctx.IndexManager()

	if err := indexMgr.CreatePhysicalIndex(filePath, pkColumn.FieldType, index.BTreeIndex); err != nil {
		if _, dropErr := cm.DropIndex(p.transactionCtx, indexName); dropErr != nil {
			fmt.Printf("Warning: failed to cleanup catalog entry: %v\n", dropErr)
		}
		return fmt.Errorf("failed to create physical index file: %v", err)
	}

	tableFile, err := cm.GetTableFile(tableID)
	if err != nil {
		os.Remove(filePath)
		if _, dropErr := cm.DropIndex(p.transactionCtx, indexName); dropErr != nil {
			fmt.Printf("Warning: failed to cleanup catalog entry: %v\n", dropErr)
		}
		return fmt.Errorf("failed to get table file: %v", err)
	}

	if err := indexMgr.PopulateIndex(p.transactionCtx, filePath, indexID, tableFile, pkColumnIndex, pkColumn.FieldType, index.BTreeIndex); err != nil {
		os.Remove(filePath)
		if _, dropErr := cm.DropIndex(p.transactionCtx, indexName); dropErr != nil {
			fmt.Printf("Warning: failed to cleanup catalog entry: %v\n", dropErr)
		}
		return fmt.Errorf("failed to populate index: %v", err)
	}

	return nil
}
