package ddl

import (
	"fmt"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/parser/statements"
	"storemy/pkg/planner/internal/indexops"
	"storemy/pkg/planner/internal/result"
	"storemy/pkg/primitives"
	"storemy/pkg/registry"
	"storemy/pkg/storage/index"
)

type DbContext = *registry.DatabaseContext
type TxContext = *transaction.TransactionContext

type CreateTablePlan struct {
	Statement *statements.CreateStatement
	ctx       DbContext
	TxContext TxContext
}

func NewCreateTablePlan(stmt *statements.CreateStatement, ctx DbContext,
	TxContext TxContext,
) *CreateTablePlan {
	return &CreateTablePlan{
		Statement: stmt,
		ctx:       ctx,
		TxContext: TxContext,
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
func (p *CreateTablePlan) Execute() (result.Result, error) {
	cm := p.ctx.CatalogManager()
	if cm.TableExists(p.TxContext, p.Statement.TableName) {
		if p.Statement.IfNotExists {
			return &result.DDLResult{
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

	tableID, err := cm.CreateTable(p.TxContext, tableSchema)
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

	return &result.DDLResult{
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
			Position:  primitives.ColumnID(i),
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
//  3. Creates physical BTree index file and gets its actual ID
//  4. Registers index metadata in catalog using the actual file ID
//  5. Populates index with existing table data (if any)
func (p *CreateTablePlan) createPrimaryKeyIndex(tableID primitives.FileID, tableSchema *schema.Schema) error {
	cm := p.ctx.CatalogManager()

	pkColumnIndex, err := tableSchema.GetFieldIndex(p.Statement.PrimaryKey)
	if err != nil {
		return fmt.Errorf("primary key column %s not found in schema %w", p.Statement.PrimaryKey, err)
	}

	pkColumn := tableSchema.Columns[pkColumnIndex]
	indexName := fmt.Sprintf("pk_%s_%s", p.Statement.TableName, p.Statement.PrimaryKey)

	if cm.IndexExists(p.TxContext, indexName) {
		return nil
	}

	// Generate file path using centralized helper
	filePath := indexops.GenerateIndexFilePath(p.ctx, p.Statement.TableName, indexName)

	// Step 1: Create physical index file and get actual ID
	idxConfig := indexops.IndexCreationConfig{
		Ctx:         p.ctx,
		Tx:          p.TxContext,
		IndexName:   indexName,
		IndexType:   index.BTreeIndex,
		FilePath:    filePath,
		TableID:     tableID,
		TableName:   p.Statement.TableName,
		ColumnIndex: pkColumnIndex,
		ColumnName:  p.Statement.PrimaryKey,
		ColumnType:  pkColumn.FieldType,
	}

	actualIndexID, err := indexops.CreatePhysicalIndexAndGetID(&idxConfig)
	if err != nil {
		return fmt.Errorf("failed to create physical index: %v", err)
	}

	_, err = cm.CreateIndex(p.TxContext, actualIndexID, indexName, p.Statement.TableName, p.Statement.PrimaryKey, index.BTreeIndex)
	if err != nil {
		return fmt.Errorf("failed to register index in catalog: %v", err)
	}

	// Update config with actual ID for population
	idxConfig.IndexID = actualIndexID

	// Step 3: Populate the index
	return indexops.PopulateIndexWithData(&idxConfig)
}
