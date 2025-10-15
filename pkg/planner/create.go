package planner

import (
	"fmt"
	"os"
	"path/filepath"

	"storemy/pkg/catalog/schema"
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	btreeindex "storemy/pkg/memory/wrappers/btree_index"
	"storemy/pkg/parser/statements"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/index/btree"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type CreateTablePlan struct {
	Statement      *statements.CreateStatement
	ctx            DbContext
	transactionCtx TransactionCtx
}

type DDLResult struct {
	Success bool
	Message string
}

func (r *DDLResult) String() string {
	return fmt.Sprintf("DDL Result - Success: %t, Message: %s", r.Success, r.Message)
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
func (p *CreateTablePlan) Execute() (any, error) {
	catalogMgr := p.ctx.CatalogManager()
	if catalogMgr.TableExists(p.transactionCtx, p.Statement.TableName) {
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

	tableID, err := catalogMgr.CreateTable(p.transactionCtx, tableSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %v", err)
	}

	// If a primary key is specified, create a BTree index on it
	var indexMessage string
	if p.Statement.PrimaryKey != "" {
		if err := p.createPrimaryKeyIndex(tableID, tableSchema); err != nil {
			// Log the error but don't fail the table creation
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
	catalogMgr := p.ctx.CatalogManager()

	// Find the primary key column
	pkColumnIndex := tableSchema.GetFieldIndex(p.Statement.PrimaryKey)
	if pkColumnIndex < 0 {
		return fmt.Errorf("primary key column %s not found in schema", p.Statement.PrimaryKey)
	}

	pkColumn := tableSchema.Columns[pkColumnIndex]
	indexName := fmt.Sprintf("pk_%s_%s", p.Statement.TableName, p.Statement.PrimaryKey)

	// Check if index already exists (shouldn't happen, but be safe)
	if catalogMgr.IndexExists(p.transactionCtx, indexName) {
		return nil // Index already exists, nothing to do
	}

	// Create index in catalog
	indexID, filePath, err := catalogMgr.CreateIndex(
		p.transactionCtx,
		indexName,
		p.Statement.TableName,
		p.Statement.PrimaryKey,
		index.BTreeIndex,
	)
	if err != nil {
		return fmt.Errorf("failed to register index in catalog: %v", err)
	}

	// Create physical BTree index file
	if err := p.createPhysicalBTreeIndex(filePath, pkColumn.FieldType); err != nil {
		// Cleanup catalog entry on failure
		if _, dropErr := catalogMgr.DropIndex(p.transactionCtx, indexName); dropErr != nil {
			fmt.Printf("Warning: failed to cleanup catalog entry: %v\n", dropErr)
		}
		return fmt.Errorf("failed to create physical index file: %v", err)
	}

	// Populate the index with existing data (if table has data)
	if err := p.populateBTreeIndex(filePath, indexID, tableID, pkColumnIndex, pkColumn.FieldType); err != nil {
		// Cleanup on failure
		os.Remove(filePath)
		if _, dropErr := catalogMgr.DropIndex(p.transactionCtx, indexName); dropErr != nil {
			fmt.Printf("Warning: failed to cleanup catalog entry: %v\n", dropErr)
		}
		return fmt.Errorf("failed to populate index: %v", err)
	}

	return nil
}

// createPhysicalBTreeIndex creates the BTree index file on disk.
func (p *CreateTablePlan) createPhysicalBTreeIndex(filePath string, keyType types.Type) error {
	// Ensure the directory exists
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	btreeFile, err := btree.NewBTreeFile(filePath, keyType)
	if err != nil {
		return fmt.Errorf("failed to create btree file: %v", err)
	}

	return btreeFile.Close()
}

// populateBTreeIndex scans the table and inserts all tuples into the BTree index.
func (p *CreateTablePlan) populateBTreeIndex(
	filePath string,
	indexID,
	tableID,
	columnIndex int,
	keyType types.Type,
) error {
	tableFile, err := p.ctx.CatalogManager().GetTableFile(tableID)
	if err != nil {
		return fmt.Errorf("failed to get table file: %v", err)
	}

	heapFile, ok := tableFile.(*heap.HeapFile)
	if !ok {
		return fmt.Errorf("expected heap file, got %T", tableFile)
	}

	// Open the BTree index
	btreeFile, err := btree.NewBTreeFile(filePath, keyType)
	if err != nil {
		return fmt.Errorf("failed to open btree file: %v", err)
	}
	defer btreeFile.Close()

	btreeIndex := btreeindex.NewBTree(indexID, keyType, btreeFile, p.transactionCtx, p.ctx.PageStore())
	defer btreeIndex.Close()

	// Scan the table and insert into index
	seqScan, err := query.NewSeqScan(p.transactionCtx, tableID, heapFile, p.ctx.PageStore())
	if err != nil {
		return fmt.Errorf("failed to create sequential scan: %v", err)
	}
	seqScan.Open()
	defer seqScan.Close()

	return iterator.ForEach(seqScan, func(t *tuple.Tuple) error {
		key, err := t.GetField(columnIndex)
		if err != nil {
			return fmt.Errorf("failed to get field at index %d: %v", columnIndex, err)
		}

		// Skip null keys
		if key == nil {
			return nil
		}

		if t.RecordID == nil {
			return fmt.Errorf("tuple missing record ID")
		}

		if err := btreeIndex.Insert(key, t.RecordID); err != nil {
			return fmt.Errorf("failed to insert key into index: %v", err)
		}
		return nil
	})
}
