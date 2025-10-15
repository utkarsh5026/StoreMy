package planner

import (
	"fmt"
	"os"
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	btreeindex "storemy/pkg/memory/wrappers/btree_index"
	hashindex "storemy/pkg/memory/wrappers/hash_index"
	"storemy/pkg/parser/statements"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/index/btree"
	"storemy/pkg/storage/index/hash"
	"storemy/pkg/tuple"
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
func (p *CreateIndexPlan) Execute() (any, error) {
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

	tableSchema, err := cm.GetTableSchema(p.tx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %v", err)
	}

	columnIndex := tableSchema.GetFieldIndex(colName)
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

	columnType := tableSchema.Columns[columnIndex].FieldType
	if err := p.createPhysicalIndex(filePath, columnType, idxType); err != nil {
		if _, dropErr := cm.DropIndex(p.tx, indexName); dropErr != nil {
			fmt.Printf("Warning: failed to cleanup catalog entry after index creation failure: %v\n", dropErr)
		}
		return nil, fmt.Errorf("failed to create physical index: %v", err)
	}

	if err := p.populateIndex(filePath, indexID, tableID, columnIndex, columnType, idxType); err != nil {
		os.Remove(filePath)
		if _, dropErr := cm.DropIndex(p.tx, indexName); dropErr != nil {
			fmt.Printf("Warning: failed to cleanup catalog entry after population failure: %v\n", dropErr)
		}
		return nil, fmt.Errorf("failed to populate index: %v", err)
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

// createPhysicalIndex creates the actual index file on disk based on the index type.
func (p *CreateIndexPlan) createPhysicalIndex(
	filePath string,
	keyType types.Type,
	indexType index.IndexType,
) error {
	switch indexType {
	case index.HashIndex:
		hashFile, err := hash.NewHashFile(filePath, keyType, hash.DefaultBuckets)
		if err != nil {
			return fmt.Errorf("failed to create hash index file: %v", err)
		}
		return hashFile.Close()

	case index.BTreeIndex:
		btreeFile, err := btree.NewBTreeFile(filePath, keyType)
		if err != nil {
			return fmt.Errorf("failed to create btree index file: %v", err)
		}
		return btreeFile.Close()

	default:
		return fmt.Errorf("unsupported index type: %s", indexType)
	}
}

// populateIndex scans the table and inserts all existing tuples into the index.
// This is called immediately after index creation to build the initial index state.
func (p *CreateIndexPlan) populateIndex(
	filePath string,
	indexID,
	tableID,
	columnIndex int,
	keyType types.Type,
	indexType index.IndexType,
) error {
	tableFile, err := p.ctx.CatalogManager().GetTableFile(tableID)
	if err != nil {
		return fmt.Errorf("failed to get table file: %v", err)
	}
	var insertFunc func(key types.Field, rid *tuple.TupleRecordID) error

	switch indexType {
	case index.HashIndex:
		hashFile, err := hash.NewHashFile(filePath, keyType, 100)
		if err != nil {
			return fmt.Errorf("failed to open hash index: %v", err)
		}
		defer hashFile.Close()

		hashIndex := hashindex.NewHashIndex(indexID, keyType, hashFile, p.ctx.PageStore())
		insertFunc = func(key types.Field, rid *tuple.TupleRecordID) error {
			return hashIndex.Insert(p.tx, key, rid)
		}

	case index.BTreeIndex:
		btreeFile, err := btree.NewBTreeFile(filePath, keyType)
		if err != nil {
			return fmt.Errorf("failed to open btree index: %v", err)
		}
		defer btreeFile.Close()

		btreeIndex := btreeindex.NewBTree(indexID, keyType, btreeFile, p.tx, p.ctx.PageStore())
		insertFunc = func(key types.Field, rid *tuple.TupleRecordID) error {
			return btreeIndex.Insert(key, rid)
		}

	default:
		return fmt.Errorf("unsupported index type: %s", indexType)
	}

	heapFile, ok := tableFile.(*heap.HeapFile)
	if !ok {
		return fmt.Errorf("expected heap file for table, got %T", tableFile)
	}

	return p.insertIntoIndex(p.tx, heapFile, columnIndex, tableID, insertFunc)
}

func (p *CreateIndexPlan) insertIntoIndex(tx TransactionCtx, tableFile *heap.HeapFile, heapTableID, indexColIdx int, insertFunc func(key types.Field, rid *tuple.TupleRecordID) error) error {
	it, err := query.NewSeqScan(tx, heapTableID, tableFile, p.ctx.PageStore())
	if err != nil {
		return fmt.Errorf("failed to create sequential scan: %v", err)
	}
	it.Open()
	defer it.Close()

	return iterator.ForEach(it, func(t *tuple.Tuple) error {
		key, err := t.GetField(indexColIdx)
		if err != nil {
			return fmt.Errorf("failed to get field at index %d: %v", indexColIdx, err)
		}

		if key == nil {
			return nil
		}

		if t.RecordID == nil {
			return fmt.Errorf("tuple missing record ID")
		}

		if err := insertFunc(key, t.RecordID); err != nil {
			return fmt.Errorf("failed to insert key into index: %v", err)
		}
		return nil
	})
}
