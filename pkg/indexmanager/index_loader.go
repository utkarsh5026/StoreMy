// Package indexmanager provides functionality for managing database indexes.
// This file contains the indexLoader component responsible for loading index metadata
// from the catalog and opening index files.
package indexmanager

import (
	"fmt"
	"os"
	"slices"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/memory"
	btreeindex "storemy/pkg/memory/wrappers/btree_index"
	hashindex "storemy/pkg/memory/wrappers/hash_index"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/index/btree"
	"storemy/pkg/types"
)

type IndexLoader struct {
	store   *memory.PageStore
	tx      *transaction.TransactionContext
	catalog CatalogReader
}

// loadAndOpenIndexes loads index metadata from catalog and opens all index files for a table.
// This is the main entry point for initializing all indexes associated with a table.
// It performs the following steps:
//  1. Loads index metadata from the catalog
//  2. Opens each index file
//  3. Returns a slice of indexes with their metadata
//
// If an index file fails to open, a warning is printed but the process continues
// with the remaining indexes.
//
// Parameters:
//   - ctx: Transaction context for catalog operations
//   - tableID: The ID of the table whose indexes should be loaded
//
// Returns:
//   - A slice of indexWithMetadata containing successfully opened indexes
//   - An error if catalog access fails
func (il *IndexLoader) LoadIndexes(tableID primitives.FileID) ([]*IndexWithMetadata, error) {
	metadataList, err := il.loadFromCatalog(tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get indexes from catalog: %v", err)
	}

	indexes := make([]*IndexWithMetadata, 0, len(metadataList))
	for _, metadata := range metadataList {
		idx, err := il.openIndex(metadata)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to open index %s: %v\n", metadata.IndexName, err)
			continue
		}
		indexes = append(indexes, &IndexWithMetadata{
			index:    idx,
			metadata: metadata,
		})
	}

	return indexes, nil
}

func (il *IndexLoader) LoadIndexForCol(colID primitives.ColumnID, tableID primitives.FileID) (index.Index, error) {
	indexes, err := il.loadFromCatalog(tableID)
	if err != nil {
		return nil, err
	}

	colIdx := slices.IndexFunc(indexes, func(i *IndexMetadata) bool {
		return i.ColumnIndex == colID
	})
	if colIdx == -1 {
		return nil, fmt.Errorf("col %d not found", colID)
	}

	return il.openIndex(indexes[colIdx])
}

// loadFromCatalog loads raw index info from catalog and resolves it
// with schema information (ColumnIndex, KeyType) to create full IndexMetadata.
// This method enriches the catalog metadata with schema-specific information
// needed for index operations.
//
// Parameters:
//   - tableID: The ID of the table whose index metadata should be loaded
//
// Returns:
//   - A slice of complete IndexMetadata with resolved schema information
//   - An error if catalog access or schema retrieval fails
func (il *IndexLoader) loadFromCatalog(tableID primitives.FileID) ([]*IndexMetadata, error) {
	catalogIndexes, err := il.catalog.GetIndexesByTable(il.tx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get indexes from catalog: %v", err)
	}

	if len(catalogIndexes) == 0 {
		return []*IndexMetadata{}, nil
	}

	schema, err := il.catalog.GetTableSchema(tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %v", err)
	}

	return resolveIndexMetadata(catalogIndexes, schema), nil
}

// resolveIndexMetadata resolves catalog metadata with schema to create complete IndexMetadata.
// It matches column names from the catalog with actual column positions and types
// from the table schema.
//
// If a column referenced by an index is not found in the schema, a warning is printed
// and that index is skipped.
//
// Parameters:
//   - catalogIndexes: Raw index metadata from the catalog
//   - schema: The table schema containing column information
//
// Returns a slice of IndexMetadata with resolved column indices and key types.
func resolveIndexMetadata(catalogIndexes []*systemtable.IndexMetadata, schema *schema.Schema) []*IndexMetadata {
	result := make([]*IndexMetadata, 0, len(catalogIndexes))

	for _, catIdx := range catalogIndexes {
		columnIndex, keyType, err := findColumnInfo(schema, catIdx.ColumnName)

		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: column %s not found in schema for index %s\n",
				catIdx.ColumnName, catIdx.IndexName)
			continue
		}

		result = append(result, &IndexMetadata{
			IndexMetadata: *catIdx,
			ColumnIndex:   columnIndex,
			KeyType:       keyType,
		})
	}

	return result
}

// findColumnInfo finds the column index and type in the schema.
// This helper function searches for a column by name in the table schema.
//
// Parameters:
//   - schema: The table schema to search
//   - columnName: The name of the column to find
//
// Returns:
//   - The zero-based index of the column in the schema
//   - The data type of the column
//   - Returns (-1, IntType) if the column is not found
func findColumnInfo(schema *schema.Schema, columnName string) (primitives.ColumnID, types.Type, error) {
	var i primitives.ColumnID
	for i = 0; i < schema.TupleDesc.NumFields(); i++ {
		fieldName, _ := schema.TupleDesc.GetFieldName(i)
		if fieldName == columnName {
			return i, schema.TupleDesc.Types[i], nil
		}
	}
	return 0, types.IntType, fmt.Errorf("column %s not found", columnName)
}

// openIndex opens an index file based on its metadata.
// This method dispatches to the appropriate index type handler based on
// the IndexType specified in the metadata.
//
// Parameters:
//   - ctx: Transaction context for index operations
//   - m: The complete index metadata including file path and index type
//
// Returns:
//   - The opened index instance
//   - An error if the index type is unsupported or opening fails
func (il *IndexLoader) openIndex(m *IndexMetadata) (index.Index, error) {
	switch m.IndexType {
	case index.BTreeIndex:
		return il.openBTreeIndex(m)
	case index.HashIndex:
		return il.openHashIndex(m)
	default:
		return nil, fmt.Errorf("unsupported index type: %s", m.IndexType)
	}
}

// openBTreeIndex opens a B+Tree index file.
// B+Tree indexes provide efficient range queries and ordered traversal.
//
// Parameters:
//   - m: Index metadata containing file path and key type
//
// Returns:
//   - A BTree index wrapper ready for use
//   - An error if the file cannot be opened or initialized
func (il *IndexLoader) openBTreeIndex(m *IndexMetadata) (*btreeindex.BTree, error) {
	file, err := btree.NewBTreeFile(m.FilePath, m.KeyType)
	if err != nil {
		return nil, fmt.Errorf("failed to open BTree file: %v", err)
	}

	btreeIdx := btreeindex.NewBTree(m.IndexID, m.KeyType, file, il.tx, il.store)
	return btreeIdx, nil
}

// openHashIndex opens a hash index file.
// Hash indexes provide fast equality lookups but do not support range queries.
//
// Parameters:
//   - ctx: Transaction context for index operations
//   - m: Index metadata containing file path and key type
//
// Returns:
//   - A HashIndex wrapper ready for use
//   - An error if the file cannot be opened or initialized
func (il *IndexLoader) openHashIndex(m *IndexMetadata) (*hashindex.HashIndex, error) {
	file, err := index.NewHashFile(m.FilePath, m.KeyType, index.DefaultBuckets)
	if err != nil {
		return nil, fmt.Errorf("failed to open hash file: %v", err)
	}

	hashIdx := hashindex.NewHashIndex(m.IndexID, m.KeyType, file, il.store, il.tx)
	return hashIdx, nil
}
