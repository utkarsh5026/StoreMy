package indexmanager

import (
	"fmt"
	"os"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/memory"
	btreeindex "storemy/pkg/memory/wrappers/btree_index"
	hashindex "storemy/pkg/memory/wrappers/hash_index"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/index/btree"
	"storemy/pkg/storage/index/hash"
	"storemy/pkg/types"
)

// indexLoader handles loading index metadata from catalog and opening index files.
type indexLoader struct {
	catalog   CatalogReader
	pageStore *memory.PageStore
}

// newIndexLoader creates a new index loader.
func newIndexLoader(catalog CatalogReader, pageStore *memory.PageStore) *indexLoader {
	return &indexLoader{
		catalog:   catalog,
		pageStore: pageStore,
	}
}

// loadAndOpenIndexes loads index metadata from catalog and opens all index files for a table.
func (il *indexLoader) loadAndOpenIndexes(ctx TxCtx, tableID int) ([]*indexWithMetadata, error) {
	metadataList, err := il.loadFromCatalog(ctx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get indexes from catalog: %v", err)
	}

	indexes := make([]*indexWithMetadata, 0, len(metadataList))
	for _, metadata := range metadataList {
		idx, err := il.openIndex(ctx, metadata)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to open index %s: %v\n", metadata.IndexName, err)
			continue
		}
		indexes = append(indexes, &indexWithMetadata{
			index:    idx,
			metadata: metadata,
		})
	}

	return indexes, nil
}

// loadIndexMetadataFromCatalog loads raw index info from catalog and resolves it
// with schema information (ColumnIndex, KeyType) to create full IndexMetadata.
func (il *indexLoader) loadFromCatalog(ctx TxCtx, tableID int) ([]*IndexMetadata, error) {
	catalogIndexes, err := il.catalog.GetIndexesByTable(ctx, tableID)
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
func resolveIndexMetadata(
	catalogIndexes []*systemtable.IndexMetadata,
	schema *schema.Schema,
) []*IndexMetadata {
	result := make([]*IndexMetadata, 0, len(catalogIndexes))

	for _, catIdx := range catalogIndexes {
		columnIndex, keyType := findColumnInfo(schema, catIdx.ColumnName)

		if columnIndex == -1 {
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
func findColumnInfo(schema *schema.Schema, columnName string) (int, types.Type) {
	for i := 0; i < schema.TupleDesc.NumFields(); i++ {
		fieldName, _ := schema.TupleDesc.GetFieldName(i)
		if fieldName == columnName {
			return i, schema.TupleDesc.Types[i]
		}
	}
	return -1, types.IntType
}

// openIndex opens an index file based on its metadata.
func (il *indexLoader) openIndex(ctx TxCtx, m *IndexMetadata) (index.Index, error) {
	switch m.IndexType {
	case index.BTreeIndex:
		return il.openBTreeIndex(ctx, m)
	case index.HashIndex:
		return il.openHashIndex(ctx, m)
	default:
		return nil, fmt.Errorf("unsupported index type: %s", m.IndexType)
	}
}

// openBTreeIndex opens a B+Tree index file.
func (il *indexLoader) openBTreeIndex(ctx TxCtx, m *IndexMetadata) (*btreeindex.BTree, error) {
	file, err := btree.NewBTreeFile(m.FilePath, m.KeyType)
	if err != nil {
		return nil, fmt.Errorf("failed to open BTree file: %v", err)
	}

	btreeIdx := btreeindex.NewBTree(m.IndexID, m.KeyType, file, ctx, il.pageStore)
	return btreeIdx, nil
}

// openHashIndex opens a hash index file.
func (il *indexLoader) openHashIndex(ctx TxCtx, m *IndexMetadata) (*hashindex.HashIndex, error) {
	file, err := hash.NewHashFile(m.FilePath, m.KeyType, hash.DefaultBuckets)
	if err != nil {
		return nil, fmt.Errorf("failed to open hash file: %v", err)
	}

	hashIdx := hashindex.NewHashIndex(m.IndexID, m.KeyType, file, il.pageStore, ctx)
	return hashIdx, nil
}

// loadAndOpenIndexes is a convenience method on IndexManager that delegates to the loader.
func (im *IndexManager) loadAndOpenIndexes(ctx TxCtx, tableID int) ([]*indexWithMetadata, error) {
	return im.loader.loadAndOpenIndexes(ctx, tableID)
}
