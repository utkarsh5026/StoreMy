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
	metadataList, err := il.loadIndexMetadataFromCatalog(ctx, tableID)
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
func (il *indexLoader) loadIndexMetadataFromCatalog(ctx TxCtx, tableID int) ([]*IndexMetadata, error) {
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

	return il.resolveIndexMetadata(catalogIndexes, schema), nil
}

// resolveIndexMetadata resolves catalog metadata with schema to create complete IndexMetadata.
func (il *indexLoader) resolveIndexMetadata(
	catalogIndexes []*systemtable.IndexMetadata,
	schema *schema.Schema,
) []*IndexMetadata {
	result := make([]*IndexMetadata, 0, len(catalogIndexes))

	for _, catIdx := range catalogIndexes {
		columnIndex, keyType := il.findColumnInfo(schema, catIdx.ColumnName)

		if columnIndex == -1 {
			fmt.Fprintf(os.Stderr, "Warning: column %s not found in schema for index %s\n",
				catIdx.ColumnName, catIdx.IndexName)
			continue
		}

		result = append(result, &IndexMetadata{
			IndexID:     catIdx.IndexID,
			IndexName:   catIdx.IndexName,
			TableID:     catIdx.TableID,
			ColumnName:  catIdx.ColumnName,
			IndexType:   catIdx.IndexType,
			FilePath:    catIdx.FilePath,
			CreatedAt:   catIdx.CreatedAt,
			ColumnIndex: columnIndex,
			KeyType:     keyType,
		})
	}

	return result
}

// findColumnInfo finds the column index and type in the schema.
func (il *indexLoader) findColumnInfo(schema *schema.Schema, columnName string) (int, types.Type) {
	for i := 0; i < schema.TupleDesc.NumFields(); i++ {
		fieldName, _ := schema.TupleDesc.GetFieldName(i)
		if fieldName == columnName {
			return i, schema.TupleDesc.Types[i]
		}
	}
	return -1, types.IntType
}

// openIndex opens an index file based on its metadata.
func (il *indexLoader) openIndex(
	ctx TxCtx,
	metadata *IndexMetadata,
) (index.Index, error) {
	switch metadata.IndexType {
	case index.BTreeIndex:
		return il.openBTreeIndex(ctx, metadata)
	case index.HashIndex:
		return il.openHashIndex(ctx, metadata)
	default:
		return nil, fmt.Errorf("unsupported index type: %s", metadata.IndexType)
	}
}

// openBTreeIndex opens a B+Tree index file.
func (il *indexLoader) openBTreeIndex(
	ctx TxCtx,
	metadata *IndexMetadata,
) (*btreeindex.BTree, error) {
	file, err := btree.NewBTreeFile(metadata.FilePath, metadata.KeyType)
	if err != nil {
		return nil, fmt.Errorf("failed to open BTree file: %v", err)
	}

	btreeIdx := btreeindex.NewBTree(metadata.IndexID, metadata.KeyType, file, ctx, il.pageStore)
	return btreeIdx, nil
}

// openHashIndex opens a hash index file.
func (il *indexLoader) openHashIndex(ctx TxCtx, metadata *IndexMetadata) (*hashindex.HashIndex, error) {
	numBuckets := hash.DefaultBuckets

	file, err := hash.NewHashFile(metadata.FilePath, metadata.KeyType, numBuckets)
	if err != nil {
		return nil, fmt.Errorf("failed to open hash file: %v", err)
	}

	// Register the file with the PageStore
	il.pageStore.RegisterDbFile(metadata.IndexID, file)

	hashIdx := hashindex.NewHashIndex(metadata.IndexID, metadata.KeyType, file, il.pageStore, ctx)
	return hashIdx, nil
}

// loadAndOpenIndexes is a convenience method on IndexManager that delegates to the loader.
func (im *IndexManager) loadAndOpenIndexes(ctx TxCtx, tableID int) ([]*indexWithMetadata, error) {
	return im.loader.loadAndOpenIndexes(ctx, tableID)
}
