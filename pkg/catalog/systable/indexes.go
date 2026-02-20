package systable

import (
	"fmt"
	"storemy/pkg/catalog/catalogio"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/tuple"
	"strings"
	"time"
)

// IndexMetadata represents metadata for a database index
type IndexMetadata struct {
	IndexID    primitives.FileID
	IndexName  string
	TableID    primitives.FileID
	ColumnName string
	IndexType  index.IndexType
	FilePath   primitives.Filepath
	CreatedAt  time.Time
}

type IndexesTable struct {
	*BaseOperations[IndexMetadata]
}

// NewIndexOperations creates a new IndexesTable instance.
func NewIndexOperations(access catalogio.CatalogAccess, id primitives.FileID) *IndexesTable {
	return &IndexesTable{
		BaseOperations: NewBaseOperations(access, id, IndexesTableDescriptor),
	}
}

// GetID retrieves the index ID from a tuple
func (it *IndexesTable) GetID(t *tuple.Tuple) (primitives.FileID, error) {
	expectedFields := 7
	if int(t.NumFields()) != expectedFields {
		return 0, fmt.Errorf("invalid tuple: expected %d fields, got %d", expectedFields, t.TupleDesc.NumFields())
	}
	return primitives.FileID(getUint64Field(t, 0)), nil
}

// GetIndexesByTable retrieves all indexes for a given table from CATALOG_INDEXES.
func (io *IndexesTable) GetIndexesByTable(tx *transaction.TransactionContext, tableID primitives.FileID) ([]IndexMetadata, error) {
	return io.FindAll(tx, func(im IndexMetadata) bool {
		return im.TableID == tableID
	})
}

// GetIndexByName retrieves index metadata from CATALOG_INDEXES by index name.
// Index name matching is case-insensitive.
func (io *IndexesTable) GetIndexByName(tx *transaction.TransactionContext, indexName string) (IndexMetadata, error) {
	return io.FindOne(tx, func(im IndexMetadata) bool {
		return strings.EqualFold(im.IndexName, indexName)
	})
}

// GetIndexByID retrieves index metadata from CATALOG_INDEXES by index ID.
func (io *IndexesTable) GetIndexByID(tx *transaction.TransactionContext, indexID primitives.FileID) (IndexMetadata, error) {
	return io.FindOne(tx, func(im IndexMetadata) bool {
		return im.IndexID == indexID
	})
}

// DeleteIndexFromCatalog removes an index entry from CATALOG_INDEXES.
func (io *IndexesTable) DeleteIndexFromCatalog(tx *transaction.TransactionContext, indexID primitives.FileID) error {
	if err := io.DeleteBy(tx, func(im IndexMetadata) bool {
		return im.IndexID == indexID
	}); err != nil {
		return fmt.Errorf("failed to delete index: %w", err)
	}
	return nil
}
