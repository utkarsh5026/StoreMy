package indexmanager

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	btreeindex "storemy/pkg/memory/wrappers/btree_index"
	hashindex "storemy/pkg/memory/wrappers/hash_index"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// indexMaintenance handles DML operations (insert, delete, update) on indexes.
type indexMaintenance struct {
	getIndexes func(*transaction.TransactionContext, int) ([]*indexWithMetadata, error)
}

// newIndexMaintenance creates a new index maintenance handler.
func newIndexMaintenance(
	getIndexes func(*transaction.TransactionContext, int) ([]*indexWithMetadata, error),
) *indexMaintenance {
	return &indexMaintenance{
		getIndexes: getIndexes,
	}
}

// OnInsert maintains all indexes for a table when a tuple is inserted.
func (im *indexMaintenance) OnInsert(
	ctx *transaction.TransactionContext,
	tableID int,
	t *tuple.Tuple,
) error {
	if t == nil || t.RecordID == nil {
		return fmt.Errorf("tuple must be non-nil and have a RecordID")
	}

	indexes, err := im.getIndexes(ctx, tableID)
	if err != nil {
		return fmt.Errorf("failed to get indexes for table %d: %v", tableID, err)
	}

	if len(indexes) == 0 {
		return nil
	}

	for _, idxWithMeta := range indexes {
		key, err := extractKey(t, idxWithMeta.metadata)
		if err != nil {
			return fmt.Errorf("failed to extract key for index %s: %v", idxWithMeta.metadata.IndexName, err)
		}

		if err := im.insertIntoIndex(idxWithMeta, key, t.RecordID); err != nil {
			return fmt.Errorf("failed to insert into index %s: %v", idxWithMeta.metadata.IndexName, err)
		}
	}

	return nil
}

// OnDelete maintains all indexes for a table when a tuple is deleted.
func (im *indexMaintenance) OnDelete(
	ctx *transaction.TransactionContext,
	tableID int,
	t *tuple.Tuple,
) error {
	if t == nil || t.RecordID == nil {
		return fmt.Errorf("tuple must be non-nil and have a RecordID")
	}

	indexes, err := im.getIndexes(ctx, tableID)
	if err != nil {
		return fmt.Errorf("failed to get indexes for table %d: %v", tableID, err)
	}

	if len(indexes) == 0 {
		return nil
	}

	for _, idxWithMeta := range indexes {
		key, err := extractKey(t, idxWithMeta.metadata)
		if err != nil {
			return fmt.Errorf("failed to extract key for index %s: %v", idxWithMeta.metadata.IndexName, err)
		}

		if err := im.deleteFromIndex(idxWithMeta, key, t.RecordID); err != nil {
			return fmt.Errorf("failed to delete from index %s: %v", idxWithMeta.metadata.IndexName, err)
		}
	}

	return nil
}

// OnUpdate maintains all indexes for a table when a tuple is updated.
func (im *indexMaintenance) OnUpdate(
	ctx *transaction.TransactionContext,
	tableID int,
	oldTuple *tuple.Tuple,
	newTuple *tuple.Tuple,
) error {
	if err := im.OnDelete(ctx, tableID, oldTuple); err != nil {
		return err
	}

	if err := im.OnInsert(ctx, tableID, newTuple); err != nil {
		// Rollback: re-insert old tuple
		_ = im.OnInsert(ctx, tableID, oldTuple)
		return err
	}

	return nil
}

// insertIntoIndex inserts a key-value pair into the appropriate index type.
func (im *indexMaintenance) insertIntoIndex(
	idxWithMeta *indexWithMetadata,
	key types.Field,
	rid *tuple.TupleRecordID,
) error {
	if btreeIdx, ok := idxWithMeta.index.(*btreeindex.BTree); ok {
		return btreeIdx.Insert(key, rid)
	} else if hashIdx, ok := idxWithMeta.index.(*hashindex.HashIndex); ok {
		return hashIdx.Insert(key, rid)
	}
	return fmt.Errorf("unsupported index type for %s", idxWithMeta.metadata.IndexName)
}

// deleteFromIndex deletes a key-value pair from the appropriate index type.
func (im *indexMaintenance) deleteFromIndex(
	idxWithMeta *indexWithMetadata,
	key types.Field,
	rid *tuple.TupleRecordID,
) error {
	if btreeIdx, ok := idxWithMeta.index.(*btreeindex.BTree); ok {
		return btreeIdx.Delete(key, rid)
	} else if hashIdx, ok := idxWithMeta.index.(*hashindex.HashIndex); ok {
		return hashIdx.Delete(key, rid)
	}
	return fmt.Errorf("unsupported index type for %s", idxWithMeta.metadata.IndexName)
}

// extractKey extracts the indexed column value from a tuple.
func extractKey(t *tuple.Tuple, metadata *IndexMetadata) (types.Field, error) {
	if metadata.ColumnIndex < 0 || metadata.ColumnIndex >= t.TupleDesc.NumFields() {
		return nil, fmt.Errorf("invalid column index %d for tuple with %d fields",
			metadata.ColumnIndex, t.TupleDesc.NumFields())
	}

	field, err := t.GetField(metadata.ColumnIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to get field at index %d: %v", metadata.ColumnIndex, err)
	}

	if field.Type() != metadata.KeyType {
		return nil, fmt.Errorf("field type mismatch: expected %v, got %v",
			metadata.KeyType, field.Type())
	}

	return field, nil
}

// OnInsert is a convenience method on IndexManager that delegates to maintenance.
func (im *IndexManager) OnInsert(
	ctx *transaction.TransactionContext,
	tableID int,
	t *tuple.Tuple,
) error {
	return im.maintenance.OnInsert(ctx, tableID, t)
}

// OnDelete is a convenience method on IndexManager that delegates to maintenance.
func (im *IndexManager) OnDelete(
	ctx *transaction.TransactionContext,
	tableID int,
	t *tuple.Tuple,
) error {
	return im.maintenance.OnDelete(ctx, tableID, t)
}

// OnUpdate is a convenience method on IndexManager that delegates to maintenance.
func (im *IndexManager) OnUpdate(
	ctx *transaction.TransactionContext,
	tableID int,
	oldTuple *tuple.Tuple,
	newTuple *tuple.Tuple,
) error {
	return im.maintenance.OnUpdate(ctx, tableID, oldTuple, newTuple)
}
