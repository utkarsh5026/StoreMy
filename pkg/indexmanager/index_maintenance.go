package indexmanager

import (
	"fmt"
	btreeindex "storemy/pkg/memory/wrappers/btree_index"
	hashindex "storemy/pkg/memory/wrappers/hash_index"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// operationType represents the type of operation being performed on an index.
// It is used internally to distinguish between different DML operations
// and route them to the appropriate index methods.
type operationType int

const (
	insertOp operationType = iota
	deleteOp
)

// String returns a human-readable string representation of the operation type.
// This is useful for error messages and logging.
func (op operationType) String() string {
	switch op {
	case insertOp:
		return "insert"
	case deleteOp:
		return "delete"
	default:
		return "unknown"
	}
}

// ProcessIndexOperation performs a specified operation (insert or delete) on all indexes
// associated with a table for a given tuple. This is the core method that coordinates
// index maintenance across all index types.
//
// The method validates the tuple, retrieves all indexes for the table, extracts the
// appropriate key values, and applies the operation to each index. If any operation
// fails, an error is returned immediately without attempting further operations.
//
// Parameters:
//   - ctx: Transaction context for the operation
//   - tableID: The ID of the table whose indexes should be updated
//   - t: The tuple to process. Must be non-nil with a valid RecordID
//   - opType: The type of operation to perform (insertOp or deleteOp)
//
// Returns:
//   - error: nil if all operations succeed, or an error describing the first failure
func (im *IndexManager) processIndexOperation(ctx TxCtx, tableID primitives.TableID, t *tuple.Tuple, opType operationType) error {
	if t == nil || t.TableNotAssigned() {
		return fmt.Errorf("tuple must be non-nil and have a RecordID")
	}

	indexes, err := im.getIndexesForTable(ctx, tableID)
	if err != nil {
		return fmt.Errorf("failed to get indexes for table %d: %v", tableID, err)
	}

	if len(indexes) == 0 {
		return nil
	}

	for _, idx := range indexes {
		key, err := extractKey(t, idx.metadata)
		indexName := idx.metadata.IndexName
		if err != nil {
			return fmt.Errorf("failed to extract key for index %s: %v", indexName, err)
		}

		var opErr error
		switch opType {
		case insertOp:
			opErr = applyIndexOperation(idx, key, t.RecordID, insertOp)
		case deleteOp:
			opErr = applyIndexOperation(idx, key, t.RecordID, deleteOp)
		}

		if opErr != nil {
			return fmt.Errorf("failed to %s index %s: %v", opType.String(), indexName, opErr)
		}
	}

	return nil
}

// applyIndexOperation applies a specific operation to a single index.
// This function handles the type-specific logic for different index implementations
// (B-Tree and Hash indexes) and delegates to their respective Insert or Delete methods.
//
// Parameters:
//   - idx: The index to operate on, wrapped with its metadata
//   - key: The key value extracted from the tuple
//   - rid: The RecordID of the tuple being operated on
//   - opType: The type of operation to perform (insertOp or deleteOp)
//
// Returns:
//   - error: nil if the operation succeeds, or an error if:
//   - The index type is not supported
//   - The underlying index operation fails
func applyIndexOperation(idx *indexWithMetadata, key types.Field, rid *tuple.TupleRecordID, opType operationType) error {
	switch v := idx.index.(type) {
	case *btreeindex.BTree:
		if opType == insertOp {
			return v.Insert(key, rid)
		}
		return v.Delete(key, rid)
	case *hashindex.HashIndex:
		if opType == insertOp {
			return v.Insert(key, rid)
		}
		return v.Delete(key, rid)
	default:
		return fmt.Errorf("unsupported index type for %s", idx.metadata.IndexName)
	}
}

// extractKey extracts the indexed column value from a tuple.
// This method retrieves the field at the column position specified in the
// index metadata and validates that it matches the expected type.
//
// Parameters:
//   - t: The tuple from which to extract the key
//   - metadata: Index metadata containing the column index and expected type
//
// Returns:
//   - types.Field: The extracted field value if successful
//   - error: nil if extraction succeeds, or an error if:
//   - The column index is out of bounds
//   - The field cannot be retrieved
//   - The field type doesn't match the index's expected key type
func extractKey(t *tuple.Tuple, metadata *IndexMetadata) (types.Field, error) {
	colIndex := metadata.ColumnIndex
	if colIndex < 0 || colIndex >= t.TupleDesc.NumFields() {
		return nil, fmt.Errorf("invalid column index %d for tuple with %d fields",
			colIndex, t.TupleDesc.NumFields())
	}

	field, err := t.GetField(colIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to get field at index %d: %v", colIndex, err)
	}

	if field.Type() != metadata.KeyType {
		return nil, fmt.Errorf("field type mismatch: expected %v, got %v",
			metadata.KeyType, field.Type())
	}

	return field, nil
}

// OnInsert maintains all indexes for a table when a new tuple is inserted.
// This method extracts the indexed column values from the tuple and inserts
// them into all applicable indexes for the table.
//
// The operation is atomic across all indexes - if any index insertion fails,
// an error is returned. However, partial insertions may have occurred before
// the failure, so callers should handle rollback at a higher level.
//
// Parameters:
//   - ctx: Transaction context for the operation
//   - tableID: The ID of the table where the tuple is being inserted
//   - t: The tuple being inserted. Must be non-nil and have a valid RecordID
//
// Returns:
//   - error: nil if all index insertions succeed, or an error describing the failure
func (im *IndexManager) OnInsert(ctx TxCtx, tableID primitives.TableID, t *tuple.Tuple) error {
	return im.processIndexOperation(ctx, tableID, t, insertOp)
}

// OnDelete maintains all indexes for a table when a tuple is deleted.
// This method extracts the indexed column values from the tuple being deleted
// and removes the corresponding entries from all applicable indexes.
//
// The operation is atomic across all indexes - if any index deletion fails,
// an error is returned. Partial deletions may have occurred before the failure.
//
// Parameters:
//   - ctx: Transaction context for the operation
//   - tableID: The ID of the table where the tuple is being deleted
//   - t: The tuple being deleted. Must be non-nil and have a valid RecordID
//
// Returns:
//   - error: nil if all index deletions succeed, or an error describing the failure
func (im *IndexManager) OnDelete(ctx TxCtx, tableID primitives.TableID, t *tuple.Tuple) error {
	return im.processIndexOperation(ctx, tableID, t, deleteOp)
}

// OnUpdate maintains all indexes for a table when a tuple is updated.
// This is implemented as a delete-then-insert operation to ensure proper
// index maintenance when indexed column values change.
//
// If the insertion fails after deletion, the method attempts to re-insert
// the old tuple to maintain consistency, though this rollback may also fail.
// Callers should implement proper transaction rollback mechanisms.
//
// Parameters:
//   - ctx: Transaction context for the operation
//   - tableID: The ID of the table where the tuple is being updated
//   - old: The tuple before the update. Must be non-nil and have a valid RecordID
//   - new: The tuple after the update. Must be non-nil and have a valid RecordID
//
// Returns:
//   - error: nil if both deletion and insertion succeed, or an error describing the failure
func (im *IndexManager) OnUpdate(ctx TxCtx, tableID primitives.TableID, old, new *tuple.Tuple) error {
	if err := im.OnDelete(ctx, tableID, old); err != nil {
		return err
	}

	if err := im.OnInsert(ctx, tableID, new); err != nil {
		_ = im.OnInsert(ctx, tableID, old)
		return err
	}

	return nil
}
