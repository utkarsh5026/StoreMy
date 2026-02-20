package systable

import (
	"storemy/pkg/catalog/schema"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// mockCatalogAccess implements both CatalogReader and CatalogWriter for testing
type mockCatalogAccess struct {
	tuples map[primitives.FileID][]*tuple.Tuple // tableID -> tuples
}

func newMockCatalogAccess() *mockCatalogAccess {
	return &mockCatalogAccess{
		tuples: make(map[primitives.FileID][]*tuple.Tuple),
	}
}

func (m *mockCatalogAccess) IterateTable(tableID primitives.FileID, tx TxContext, fn func(*tuple.Tuple) error) error {
	for _, t := range m.tuples[tableID] {
		if err := fn(t); err != nil {
			return err
		}
	}
	return nil
}

func (m *mockCatalogAccess) InsertRow(tableID primitives.FileID, tx TxContext, t *tuple.Tuple) error {
	m.tuples[tableID] = append(m.tuples[tableID], t)
	return nil
}

func (m *mockCatalogAccess) DeleteRow(tableID primitives.FileID, tx TxContext, t *tuple.Tuple) error {
	tuples := m.tuples[tableID]
	for i, existing := range tuples {
		if tuplesEqual(existing, t) {
			m.tuples[tableID] = append(tuples[:i], tuples[i+1:]...)
			return nil
		}
	}
	return nil
}

func tuplesEqual(t1, t2 *tuple.Tuple) bool {
	numFields := t1.TupleDesc.NumFields()
	if numFields != t2.TupleDesc.NumFields() {
		return false
	}

	var i primitives.ColumnID
	for i = 0; i < numFields; i++ {
		f1, _ := t1.GetField(i)
		f2, _ := t2.GetField(i)
		if f1.Type() != f2.Type() {
			return false
		}
		// Simple comparison - in real implementation would compare values
		if f1.String() != f2.String() {
			return false
		}
	}
	return true
}

// createColumnMetadata creates a column metadata tuple for testing with all options
func createColumnMetadata(tableID primitives.FileID, name string, position primitives.ColumnID, fieldType types.Type, isPrimary, isAutoInc bool, nextAutoValue uint64) *tuple.Tuple {
	col := schema.ColumnMetadata{
		TableID:       tableID,
		Name:          name,
		Position:      position,
		FieldType:     fieldType,
		IsPrimary:     isPrimary,
		IsAutoInc:     isAutoInc,
		NextAutoValue: nextAutoValue,
	}
	return ColumnsTableDescriptor.CreateTuple(col)
}

// setupTableColumns adds column metadata to the mock for a table
func setupTableColumns(mock *mockCatalogAccess, columnsTableID primitives.FileID, tableID primitives.FileID, columns []struct {
	name      string
	fieldType types.Type
	isPrimary bool
}) {
	for i, col := range columns {
		colTuple := createColumnMetadata(tableID, col.name, primitives.ColumnID(i), col.fieldType, col.isPrimary, false, 0)
		mock.tuples[columnsTableID] = append(mock.tuples[columnsTableID], colTuple)
	}
}

func fid(id int) primitives.FileID {
	return primitives.FileID(id)
}

// createColumnTuple creates a column tuple for testing
func createColumnTuple(tableID primitives.FileID, name string, position primitives.ColumnID, fieldType types.Type, isAutoInc bool, nextAutoValue uint64) *tuple.Tuple {
	col := schema.ColumnMetadata{
		TableID:       tableID,
		Name:          name,
		Position:      position,
		FieldType:     fieldType,
		IsAutoInc:     isAutoInc,
		NextAutoValue: nextAutoValue,
	}

	return ColumnsTableDescriptor.CreateTuple(col)
}

// Suppress unused import warning
var _ = (*transaction.TransactionContext)(nil)
