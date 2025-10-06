package planner

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/registry"
	"storemy/pkg/tuple"
)

// TableMetadata holds resolved table information
type tableMetadata struct {
	TableID   int
	TupleDesc *tuple.TupleDescription
}

// resolveTableMetadata gets table ID and schema in one call
func resolveTableMetadata(tableName string, ctx *registry.DatabaseContext) (*tableMetadata, error) {
	tableID, err := ctx.TableManager().GetTableID(tableName)
	if err != nil {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	tupleDesc, err := ctx.TableManager().GetTupleDesc(tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema for table %s: %v", tableName, err)
	}

	return &tableMetadata{
		TableID:   tableID,
		TupleDesc: tupleDesc,
	}, nil
}

// FindFieldIndex locates a field by name in the tuple descriptor
func findFieldIndex(fieldName string, tupleDesc *tuple.TupleDescription) (int, error) {
	for i := 0; i < tupleDesc.NumFields(); i++ {
		name, _ := tupleDesc.GetFieldName(i)
		if name == fieldName {
			return i, nil
		}
	}
	return -1, fmt.Errorf("column %s not found", fieldName)
}

// CollectAllTuples executes an iterator and collects all results
func collectAllTuples(it iterator.DbIterator) ([]*tuple.Tuple, error) {
	if err := it.Open(); err != nil {
		return nil, fmt.Errorf("failed to open iterator: %v", err)
	}
	defer it.Close()

	var tuples []*tuple.Tuple

	for {
		hasNext, err := it.HasNext()
		if err != nil {
			return nil, fmt.Errorf("error during iteration: %v", err)
		}

		if !hasNext {
			break
		}

		t, err := it.Next()
		if err != nil {
			return nil, fmt.Errorf("error fetching tuple: %v", err)
		}

		tuples = append(tuples, t)
	}

	return tuples, nil
}
