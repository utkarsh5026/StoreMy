package schema

import (
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/types"
)

// ColumnMetadata represents comprehensive metadata for a single column in a table schema.
type ColumnMetadata struct {
	Name          string              // Column name
	FieldType     types.Type          // Column data type
	Position      primitives.ColumnID // Column position in tuple (0-indexed)
	IsPrimary     bool                // Whether this is the primary key column
	IsAutoInc     bool                // Whether this column auto-increments
	NextAutoValue uint64              // Next auto-increment value (if IsAutoInc is true)
	TableID       primitives.TableID  // Table this column belongs to
}

// NewColumnMetadata creates a new ColumnMetadata instance.
func NewColumnMetadata(name string, fieldType types.Type, position primitives.ColumnID, tableID primitives.TableID, isPrimary, isAutoInc bool) (*ColumnMetadata, error) {
	if name == "" {
		return nil, fmt.Errorf("column name cannot be empty")
	}

	if !types.IsValidType(fieldType) {
		return nil, fmt.Errorf("field type cannot be nil for column '%s'", name)
	}

	if position < 0 {
		return nil, fmt.Errorf("column position must be non-negative, got %d for column '%s'", position, name)
	}

	var nextAutoValue uint64 = 0
	if isAutoInc {
		if fieldType != types.IntType {
			return nil, fmt.Errorf("auto-increment column '%s' must be of type INT, got %s", name, fieldType.String())
		}
		nextAutoValue = 1 // Initialize auto-increment columns with next value = 1
	}
	return &ColumnMetadata{
		Name:          name,
		FieldType:     fieldType,
		Position:      position,
		IsPrimary:     isPrimary,
		IsAutoInc:     isAutoInc,
		NextAutoValue: nextAutoValue,
		TableID:       tableID,
	}, nil
}
