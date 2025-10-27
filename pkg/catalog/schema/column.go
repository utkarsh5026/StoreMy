package schema

import (
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/types"
)

// ColumnMetadata represents comprehensive metadata for a single column in a table schema.
// It encapsulates all the information needed to define and manage a column's behavior,
// including its data type, position, constraints, and auto-increment settings.
type ColumnMetadata struct {
	Name          string              // Column name
	FieldType     types.Type          // Column data type
	Position      primitives.ColumnID // Column position in tuple (0-indexed)
	IsPrimary     bool                // Whether this is the primary key column
	IsAutoInc     bool                // Whether this column auto-increments
	NextAutoValue uint64              // Next auto-increment value (if IsAutoInc is true)
	TableID       primitives.TableID  // Table this column belongs to
}

// NewColumnMetadata creates a new ColumnMetadata instance with the specified properties.
//
// Parameters:
//   - name: The column identifier (must be non-empty)
//   - fieldType: The data type for column values (must be a valid type)
//   - position: Zero-indexed position of the column in the table's tuple
//   - tableID: Identifier of the parent table
//   - isPrimary: Whether this column is the primary key
//   - isAutoInc: Whether this column should auto-increment
//
// Returns:
//   - *ColumnMetadata: A pointer to the newly created column metadata
//   - error: An error if validation fails
//
// Validation Rules:
//   - Column name must not be empty
//   - Field type must be valid (non-nil and recognized by the type system)
//   - Auto-increment columns must be of type INT
//   - Auto-increment columns are initialized with NextAutoValue = 1
//
// Example:
//
//	col, err := NewColumnMetadata("user_id", types.IntType, 0, tableID, true, true)
//	if err != nil {
//	    log.Fatal(err)
//	}
func NewColumnMetadata(name string, fieldType types.Type, position primitives.ColumnID, tableID primitives.TableID, isPrimary, isAutoInc bool) (*ColumnMetadata, error) {
	if name == "" {
		return nil, fmt.Errorf("column name cannot be empty")
	}

	if !types.IsValidType(fieldType) {
		return nil, fmt.Errorf("field type cannot be nil for column '%s'", name)
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
