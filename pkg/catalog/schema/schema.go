package schema

import (
	"fmt"
	"slices"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// Schema represents a complete table schema with metadata and helper methods.
// It provides a rich interface for working with table structures beyond just field types.
//
// A Schema encapsulates all metadata about a database table, including:
//   - Column definitions (names, types, positions)
//   - Primary key information
//   - Table identification (ID and name)
//   - Fast lookup capabilities for field resolution
//
// Schema is immutable after creation and thread-safe for concurrent read operations.
type Schema struct {
	// TupleDesc defines the structure of tuples (rows) in this table,
	// including field types and names in their proper order.
	TupleDesc *tuple.TupleDescription

	// TableID is the unique identifier for this table in the catalog.
	TableID primitives.TableID

	// TableName is the human-readable name of the table.
	TableName string

	// PrimaryKey is the name of the primary key column, or empty string if none exists.
	PrimaryKey string

	// PrimaryKeyIndex is the zero-based index of the primary key column,
	// or -1 if no primary key is defined.
	PrimaryKeyIndex int

	// Columns contains metadata for all columns in the table,
	// sorted by their position.
	Columns []ColumnMetadata

	// fieldNameToIndex provides O(1) lookup from field name to column index.
	fieldNameToIndex map[string]primitives.ColumnID
}

// NewSchema creates a new Schema from column metadata.
//
// The function validates and processes the provided columns:
//   - Sorts columns by their position to ensure proper ordering
//   - Builds internal lookup structures for efficient field resolution
//   - Identifies the primary key column if one exists
//   - Creates the underlying tuple description
//
// Parameters:
//   - tableID: Unique identifier for the table
//   - tableName: Human-readable name of the table
//   - columns: Slice of column metadata describing each field
//
// Returns:
//   - *Schema: The constructed schema object
//   - error: Non-nil if validation fails or columns are invalid
//
// Errors:
//   - Returns error if columns slice is empty
//   - Returns error if tuple description creation fails
//
// Example:
//
//	columns := []ColumnMetadata{
//	    {Name: "id", FieldType: types.Int64Type, Position: 0, IsPrimary: true},
//	    {Name: "name", FieldType: types.StringType, Position: 1},
//	}
//	schema, err := NewSchema(tableID, "users", columns)
func NewSchema(tableID primitives.TableID, tableName string, columns []ColumnMetadata) (*Schema, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("schema must have at least one column")
	}

	sortedCols := slices.Clone(columns)
	slices.SortFunc(sortedCols, func(a, b ColumnMetadata) int {
		return int(a.Position) - int(b.Position)
	})

	fieldTypes := make([]types.Type, len(sortedCols))
	fieldNames := make([]string, len(sortedCols))
	fieldNameToIndex := make(map[string]primitives.ColumnID, len(sortedCols))

	primaryKey := ""
	primaryKeyIndex := -1

	for i, col := range sortedCols {
		fieldTypes[i] = col.FieldType
		fieldNames[i] = col.Name
		fieldNameToIndex[col.Name] = primitives.ColumnID(i)

		if col.IsPrimary {
			primaryKey = col.Name
			primaryKeyIndex = i
		}
	}

	tupleDesc, err := tuple.NewTupleDesc(fieldTypes, fieldNames)
	if err != nil {
		return nil, fmt.Errorf("failed to create tuple description: %w", err)
	}

	return &Schema{
		TupleDesc:        tupleDesc,
		TableID:          tableID,
		TableName:        tableName,
		PrimaryKey:       primaryKey,
		PrimaryKeyIndex:  primaryKeyIndex,
		Columns:          sortedCols,
		fieldNameToIndex: fieldNameToIndex,
	}, nil
}

// GetFieldIndex returns the zero-based column index for a given field name.
//
// This method provides efficient O(1) lookup using an internal map.
// Field names are case-sensitive.
//
// Parameters:
//   - fieldName: The name of the field to look up
//
// Returns:
//   - primitives.ColumnID: The zero-based index of the field
//   - error: Non-nil if the field name doesn't exist in the schema
//
// Example:
//
//	idx, err := schema.GetFieldIndex("user_id")
//	if err != nil {
//	    // Handle field not found
//	}
func (s *Schema) GetFieldIndex(fieldName string) (primitives.ColumnID, error) {
	if idx, ok := s.fieldNameToIndex[fieldName]; ok {
		return idx, nil
	}
	return 0, fmt.Errorf("invalid index")
}

// NumFields returns the total number of columns in the schema.
//
// This is equivalent to the number of fields in each tuple/row.
//
// Returns:
//   - int: The count of columns
//
// Example:
//
//	if schema.NumFields() > 10 {
//	    // Wide table detected
//	}
func (s *Schema) NumFields() int {
	return len(s.Columns)
}

// FieldNames returns a slice containing all field names in their proper order.
//
// The returned slice is a new copy and can be safely modified without
// affecting the schema. Fields are ordered by their position in the schema.
//
// Returns:
//   - []string: Ordered slice of field names
//
// Example:
//
//	names := schema.FieldNames()
//	fmt.Printf("Table columns: %v\n", names)
func (s *Schema) FieldNames() []string {
	names := make([]string, len(s.Columns))
	for i, col := range s.Columns {
		names[i] = col.Name
	}
	return names
}

// FieldTypes returns a slice containing all field types in their proper order.
//
// The returned slice is a new copy and can be safely modified without
// affecting the schema. Types are ordered to match the field positions.
//
// Returns:
//   - []types.Type: Ordered slice of field types
//
// Example:
//
//	types := schema.FieldTypes()
//	for i, t := range types {
//	    fmt.Printf("Field %d type: %v\n", i, t)
//	}
func (s *Schema) FieldTypes() []types.Type {
	fieldTypes := make([]types.Type, len(s.Columns))
	for i, col := range s.Columns {
		fieldTypes[i] = col.FieldType
	}
	return fieldTypes
}
