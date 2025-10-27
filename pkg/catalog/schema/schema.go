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
type Schema struct {
	TupleDesc *tuple.TupleDescription
	TableID   primitives.TableID
	TableName string

	// Primary key metadata
	PrimaryKey      string
	PrimaryKeyIndex int

	// Column metadata
	Columns []ColumnMetadata

	// Fast lookup indices
	fieldNameToIndex map[string]int
}

// NewSchema creates a new Schema from column metadata.
func NewSchema(tableID primitives.TableID, tableName string, columns []ColumnMetadata) (*Schema, error) {
	// Note: tableID can be any integer value (including negative) as it comes from a hash function
	// Only InvalidTableID (-1) is reserved for system table schemas during initialization
	// We don't validate the tableID here since it's a hash that can be any value

	if len(columns) == 0 {
		return nil, fmt.Errorf("schema must have at least one column")
	}

	sortedCols := slices.Clone(columns)
	slices.SortFunc(sortedCols, func(a, b ColumnMetadata) int {
		return int(a.Position) - int(b.Position)
	})

	fieldTypes := make([]types.Type, len(sortedCols))
	fieldNames := make([]string, len(sortedCols))
	fieldNameToIndex := make(map[string]int, len(sortedCols))

	primaryKey := ""
	primaryKeyIndex := -1

	for i, col := range sortedCols {
		fieldTypes[i] = col.FieldType
		fieldNames[i] = col.Name
		fieldNameToIndex[col.Name] = i

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

// GetFieldIndex returns the field index for a given field name.
// Returns -1 if the field doesn't exist.
func (s *Schema) GetFieldIndex(fieldName string) int {
	if idx, ok := s.fieldNameToIndex[fieldName]; ok {
		return idx
	}
	return -1
}

// HasColumn returns true if the schema contains a column with the given name.
func (s *Schema) HasColumn(fieldName string) bool {
	_, ok := s.fieldNameToIndex[fieldName]
	return ok
}

// GetColumnMetadata returns the metadata for a column by name.
// Returns nil if the column doesn't exist.
func (s *Schema) GetColumnMetadata(fieldName string) *ColumnMetadata {
	idx := s.GetFieldIndex(fieldName)
	if idx < 0 {
		return nil
	}
	return &s.Columns[idx]
}

// GetColumnMetadataByIndex returns the metadata for a column by its position index.
// Returns nil if the index is out of bounds.
func (s *Schema) GetColumnMetadataByIndex(index int) *ColumnMetadata {
	if index < 0 || index >= len(s.Columns) {
		return nil
	}
	return &s.Columns[index]
}

// GetPrimaryKeyIndex returns the field index of the primary key column.
// Returns -1 if there is no primary key.
func (s *Schema) GetPrimaryKeyIndex() int {
	return s.PrimaryKeyIndex
}

// GetPrimaryKeyName returns the name of the primary key column.
// Returns empty string if there is no primary key.
func (s *Schema) GetPrimaryKeyName() string {
	return s.PrimaryKey
}

// GetAutoIncrementColumns returns a slice of all auto-increment columns.
func (s *Schema) GetAutoIncrementColumns() []ColumnMetadata {
	var autoIncCols []ColumnMetadata
	for _, col := range s.Columns {
		if col.IsAutoInc {
			autoIncCols = append(autoIncCols, col)
		}
	}
	return autoIncCols
}

// NumFields returns the number of fields in the schema.
func (s *Schema) NumFields() int {
	return len(s.Columns)
}

// FieldNames returns a slice of all field names in order.
func (s *Schema) FieldNames() []string {
	names := make([]string, len(s.Columns))
	for i, col := range s.Columns {
		names[i] = col.Name
	}
	return names
}

// FieldTypes returns a slice of all field types in order.
func (s *Schema) FieldTypes() []types.Type {
	fieldTypes := make([]types.Type, len(s.Columns))
	for i, col := range s.Columns {
		fieldTypes[i] = col.FieldType
	}
	return fieldTypes
}
