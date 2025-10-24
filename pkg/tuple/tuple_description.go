package tuple

import (
	"fmt"
	"storemy/pkg/types"
	"strings"
)

// TupleDescription describes the schema of a tuple (like a table schema).
// It contains the types and names of fields in a tuple, providing metadata
// about the structure of data records in the database.
type TupleDescription struct {
	// Types contains the data type of each field in order
	Types []types.Type
	// FieldNames contains the name of each field (optional, may be nil)
	FieldNames []string
}

// NewTupleDesc creates a new TupleDescription given field types and optional field names.
// If fieldNames is nil, fields will have no names.
//
// Parameters:
//   - fieldTypes: slice of field types (must contain at least one element)
//   - fieldNames: optional slice of field names (must match fieldTypes length if provided)
//
// Returns:
//   - *TupleDescription: newly created tuple descriptor
//   - error: if fieldTypes is empty or fieldNames length doesn't match fieldTypes length
func NewTupleDesc(fieldTypes []types.Type, fieldNames []string) (*TupleDescription, error) {
	if len(fieldTypes) < 1 {
		return nil, fmt.Errorf("must provide at least one field type")
	}

	typesCopy := make([]types.Type, len(fieldTypes))
	copy(typesCopy, fieldTypes)

	var namesCopy []string
	if fieldNames != nil {
		if len(fieldNames) != len(fieldTypes) {
			return nil, fmt.Errorf("field names length (%d) must match field types length (%d)",
				len(fieldNames), len(fieldTypes))
		}
		namesCopy = make([]string, len(fieldNames))
		copy(namesCopy, fieldNames)
	}

	return &TupleDescription{
		Types:      typesCopy,
		FieldNames: namesCopy,
	}, nil
}

// NumFields returns the number of fields in this tuple descriptor.
//
// Returns:
//   - int: total number of fields in the schema
func (td *TupleDescription) NumFields() int {
	return len(td.Types)
}

// GetFieldName returns the name of the ith field.
//
// Parameters:
//   - i: zero-based index of the field
//
// Returns:
//   - string: field name, or empty string if no names were provided
//   - error: if index is out of bounds
func (td *TupleDescription) GetFieldName(i int) (string, error) {
	if i < 0 || i >= len(td.Types) {
		return "", fmt.Errorf("field index %d out of bounds [0, %d)", i, len(td.Types))
	}

	if td.FieldNames == nil {
		return "", nil
	}

	return td.FieldNames[i], nil
}

// TypeAtIndex returns the type of the ith field.
//
// Parameters:
//   - i: zero-based index of the field
//
// Returns:
//   - types.Type: data type of the field
//   - error: if index is out of bounds
func (td *TupleDescription) TypeAtIndex(i int) (types.Type, error) {
	if i < 0 || i >= len(td.Types) {
		return 0, fmt.Errorf("field index %d out of bounds [0, %d)", i, len(td.Types))
	}
	return td.Types[i], nil
}

// GetSize returns the size in bytes of tuples corresponding to this TupleDescription.
// This is the sum of all field type sizes.
//
// Returns:
//   - uint32: total size in bytes
func (td *TupleDescription) GetSize() uint32 {
	var size uint32
	for _, fieldType := range td.Types {
		size += fieldType.Size()
	}
	return size
}

// Equals checks if two TupleDescriptions are equal.
// Two descriptors are equal if they have the same size and field types in the same order.
// Field names are not compared.
//
// Parameters:
//   - other: the TupleDescription to compare against
//
// Returns:
//   - bool: true if the descriptors are equal, false otherwise
func (td *TupleDescription) Equals(other *TupleDescription) bool {
	if other == nil {
		return false
	}

	if td.GetSize() != other.GetSize() {
		return false
	}

	if len(td.Types) != len(other.Types) {
		return false
	}

	for i, fieldType := range td.Types {
		if fieldType != other.Types[i] {
			return false
		}
	}
	return true
}

// String returns a string representation of this TupleDescription.
// Format: "Type1(fieldName1),Type2(fieldName2),..."
// If a field has no name, "null" is used as the name.
//
// Returns:
//   - string: comma-separated list of field descriptions
func (td *TupleDescription) String() string {
	var parts []string

	for i, fieldType := range td.Types {
		var fieldName string
		if td.FieldNames != nil && i < len(td.FieldNames) {
			fieldName = td.FieldNames[i]
		} else {
			fieldName = "null"
		}

		part := fmt.Sprintf("%s(%s)", fieldType.String(), fieldName)
		parts = append(parts, part)
	}

	return strings.Join(parts, ",")
}

// FindFieldIndex locates a field by name in the tuple descriptor.
// Performs case-sensitive linear search through the schema definition.
//
// Parameters:
//   - fieldName: name of the field to find
//
// Returns:
//   - int: zero-based index of the field
//   - error: if the field is not found
func (td *TupleDescription) FindFieldIndex(fieldName string) (int, error) {
	for i := 0; i < td.NumFields(); i++ {
		name, _ := td.GetFieldName(i)
		if name == fieldName {
			return i, nil
		}
	}
	return -1, fmt.Errorf("column %s not found", fieldName)
}

// Combine merges two TupleDescriptions into one.
// The resulting descriptor contains all fields from td1 followed by all fields from td2.
// If either descriptor is nil, returns the other descriptor.
// If both are nil, returns nil.
//
// Parameters:
//   - td1: first TupleDescription to merge
//   - td2: second TupleDescription to merge
//
// Returns:
//   - *TupleDescription: combined tuple descriptor with all fields from both inputs
func Combine(td1, td2 *TupleDescription) *TupleDescription {
	if td1 == nil && td2 == nil {
		return nil
	}
	if td1 == nil {
		return td2
	}
	if td2 == nil {
		return td1
	}

	newTypes := make([]types.Type, 0, len(td1.Types)+len(td2.Types))
	newTypes = append(newTypes, td1.Types...)
	newTypes = append(newTypes, td2.Types...)

	var newFieldNames []string
	if td1.FieldNames != nil || td2.FieldNames != nil {
		newFieldNames = make([]string, 0, len(newTypes))

		if td1.FieldNames != nil {
			newFieldNames = append(newFieldNames, td1.FieldNames...)
		} else {
			for i := 0; i < len(td1.Types); i++ {
				newFieldNames = append(newFieldNames, "")
			}
		}

		if td2.FieldNames != nil {
			newFieldNames = append(newFieldNames, td2.FieldNames...)
		} else {
			for i := 0; i < len(td2.Types); i++ {
				newFieldNames = append(newFieldNames, "")
			}
		}
	}

	combined, _ := NewTupleDesc(newTypes, newFieldNames)
	return combined
}
