package tuple

import (
	"fmt"
	"storemy/pkg/types"
	"strings"
)

// Tuple represents a row of data in the database.
// Each tuple contains a collection of fields that conform to a schema (TupleDescription).
// Tuples are the fundamental unit of data storage and retrieval in the database.
//
// Example:
//
//	td := NewTupleDescription([]types.Type{types.IntType, types.StringType}, []string{"id", "name"})
//	tuple := NewTuple(td)
//	tuple.SetField(0, types.NewIntField(1))
//	tuple.SetField(1, types.NewStringField("Alice"))
type Tuple struct {
	TupleDesc *TupleDescription // Schema of this tuple
	fields    []types.Field     // The actual field values
	RecordID  *TupleRecordID    // Where this tuple is stored (can be nil)
}

// NewTuple creates a new tuple with the given schema.
// All fields are initialized to nil and must be set using SetField.
//
// Parameters:
//   - td: The tuple description (schema) that defines the structure of this tuple
//
// Returns:
//   - A pointer to a newly created Tuple with uninitialized fields
func NewTuple(td *TupleDescription) *Tuple {
	return &Tuple{
		TupleDesc: td,
		fields:    make([]types.Field, td.NumFields()),
	}
}

// SetField sets the value of the field at the specified index.
// The field type must match the type specified in the tuple's schema.
//
// Parameters:
//   - i: The index of the field to set (0-based)
//   - field: The field value to set
//
// Returns:
//   - error: Returns an error if the index is out of bounds or if the field type doesn't match the schema
func (t *Tuple) SetField(i int, field types.Field) error {
	if i < 0 || i >= len(t.fields) {
		return fmt.Errorf("field index %d out of bounds [0, %d)", i, len(t.fields))
	}

	expectedType, _ := t.TupleDesc.TypeAtIndex(i)
	if field.Type() != expectedType {
		return fmt.Errorf("field type mismatch: expected %v, got %v",
			expectedType, field.Type())
	}

	t.fields[i] = field
	return nil
}

// GetField returns the value of the field at the specified index.
//
// Parameters:
//   - i: The index of the field to retrieve (0-based)
//
// Returns:
//   - types.Field: The field value at the specified index
//   - error: Returns an error if the index is out of bounds
func (t *Tuple) GetField(i int) (types.Field, error) {
	if i < 0 || i >= len(t.fields) {
		return nil, fmt.Errorf("field index %d out of bounds [0, %d)", i, len(t.fields))
	}
	return t.fields[i], nil
}

// String returns a string representation of this tuple.
// Fields are separated by tabs and the string ends with a newline.
// Nil fields are represented as "null".
//
// Format: field1\tfield2\tfield3\t...\tfieldN\n
//
// Returns:
//   - A tab-separated string representation of all fields followed by a newline
func (t *Tuple) String() string {
	var parts []string
	for _, field := range t.fields {
		if field != nil {
			parts = append(parts, field.String())
		} else {
			parts = append(parts, "null")
		}
	}
	return strings.Join(parts, "\t") + "\n"
}

// CombineTuples combines two tuples into a single tuple by concatenating their fields.
// This is commonly used in join operations where tuples from different tables are merged.
// The resulting tuple's schema is the combination of both input tuple schemas.
//
// Parameters:
//   - t1: The first tuple (fields will appear first in the result)
//   - t2: The second tuple (fields will appear after t1's fields)
//
// Returns:
//   - *Tuple: A new tuple containing all fields from t1 followed by all fields from t2
//   - error: Returns an error if either tuple is nil or if field copying fails
func CombineTuples(t1, t2 *Tuple) (*Tuple, error) {
	if t1 == nil || t2 == nil {
		return nil, fmt.Errorf("cannot combine nil tuples")
	}

	newTupleDesc := Combine(t1.TupleDesc, t2.TupleDesc)
	newTuple := NewTuple(newTupleDesc)

	if err := t1.copyFieldsTo(newTuple, 0); err != nil {
		return nil, err
	}

	if err := t2.copyFieldsTo(newTuple, t1.TupleDesc.NumFields()); err != nil {
		return nil, err
	}

	return newTuple, nil
}

// copyFieldsTo copies all fields from this tuple to the target tuple starting at the specified index.
// This is an internal helper method used by CombineTuples.
//
// Parameters:
//   - target: The tuple to copy fields into
//   - startIndex: The index in the target tuple where copying should begin
//
// Returns:
//   - error: Returns an error if any field retrieval or setting fails
func (t *Tuple) copyFieldsTo(target *Tuple, startIndex int) error {
	for i := 0; i < t.TupleDesc.NumFields(); i++ {
		field, err := t.GetField(i)
		if err != nil {
			return err
		}
		if field != nil {
			if err := target.SetField(startIndex+i, field); err != nil {
				return err
			}
		}
	}
	return nil
}

// Clone creates a deep copy of this tuple with all field values.
// The cloned tuple shares the same TupleDescription but has independent field values.
// This ensures that modifications to the clone do not affect the original tuple.
//
// Returns:
//   - *Tuple: A new tuple with the same schema and field values as the original
//   - error: Returns an error if any field cannot be copied
func (t *Tuple) Clone() (*Tuple, error) {
	newTup := NewTuple(t.TupleDesc)

	for i := range t.TupleDesc.NumFields() {
		field, err := t.GetField(i)
		if err != nil {
			return nil, fmt.Errorf("failed to get field %d: %w", i, err)
		}

		if err := newTup.SetField(i, field); err != nil {
			return nil, fmt.Errorf("failed to copy field %d: %w", i, err)
		}
	}

	return newTup, nil
}

// WithUpdatedFields returns a new tuple with specified fields updated.
// The original tuple remains unchanged (immutable operation).
// This is useful for update operations where you want to modify specific fields
// while keeping the rest unchanged.
//
// Parameters:
//   - fieldUpdates: A map from field index to new field value. Only specified fields are updated.
//
// Returns:
//   - *Tuple: A new tuple with the specified fields updated
//   - error: Returns an error if cloning fails or if any field update is invalid
func (t *Tuple) WithUpdatedFields(fieldUpdates map[int]types.Field) (*Tuple, error) {
	newTup, err := t.Clone()
	if err != nil {
		return nil, fmt.Errorf("failed to clone tuple: %w", err)
	}

	for fieldIdx, newValue := range fieldUpdates {
		if err := newTup.SetField(fieldIdx, newValue); err != nil {
			return nil, fmt.Errorf("failed to update field %d: %w", fieldIdx, err)
		}
	}

	return newTup, nil
}
