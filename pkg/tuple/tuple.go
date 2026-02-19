package tuple

import (
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/types"
	"strings"
)

// TupleRecordID represents a reference to a specific tuple on a specific page
type TupleRecordID struct {
	PageID   primitives.PageID // The page containing this tuple
	TupleNum primitives.SlotID // The tuple number within the page
}

// NewTupleRecordID creates a new TupleRecordID
func NewTupleRecordID(pageID primitives.PageID, tupleNum primitives.SlotID) *TupleRecordID {
	return &TupleRecordID{
		PageID:   pageID,
		TupleNum: tupleNum,
	}
}

func (rid *TupleRecordID) Equals(other *TupleRecordID) bool {
	if other == nil {
		return false
	}
	return rid.PageID.Equals(other.PageID) && rid.TupleNum == other.TupleNum
}

func (rid *TupleRecordID) String() string {
	return fmt.Sprintf("TupleRecordID(page=%s, tuple=%d)", rid.PageID.String(), rid.TupleNum)
}

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
func (t *Tuple) SetField(i primitives.ColumnID, field types.Field) error {
	if i >= t.fieldCount() {
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

// TableNotAssigned checks whether this tuple has been assigned a physical storage location.
// A tuple without a RecordID is considered "not assigned" to a table, meaning it exists
// only in memory and has not been persisted to disk.
//
// Returns:
//   - bool: true if the tuple has no RecordID (not persisted), false otherwise
func (t *Tuple) TableNotAssigned() bool {
	return t.RecordID == nil
}

// NumFields returns the number of fields in this tuple according to its schema.
//
// Returns:
//   - primitives.ColumnID: The total number of fields defined in the tuple's schema.
func (t *Tuple) NumFields() primitives.ColumnID {
	return t.TupleDesc.NumFields()
}

// GetField returns the value of the field at the specified index.
//
// Parameters:
//   - i: The index of the field to retrieve (0-based)
//
// Returns:
//   - types.Field: The field value at the specified index
//   - error: Returns an error if the index is out of bounds
func (t *Tuple) GetField(i primitives.ColumnID) (types.Field, error) {
	if i >= t.fieldCount() {
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
func (t *Tuple) copyFieldsTo(target *Tuple, startIndex primitives.ColumnID) error {
	var i primitives.ColumnID
	for i = 0; i < t.TupleDesc.NumFields(); i++ {
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
func (t *Tuple) WithUpdatedFields(fieldUpdates map[primitives.ColumnID]types.Field) (*Tuple, error) {
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

// Equals returns true if this tuple has the same number of fields and all
// corresponding fields are equal to those in other.
func (t *Tuple) Equals(other *Tuple) bool {
	if t.TupleDesc.NumFields() != other.TupleDesc.NumFields() {
		return false
	}
	for i := range t.TupleDesc.NumFields() {
		f1, _ := t.GetField(i)
		f2, _ := other.GetField(i)
		if !f1.Equals(f2) {
			return false
		}
	}
	return true
}

// CompareAt compares this tuple with another tuple at the given field index.
// Returns -1 if t < other, 0 if equal, and 1 if t > other at that field.
//
// Parameters:
//   - other: The tuple to compare against
//   - fieldIdx: The index of the field to compare (0-based)
//
// Returns:
//   - int: -1 if this tuple's field is less than other's, 0 if equal, 1 if greater
//   - error: Returns an error if the field index is out of bounds or comparison fails
func (t *Tuple) CompareAt(other *Tuple, fieldIdx primitives.ColumnID) (int, error) {
	f1, err := t.GetField(fieldIdx)
	if err != nil {
		return 0, fmt.Errorf("failed to get field %d from first tuple: %w", fieldIdx, err)
	}

	f2, err := other.GetField(fieldIdx)
	if err != nil {
		return 0, fmt.Errorf("failed to get field %d from second tuple: %w", fieldIdx, err)
	}

	lt, err := f1.Compare(primitives.LessThan, f2)
	if err != nil {
		return 0, fmt.Errorf("comparison failed at field %d: %w", fieldIdx, err)
	}
	if lt {
		return -1, nil
	}

	eq, err := f1.Compare(primitives.Equals, f2)
	if err != nil {
		return 0, fmt.Errorf("comparison failed at field %d: %w", fieldIdx, err)
	}
	if eq {
		return 0, nil
	}

	return 1, nil
}

// Hash computes a polynomial rolling hash over all fields of the tuple.
// Uses h = h*31 + fieldHash for each field, matching the hashTuple convention
// used in set-operation operators.
func (t *Tuple) Hash() (primitives.HashCode, error) {
	var hash primitives.HashCode
	for i := primitives.ColumnID(0); i < t.fieldCount(); i++ {
		field, err := t.GetField(i)
		if err != nil {
			return 0, fmt.Errorf("failed to hash field %d: %w", i, err)
		}
		if field == nil {
			continue
		}
		fieldHash, err := field.Hash()
		if err != nil {
			return 0, fmt.Errorf("failed to hash field %d: %w", i, err)
		}
		hash = hash*31 + fieldHash
	}
	return hash, nil
}

// Project creates a new tuple containing only the fields at the given column
// indices (in the order provided). resultDesc must describe exactly len(cols)
// fields. The RecordID of the source tuple is preserved on the result.
func (t *Tuple) Project(cols []primitives.ColumnID, resultDesc *TupleDescription) (*Tuple, error) {
	if primitives.ColumnID(len(cols)) != resultDesc.NumFields() { // #nosec G115
		return nil, fmt.Errorf("project: cols length %d does not match resultDesc field count %d",
			len(cols), resultDesc.NumFields())
	}
	result := NewTuple(resultDesc)
	for i, srcIdx := range cols {
		field, err := t.GetField(srcIdx)
		if err != nil {
			return nil, fmt.Errorf("project: failed to get field %d: %w", srcIdx, err)
		}
		if err := result.SetField(primitives.ColumnID(i), field); err != nil { // #nosec G115
			return nil, fmt.Errorf("project: failed to set field %d: %w", i, err)
		}
	}
	result.RecordID = t.RecordID
	return result, nil
}

// ToStringSlice returns all field values as strings in order.
// Nil fields are represented as "NULL".
func (t *Tuple) ToStringSlice() []string {
	out := make([]string, t.fieldCount())
	for i := primitives.ColumnID(0); i < t.fieldCount(); i++ {
		field := t.fields[i]
		if field == nil {
			out[i] = "NULL"
		} else {
			out[i] = field.String()
		}
	}
	return out
}

// fieldCount returns the number of fields in the tuple.
// This is an unexported helper method, used to get the count of stored field values.
//
// Returns:
//   - primitives.ColumnID: The number of fields in the tuple.
func (t *Tuple) fieldCount() primitives.ColumnID {
	return primitives.ColumnID(len(t.fields)) // #nosec G115
}
