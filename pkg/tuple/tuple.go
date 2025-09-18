package tuple

import (
	"fmt"
	"storemy/pkg/types"
	"strings"
)

// Tuple represents a row of data in the database
type Tuple struct {
	TupleDesc *TupleDescription // Schema of this tuple
	fields    []types.Field     // The actual field values
	RecordID  *RecordID         // Where this tuple is stored (can be nil)
}

// NewTuple creates a new tuple with the given schema
func NewTuple(td *TupleDescription) *Tuple {
	return &Tuple{
		TupleDesc: td,
		fields:    make([]types.Field, td.NumFields()),
	}
}

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

// GetField returns the value of the ith field
func (t *Tuple) GetField(i int) (types.Field, error) {
	if i < 0 || i >= len(t.fields) {
		return nil, fmt.Errorf("field index %d out of bounds [0, %d)", i, len(t.fields))
	}
	return t.fields[i], nil
}

// String returns a string representation of this tuple
// Format: field1\tfield2\tfield3\t...\tfieldN\n
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

// CombineTuples combines two tuples into a single tuple
// This is useful for joins where we concatenate tuples from different tables
func CombineTuples(t1, t2 *Tuple) (*Tuple, error) {
	if t1 == nil || t2 == nil {
		return nil, fmt.Errorf("cannot combine nil tuples")
	}

	newTupleDesc := Combine(t1.TupleDesc, t2.TupleDesc)
	newTuple := NewTuple(newTupleDesc)

	fieldIndex := 0
	for i := 0; i < t1.TupleDesc.NumFields(); i++ {
		field, err := t1.GetField(i)
		if err != nil {
			return nil, err
		}
		if field != nil {
			if err := newTuple.SetField(fieldIndex, field); err != nil {
				return nil, err
			}
		}
		fieldIndex++
	}

	for i := 0; i < t2.TupleDesc.NumFields(); i++ {
		field, err := t2.GetField(i)
		if err != nil {
			return nil, err
		}
		if field != nil {
			if err := newTuple.SetField(fieldIndex, field); err != nil {
				return nil, err
			}
		}
		fieldIndex++
	}
	return newTuple, nil
}
