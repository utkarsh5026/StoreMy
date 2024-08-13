package fields

import (
	"StoreMy/src/pkg/storage/ids"
	"fmt"
	"strings"
)

type Field interface {
	Type() Type
	String() string
	Compare(op Predicate, value Field) (bool, error)
	Serialize() []byte
}

type Type int
type Predicate int

const (
	Equals Predicate = iota
	NotEquals
	GreaterThan
	LessThan
	GreaterThanOrEqual
	LessThanOrEqual
	Like // Only for StringField
)

const (
	IntType    Type = iota // Represents an integer filed in a tuple
	StringType             // Represents a string field in a tuple
)

type TupleDescription struct {
	Fields []string
	Types  []Type
}

func NewTupleDescription(types []Type, fields []string) *TupleDescription {
	return &TupleDescription{
		Fields: fields,
		Types:  types,
	}
}

// NumFields returns the number of fields in the TupleDescription
func (td *TupleDescription) NumFields() int {
	return len(td.Fields)
}

func (td *TupleDescription) GetFieldName(i int) string {
	if i < 0 || i >= len(td.Fields) {
		return ""
	}
	return td.Fields[i]
}

// GetFieldType returns the Type of the field at the given index.
// If the index is out of range, it returns -1.
//
// Parameters:
//
//	idx - the index of the field type to retrieve
//
// Returns:
//
//	Type - the type of the field at the specified index, or -1 if the index is out of range
//	error - an error if the index is out of range
func (td *TupleDescription) GetFieldType(idx int) (Type, error) {
	if idx < 0 || idx >= len(td.Types) {
		return IntType, fmt.Errorf("index out of range")
	}
	return td.Types[idx], nil
}

type Tuple struct {
	description *TupleDescription
	fields      []Field
	recordId    ids.RecordId
}

func NewTuple(desc *TupleDescription) *Tuple {
	return &Tuple{
		description: desc,
		fields:      make([]Field, desc.NumFields()),
	}
}
func (t *Tuple) GetField(i int) Field {
	if i < 0 || i >= len(t.fields) {
		return nil
	}
	return t.fields[i]
}

func (t *Tuple) SetField(i int, f Field) error {
	if i >= 0 && i < len(t.fields) {
		t.fields[i] = f
		return nil
	}
	return fmt.Errorf("index out of range")
}

// GetRecordID returns the RecordID of this tuple
func (t *Tuple) GetRecordID() ids.RecordId {
	return t.recordId
}

// SetRecordID sets the RecordID of this tuple
func (t *Tuple) SetRecordID(rid ids.RecordId) {
	t.recordId = rid
}

// String returns a string representation of this tuple
func (t *Tuple) String() string {
	var sb strings.Builder
	for i, f := range t.fields {
		if i > 0 {
			sb.WriteString("\t")
		}
		sb.WriteString(f.String())
	}
	sb.WriteString("\n")
	return sb.String()
}
