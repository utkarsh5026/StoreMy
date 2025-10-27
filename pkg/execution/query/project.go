package query

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// Project implements column selection - it chooses which fields to output from input tuples.
// This operator corresponds to the SELECT clause in SQL queries, allowing users to specify
// exactly which columns should appear in the result set.
//
// Conceptually: SELECT col1, col3, col5 FROM table
type Project struct {
	base           *BaseIterator
	projectedCols  []primitives.ColumnID
	projectedTypes []types.Type
	source         *SourceIter
	tupleDesc      *tuple.TupleDescription
}

// NewProject creates a new Project operator that selects specific fields from input tuples.
func NewProject(projectedCols []primitives.ColumnID, projectedTypes []types.Type, source iterator.DbIterator) (*Project, error) {
	if err := validateProjectInputs(projectedCols, projectedTypes, source); err != nil {
		return nil, err
	}

	childTupleDesc := source.GetTupleDesc()
	fieldNames, err := validateAndExtractFieldNames(projectedCols, projectedTypes, childTupleDesc)
	if err != nil {
		return nil, err
	}

	tupleDesc, err := tuple.NewTupleDesc(projectedTypes, fieldNames)
	if err != nil {
		return nil, fmt.Errorf("failed to create output tuple desc: %v", err)
	}

	sourceOp, err := NewSourceOperator(source)
	if err != nil {
		return nil, err
	}

	p := &Project{
		projectedCols:  projectedCols,
		projectedTypes: projectedTypes,
		source:         sourceOp,
		tupleDesc:      tupleDesc,
	}

	p.base = NewBaseIterator(p.readNext)
	return p, nil
}

// validateProjectInputs performs basic validation of constructor parameters
func validateProjectInputs(projectedCols []primitives.ColumnID, projectedTypes []types.Type, source iterator.DbIterator) error {
	if source == nil {
		return fmt.Errorf("source operator cannot be nil")
	}

	if len(projectedCols) != len(projectedTypes) {
		return fmt.Errorf("field list length (%d) must match types list length (%d)",
			len(projectedCols), len(projectedTypes))
	}

	if len(projectedCols) == 0 {
		return fmt.Errorf("must project at least one field")
	}

	if source.GetTupleDesc() == nil {
		return fmt.Errorf("source operator has nil tuple descriptor")
	}

	return nil
}

// GetTupleDesc returns the tuple description (schema) for tuples produced by this projection.
// The schema contains only the projected fields in the order specified during construction.
func (p *Project) GetTupleDesc() *tuple.TupleDescription {
	return p.tupleDesc
}

// Open initializes the Project operator for iteration by opening its source operator.
func (p *Project) Open() error {
	if err := p.source.Open(); err != nil {
		return err
	}
	p.base.MarkOpened()
	return nil
}

// Close releases resources associated with the Project operator by closing its source
// operator and performing cleanup.
func (p *Project) Close() error {
	if p.source != nil {
		p.source.Close()
	}
	return p.base.Close()
}

// Rewind resets the Project operator to the beginning of its result set.
func (p *Project) Rewind() error {
	if err := p.source.Rewind(); err != nil {
		return err
	}

	p.base.ClearCache()
	return nil
}

// HasNext checks if there are more tuples available for projection.
func (p *Project) HasNext() (bool, error) {
	return p.base.HasNext()
}

// Next retrieves the next projected tuple from the operator.
func (p *Project) Next() (*tuple.Tuple, error) {
	return p.base.Next()
}

// readNext is the internal method that implements the projection logic.
// It reads the next tuple from the source operator and creates a new tuple
// containing only the projected fields in the specified order.
func (p *Project) readNext() (*tuple.Tuple, error) {
	t, err := p.source.FetchNext()
	if err != nil || t == nil {
		return t, err
	}

	projectedTuple := tuple.NewTuple(p.tupleDesc)
	for i, fieldIndex := range p.projectedCols {
		field, err := t.GetField(fieldIndex)
		if err != nil {
			return nil, fmt.Errorf("failed to get field %d from source tuple: %v", fieldIndex, err)
		}

		if err := projectedTuple.SetField(primitives.ColumnID(i), field); err != nil {
			return nil, fmt.Errorf("failed to set field %d in projected tuple: %v", i, err)
		}
	}

	projectedTuple.RecordID = t.RecordID
	return projectedTuple, nil
}

// validateAndExtractFieldNames validates field indices and extracts corresponding field names
func validateAndExtractFieldNames(cols []primitives.ColumnID, types []types.Type,
	td *tuple.TupleDescription) ([]string, error) {
	fieldNames := make([]string, len(cols))

	for i, fieldIndex := range cols {
		if fieldIndex >= td.NumFields() {
			return nil, fmt.Errorf("field index %d out of bounds (source has %d fields)",
				fieldIndex, td.NumFields())
		}

		fieldName, err := td.GetFieldName(fieldIndex)
		if err != nil {
			return nil, fmt.Errorf("failed to get field name for index %d: %v", fieldIndex, err)
		}
		fieldNames[i] = fieldName

		if err := validateFieldType(fieldIndex, types[i], td); err != nil {
			return nil, err
		}
	}

	return fieldNames, nil
}

// validateFieldType checks that the expected type matches the source schema
func validateFieldType(idx primitives.ColumnID, expected types.Type, td *tuple.TupleDescription) error {

	actual, err := td.TypeAtIndex(idx)
	if err != nil {
		return fmt.Errorf("failed to get type for field %d: %v", idx, err)
	}

	if actual != expected {
		return fmt.Errorf("type mismatch for field %d: expected %v, got %v",
			idx, expected, actual)
	}

	return nil
}
