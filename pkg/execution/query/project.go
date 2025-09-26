package query

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// Project implements column selection - it chooses which fields to output from input tuples.
// This operator corresponds to the SELECT clause in SQL queries, allowing users to specify
// exactly which columns should appear in the result set.
//
// Conceptually: SELECT col1, col3, col5 FROM table
//
// Project takes tuples with N fields and outputs tuples with M fields (where M <= N),
// effectively creating a new schema with only the requested columns while preserving
// the original tuple ordering and record identifiers.
type Project struct {
	base      *BaseIterator           // Handles the iterator caching and state management logic
	fieldList []int                   // Indices of fields to project from input tuples (e.g., [0, 2, 4])
	typesList []types.Type            // Data types of the projected fields in output order
	child     iterator.DbIterator     // Source iterator providing input tuples to project
	tupleDesc *tuple.TupleDescription // Schema definition for output tuples after projection
}

// NewProject creates a new Project operator that selects specific fields from input tuples.
func NewProject(fieldList []int, typesList []types.Type, child iterator.DbIterator) (*Project, error) {
	if child == nil {
		return nil, fmt.Errorf("child operator cannot be nil")
	}

	if len(fieldList) != len(typesList) {
		return nil, fmt.Errorf("field list length (%d) must match types list length (%d)",
			len(fieldList), len(typesList))
	}

	if len(fieldList) == 0 {
		return nil, fmt.Errorf("must project at least one field")
	}

	childTupleDesc := child.GetTupleDesc()
	if childTupleDesc == nil {
		return nil, fmt.Errorf("child operator has nil tuple descriptor")
	}

	fieldNames := make([]string, len(fieldList))
	for i, fieldIndex := range fieldList {
		if fieldIndex < 0 || fieldIndex >= childTupleDesc.NumFields() {
			return nil, fmt.Errorf("field index %d out of bounds (child has %d fields)",
				fieldIndex, childTupleDesc.NumFields())
		}

		fieldName, err := childTupleDesc.GetFieldName(fieldIndex)
		if err != nil {
			return nil, fmt.Errorf("failed to get field name for index %d: %v", fieldIndex, err)
		}
		fieldNames[i] = fieldName

		expectedType, err := childTupleDesc.TypeAtIndex(fieldIndex)
		if err != nil {
			return nil, fmt.Errorf("failed to get type for field %d: %v", fieldIndex, err)
		}
		if expectedType != typesList[i] {
			return nil, fmt.Errorf("type mismatch for field %d: expected %v, got %v",
				fieldIndex, expectedType, typesList[i])
		}
	}

	tupleDesc, err := tuple.NewTupleDesc(typesList, fieldNames)
	if err != nil {
		return nil, fmt.Errorf("failed to create output tuple desc: %v", err)
	}

	p := &Project{
		fieldList: fieldList,
		typesList: typesList,
		child:     child,
		tupleDesc: tupleDesc,
	}

	p.base = NewBaseIterator(p.readNext)
	return p, nil
}

// GetTupleDesc returns the tuple description (schema) for tuples produced by this projection.
// The schema contains only the projected fields in the order specified during construction.
func (p *Project) GetTupleDesc() *tuple.TupleDescription {
	return p.tupleDesc
}

// Open initializes the Project operator for iteration by opening its child operator.
// This method must be called before any iteration operations can be performed.
func (p *Project) Open() error {
	if err := p.child.Open(); err != nil {
		return fmt.Errorf("failed to open child operator: %v", err)
	}

	p.base.MarkOpened()
	return nil
}

// Close releases resources associated with the Project operator by closing its child
// operator and performing cleanup.
func (p *Project) Close() error {
	if p.child != nil {
		p.child.Close()
	}
	return p.base.Close()
}

// Rewind resets the Project operator to the beginning of its result set.
// This allows the projection to be re-executed from the start, which is useful
// for operations that need to scan the projected results multiple times.
func (p *Project) Rewind() error {
	if err := p.child.Rewind(); err != nil {
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
// It reads the next tuple from the child operator and creates a new tuple
// containing only the projected fields in the specified order.
func (p *Project) readNext() (*tuple.Tuple, error) {
	hasNext, err := p.child.HasNext()
	if err != nil {
		return nil, fmt.Errorf("error checking if child has next: %v", err)
	}

	if !hasNext {
		return nil, nil // No more tuples
	}

	childTuple, err := p.child.Next()
	if err != nil {
		return nil, fmt.Errorf("error getting next tuple from child: %v", err)
	}

	if childTuple == nil {
		return nil, nil
	}

	projectedTuple := tuple.NewTuple(p.tupleDesc)
	for i, fieldIndex := range p.fieldList {
		field, err := childTuple.GetField(fieldIndex)
		if err != nil {
			return nil, fmt.Errorf("failed to get field %d from child tuple: %v", fieldIndex, err)
		}

		if err := projectedTuple.SetField(i, field); err != nil {
			return nil, fmt.Errorf("failed to set field %d in projected tuple: %v", i, err)
		}
	}

	projectedTuple.RecordID = childTuple.RecordID
	return projectedTuple, nil
}
