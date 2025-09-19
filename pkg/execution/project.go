package execution

import (
	"fmt"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// Project implements column selection - it chooses which fields to output
//
// Conceptually: SELECT col1, col3, col5 FROM table
//
// Project takes tuples with N fields and outputs tuples with M fields (where M <= N)
type Project struct {
	base      *BaseIterator           // Handles the iterator caching logic
	fieldList []int                   // Which fields to project (e.g., [0, 2, 4])
	typesList []types.Type            // Types of the projected fields
	child     DbIterator              // Where we get tuples from
	tupleDesc *tuple.TupleDescription // Schema of our output tuples
}

// NewProject creates a new Project operator that selects specific columns from the input tuples.
// It validates that the field indices are valid for the child operator's schema and that
// the types match the expected types for the projected fields.
//
// Parameters:
//   - fieldList: Indices of fields to project from the child tuples (e.g., [0, 2, 4])
//   - typesList: Expected types of the projected fields, must match fieldList length
//   - child: The child operator that provides input tuples (cannot be nil)
//
// Returns:
//   - *Project: A new Project instance configured with the specified field projection
//   - error: An error if validation fails (nil child, mismatched lengths, invalid indices, type mismatches)
func NewProject(fieldList []int, typesList []types.Type, child DbIterator) (*Project, error) {
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
//
// Returns:
//   - *tuple.TupleDescription: The schema description for the projected output tuples
func (p *Project) GetTupleDesc() *tuple.TupleDescription {
	return p.tupleDesc
}

// Open initializes the Project operator for execution by opening its child operator
// and marking itself as opened. This method must be called before any iteration operations.
// The data flow is established from child to this projection operator.
//
// Returns:
//   - error: An error if the child operator fails to open, nil otherwise
func (p *Project) Open() error {
	if err := p.child.Open(); err != nil {
		return fmt.Errorf("failed to open child operator: %v", err)
	}

	p.base.MarkOpened()
	return nil
}

// Close releases resources held by the Project operator and its child operator.
//
// Returns:
//   - error: Always returns nil as cleanup operations don't typically fail
func (p *Project) Close() error {
	if p.child != nil {
		p.child.Close()
	}
	return p.base.Close()
}

// Rewind resets the Project operator to its initial state, allowing iteration
// to begin again from the first tuple. This involves rewinding the child operator
// and clearing any cached state in the base iterator.
//
// Returns:
//   - error: An error if the child operator fails to rewind, nil otherwise
func (p *Project) Rewind() error {
	if err := p.child.Rewind(); err != nil {
		return err
	}

	p.base.ClearCache()
	return nil
}

// HasNext checks if there are more projected tuples available for iteration.
// This method delegates to the base iterator which handles the caching logic.
//
// Returns:
//   - bool: True if more tuples are available, false otherwise
//   - error: An error if the check operation fails
func (p *Project) HasNext() (bool, error) { return p.base.HasNext() }

// Next returns the next projected tuple with only the selected fields.
// This method delegates to the base iterator which calls readNext internally.
// Should only be called after HasNext() returns true.
//
// Returns:
//   - *tuple.Tuple: The next tuple with projected fields
//   - error: An error if the iteration fails or no more tuples are available
func (p *Project) Next() (*tuple.Tuple, error) { return p.base.Next() }

// readNext is the core projection logic that reads tuples from the child operator
// and creates new tuples containing only the specified projected fields.
// This method extracts the requested fields from the child tuple and constructs
// a new tuple with the projected schema.
//
// Returns:
//   - *tuple.Tuple: The next tuple with only projected fields, or nil if no more tuples
//   - error: An error if any operation during projection fails
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

		// Set field in projected tuple
		if err := projectedTuple.SetField(i, field); err != nil {
			return nil, fmt.Errorf("failed to set field %d in projected tuple: %v", i, err)
		}
	}

	projectedTuple.RecordID = childTuple.RecordID
	return projectedTuple, nil
}
