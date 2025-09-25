package query

import (
	"fmt"
	"storemy/pkg/iterator"
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
	child     iterator.DbIterator     // Where we get tuples from
	tupleDesc *tuple.TupleDescription // Schema of our output tuples
}

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

func (p *Project) GetTupleDesc() *tuple.TupleDescription {
	return p.tupleDesc
}

func (p *Project) Open() error {
	if err := p.child.Open(); err != nil {
		return fmt.Errorf("failed to open child operator: %v", err)
	}

	p.base.MarkOpened()
	return nil
}

func (p *Project) Close() error {
	if p.child != nil {
		p.child.Close()
	}
	return p.base.Close()
}

func (p *Project) Rewind() error {
	if err := p.child.Rewind(); err != nil {
		return err
	}

	p.base.ClearCache()
	return nil
}

func (p *Project) HasNext() (bool, error) { return p.base.HasNext() }

func (p *Project) Next() (*tuple.Tuple, error) { return p.base.Next() }

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
