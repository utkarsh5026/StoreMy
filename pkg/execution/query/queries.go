package query

import (
	"fmt"
	"sort"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// LimitOperator implements SQL LIMIT and OFFSET functionality.
// It restricts the number of tuples returned by a query and allows
// skipping a specified number of tuples from the beginning.
//
// Example: SELECT * FROM users LIMIT 10 OFFSET 5
// Returns 10 tuples starting from the 6th tuple.
type LimitOperator struct {
	*iterator.UnaryOperator
	limit, offset, count primitives.RowID
}

// NewLimitOperator creates a new LimitOperator instance.
//
// Parameters:
//   - child: The underlying iterator that provides tuples
//   - limit: Maximum number of tuples to return (must be non-negative)
//   - offset: Number of tuples to skip from the beginning (must be non-negative)
func NewLimitOperator(child iterator.DbIterator, limit, offset primitives.RowID) (*LimitOperator, error) {
	if err := validateChild(child); err != nil {
		return nil, err
	}

	l := &LimitOperator{
		limit:  limit,
		offset: offset,
	}

	unaryOp, err := iterator.NewUnaryOperator(child, l.readNext)
	if err != nil {
		return nil, err
	}
	l.UnaryOperator = unaryOp

	return l, nil
}

// Open initializes the limit operator and skips the offset tuples.
// This method must be called before fetching any tuples.
//
// Returns:
//   - error: If opening the child operator fails or if an error occurs while skipping offset tuples
func (l *LimitOperator) Open() error {
	if err := l.UnaryOperator.Open(); err != nil {
		return err
	}

	l.count = 0
	return l.skipOffset()
}

// readNext retrieves the next tuple within the limit range.
// It returns nil when the limit has been reached.
//
// Returns:
//   - *tuple.Tuple: The next tuple, or nil if limit is reached or no more tuples available
//   - error: If an error occurs while fetching the next tuple
func (l *LimitOperator) readNext() (*tuple.Tuple, error) {
	if l.count >= l.limit {
		return nil, nil
	}

	t, err := l.FetchNext()
	if err != nil || t == nil {
		return t, err
	}

	l.count++
	return t, nil
}

// Rewind resets the limit operator to its initial state.
// After rewinding, the operator will skip offset tuples again
// and start returning tuples from the beginning.
//
// Returns:
//   - error: If rewinding the child operator fails or if an error occurs while skipping offset tuples
func (l *LimitOperator) Rewind() error {
	l.count = 0

	if err := l.UnaryOperator.Rewind(); err != nil {
		return err
	}

	return l.skipOffset()
}

// skipOffset advances the child operator by the number of tuples specified
// by the offset value, discarding those tuples. This prepares the limit
// operator so the next retrieved tuple is the first one after the offset.
// If there are fewer tuples than the offset value, it stops early.
//
// Returns:
//   - error: If an error occurs while fetching the next tuple from the child operator.
func (l *LimitOperator) skipOffset() error {
	var i primitives.RowID
	for i = 0; i < l.offset; i++ {
		t, err := l.FetchNext()
		if err != nil {
			return err
		}
		if t == nil {
			break
		}
	}
	return nil
}

// Filter represents a filtering operator that applies a predicate to each tuple
// from its source operator, only returning tuples that satisfy the predicate condition.
type Filter struct {
	*iterator.UnaryOperator
	predicate *Predicate
}

// NewFilter creates a new Filter operator with the specified predicate and source iterator.
// The Filter will evaluate the predicate against each tuple from the source operator,
// passing through only those tuples that satisfy the condition.
func NewFilter(predicate *Predicate, source iterator.DbIterator) (*Filter, error) {
	if err := validateChild(source); err != nil {
		return nil, err
	}

	f := &Filter{
		predicate: predicate,
	}

	unaryOp, err := iterator.NewUnaryOperator(source, f.readNext)
	if err != nil {
		return nil, err
	}
	f.UnaryOperator = unaryOp

	return f, nil
}

// readNext is the internal method that implements the filtering logic.
// It continuously reads tuples from the source operator and evaluates them
// against the predicate until it finds a tuple that satisfies the condition
// or reaches the end of the input stream.
func (f *Filter) readNext() (*tuple.Tuple, error) {
	for {
		t, err := f.FetchNext()
		if err != nil || t == nil {
			return t, err
		}

		passes, err := f.predicate.Filter(t)
		if err != nil {
			return nil, fmt.Errorf("predicate evaluation failed: %v", err)
		}

		if passes {
			return t, nil
		}
	}
}

// Project implements column selection - it chooses which fields to output from input tuples.
// This operator corresponds to the SELECT clause in SQL queries, allowing users to specify
// exactly which columns should appear in the result set.
//
// Conceptually: SELECT col1, col3, col5 FROM table
type Project struct {
	*iterator.UnaryOperator
	projectedCols  []primitives.ColumnID
	projectedTypes []types.Type
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

	p := &Project{
		projectedCols:  projectedCols,
		projectedTypes: projectedTypes,
		tupleDesc:      tupleDesc,
	}

	unaryOp, err := iterator.NewUnaryOperator(source, p.readNext)
	if err != nil {
		return nil, err
	}
	p.UnaryOperator = unaryOp

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

// readNext is the internal method that implements the projection logic.
// It reads the next tuple from the source operator and creates a new tuple
// containing only the projected fields in the specified order.
func (p *Project) readNext() (*tuple.Tuple, error) {
	t, err := p.FetchNext()
	if err != nil || t == nil {
		return t, err
	}

	projected := tuple.NewTuple(p.tupleDesc)
	for i, fieldIndex := range p.projectedCols {
		field, err := t.GetField(fieldIndex)
		if err != nil {
			return nil, fmt.Errorf("failed to get field %d from source tuple: %v", fieldIndex, err)
		}

		if err := projected.SetField(primitives.ColumnID(i), field); err != nil {
			return nil, fmt.Errorf("failed to set field %d in projected tuple: %v", i, err)
		}
	}

	projected.RecordID = t.RecordID
	return projected, nil
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

// Sort operator orders tuples by a specified field in ascending or descending order.
type Sort struct {
	base         *iterator.BaseIterator
	child        iterator.DbIterator
	sorted       *iterator.SliceIterator[*tuple.Tuple]
	sortField    primitives.ColumnID // Index of field to sort by
	ascending    bool                // Sort direction: true = ASC, false = DESC
	opened       bool
	materialized bool // Flag to track if tuples have been materialized
}

// NewSort creates a new Sort operator that orders tuples by the specified field.
//
// Parameters:
//   - child: Input iterator providing tuples to sort
//   - sortField: Index of field to sort by (0-based)
//   - ascending: true for ASC, false for DESC
func NewSort(child iterator.DbIterator, sortField primitives.ColumnID, ascending bool) (*Sort, error) {
	if err := validateChild(child); err != nil {
		return nil, err
	}

	td := child.GetTupleDesc()
	if sortField >= td.NumFields() {
		return nil, fmt.Errorf("sort field index %d out of bounds (schema has %d fields)",
			sortField, td.NumFields())
	}

	s := &Sort{
		child:     child,
		sortField: sortField,
		ascending: ascending,
	}

	s.base = iterator.NewBaseIterator(s.readNext)
	return s, nil
}

// materializeTuples reads all tuples from source and sorts them.
// This is called once during Open() to prepare the sorted data.
func (s *Sort) materializeTuples() error {
	if s.materialized {
		return nil
	}

	tuples := make([]*tuple.Tuple, 0)
	for {
		hasNext, err := s.child.HasNext()
		if err != nil {
			return fmt.Errorf("error checking if child has next: %w", err)
		}
		if !hasNext {
			break
		}

		t, err := s.child.Next()
		if err != nil {
			return fmt.Errorf("error fetching tuple from source: %w", err)
		}

		if t == nil {
			break
		}

		tuples = append(tuples, t)
	}

	if err := s.sortTuples(tuples); err != nil {
		return fmt.Errorf("error sorting tuples: %w", err)
	}

	s.sorted = iterator.NewSliceIterator(tuples)
	s.materialized = true
	return nil
}

// sortTuples sorts the slice of tuples by the sort field using comparison.
func (s *Sort) sortTuples(tuples []*tuple.Tuple) error {
	var sortErr error

	sort.Slice(tuples, func(i, j int) bool {
		if sortErr != nil {
			return false
		}

		lessThan, err := s.compare(tuples, i, j)
		if err != nil {
			sortErr = fmt.Errorf("failed to compare fields: %w", err)
			return false
		}

		return lessThan
	})

	return sortErr
}

// compare compares two tuples at indices i and j based on the sort field.
// It extracts the sort field from both tuples and performs a compares them
//
// Parameters:
//   - tuples: slice of tuples to compare
//   - i: index of the first tuple
//   - j: index of the second tuple
//
// Returns:
//   - bool: true if the first tuple should come before the second in sort order
//   - error: any error encountered during field retrieval or comparison
func (s *Sort) compare(tuples []*tuple.Tuple, i, j int) (bool, error) {
	field1, err := tuples[i].GetField(s.sortField)
	if err != nil || field1 == nil {
		return false, fmt.Errorf("failed to get sort field from tuple %d: %w", i, err)
	}

	field2, err := tuples[j].GetField(s.sortField)
	if err != nil || field2 == nil {
		return false, fmt.Errorf("failed to get sort field from tuple %d: %w", j, err)
	}

	var lessThan bool
	if s.ascending {
		lessThan, err = field1.Compare(primitives.LessThan, field2)
	} else {
		lessThan, err = field2.Compare(primitives.LessThan, field1)
	}

	return lessThan, err
}

// readNext returns the next tuple from the sorted slice.
func (s *Sort) readNext() (*tuple.Tuple, error) {
	if !s.materialized {
		if err := s.materializeTuples(); err != nil {
			return nil, err
		}
	}

	if s.sorted.Remaining() == 0 {
		return nil, nil
	}

	return s.sorted.Next()
}

// Open initializes the Sort operator and materializes all tuples.
//
// This is a blocking operation that reads all input tuples and sorts them
// before returning. Must be called before HasNext/Next can be used.
func (s *Sort) Open() error {
	if err := s.child.Open(); err != nil {
		return fmt.Errorf("failed to open child operator: %w", err)
	}

	s.opened = true
	s.materialized = false

	s.base.MarkOpened()
	return nil
}

// Close releases resources held by the Sort operator and its child.
func (s *Sort) Close() error {
	s.opened = false
	s.materialized = false

	if s.sorted != nil {
		s.sorted = iterator.NewSliceIterator([]*tuple.Tuple{nil})
	}

	if err := s.child.Close(); err != nil {
		return fmt.Errorf("failed to close source operator: %w", err)
	}

	return s.base.Close()
}

// HasNext checks if there are more sorted tuples available.
func (s *Sort) HasNext() (bool, error) {
	if !s.opened {
		return false, fmt.Errorf("sort operator not opened")
	}
	return s.base.HasNext()
}

// Next returns the next sorted tuple.
func (s *Sort) Next() (*tuple.Tuple, error) {
	if !s.opened {
		return nil, fmt.Errorf("sort operator not opened")
	}
	return s.base.Next()
}

// Rewind resets the Sort operator to the beginning of the sorted results.
// Does not re-sort; just resets the read position.
func (s *Sort) Rewind() error {
	if !s.opened {
		return fmt.Errorf("sort operator not opened")
	}

	if s.sorted != nil {
		if err := s.sorted.Rewind(); err != nil {
			return err
		}
	}

	return s.base.Rewind()
}

// GetTupleDesc returns the tuple descriptor (schema) from the source.
// Sort does not modify the schema, only the order of tuples.
func (s *Sort) GetTupleDesc() *tuple.TupleDescription {
	return s.child.GetTupleDesc()
}

// validateChild performs basic validation on the child operator for sorting.
// Checks that the child is not nil and has a valid tuple descriptor.
func validateChild(child iterator.DbIterator) error {
	if child == nil {
		return fmt.Errorf("child operator cannot be nil")
	}

	if child.GetTupleDesc() == nil {
		return fmt.Errorf("child operator has nil tuple descriptor")
	}

	return nil
}
