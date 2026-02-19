package query

import (
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// TestTupleSetCollisionDetection tests that hash collisions are properly handled
func TestTupleSetCollisionDetection(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	ts := NewTupleSet(false) // Set semantics

	// Create two different tuples
	tuple1 := createSetOpTestTuple(desc, 1, "Alice")
	tuple2 := createSetOpTestTuple(desc, 2, "Bob")

	// Add both tuples
	added1 := ts.Add(tuple1)
	added2 := ts.Add(tuple2)

	if !added1 {
		t.Error("First tuple should be added")
	}
	if !added2 {
		t.Error("Second tuple should be added")
	}

	// Try to add tuple1 again - should not be added (duplicate)
	added3 := ts.Add(tuple1)
	if added3 {
		t.Error("Duplicate tuple should not be added")
	}

	// Verify both tuples are in the set
	if !ts.Contains(tuple1) {
		t.Error("Set should contain first tuple")
	}
	if !ts.Contains(tuple2) {
		t.Error("Set should contain second tuple")
	}
}

// TestTupleSetActualCollision tests handling of actual hash collisions
// This test creates tuples that might have the same hash but different values
func TestTupleSetActualCollision(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.IntType},
		[]string{"a", "b"},
	)

	ts := NewTupleSet(false)

	// Create tuples with values that might create hash collisions
	// Hash function: hash = hash*31 + fieldHash
	// We can't guarantee collisions, but we test the collision handling logic works
	tuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1, 2),
		createSetOpTestTuple(desc, 2, 1),
		createSetOpTestTuple(desc, 1, 2), // duplicate
		createSetOpTestTuple(desc, 3, 4),
	}

	// Add all tuples
	results := make([]bool, len(tuples))
	for i, tup := range tuples {
		results[i] = ts.Add(tup)
	}

	// First, second, and fourth should be added
	if !results[0] {
		t.Error("First tuple should be added")
	}
	if !results[1] {
		t.Error("Second tuple should be added")
	}
	if results[2] {
		t.Error("Third tuple (duplicate) should not be added")
	}
	if !results[3] {
		t.Error("Fourth tuple should be added")
	}

	// Verify contains works correctly
	if !ts.Contains(tuples[0]) {
		t.Error("Should contain first tuple")
	}
	if !ts.Contains(tuples[1]) {
		t.Error("Should contain second tuple")
	}
	if !ts.Contains(tuples[2]) {
		t.Error("Should contain third tuple (duplicate of first)")
	}
	if !ts.Contains(tuples[3]) {
		t.Error("Should contain fourth tuple")
	}
}

// TestTupleSetBagSemanticsWithCollisions tests bag semantics with potential collisions
func TestTupleSetBagSemanticsWithCollisions(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	ts := NewTupleSet(true) // Bag semantics

	tuple1 := createSetOpTestTuple(desc, 1, "Alice")
	tuple2 := createSetOpTestTuple(desc, 1, "Alice") // Same content
	tuple3 := createSetOpTestTuple(desc, 2, "Bob")   // Different

	// Add same tuple multiple times
	ts.Add(tuple1)
	ts.Add(tuple2) // Should increment count
	ts.Add(tuple3)

	// Verify counts
	count1 := ts.GetCount(tuple1)
	if count1 != 2 {
		t.Errorf("Expected count 2 for tuple1, got %d", count1)
	}

	count3 := ts.GetCount(tuple3)
	if count3 != 1 {
		t.Errorf("Expected count 1 for tuple3, got %d", count3)
	}
}

// TestFindTupleInList tests the findTupleInList helper function
func TestFindTupleInList(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	list := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1, "Alice"),
		createSetOpTestTuple(desc, 2, "Bob"),
		createSetOpTestTuple(desc, 3, "Charlie"),
	}

	// Test finding existing tuple
	searchTuple := createSetOpTestTuple(desc, 2, "Bob")
	idx := findTupleInList(searchTuple, list)
	if idx != 1 {
		t.Errorf("Expected index 1, got %d", idx)
	}

	// Test not finding tuple
	notFoundTuple := createSetOpTestTuple(desc, 4, "David")
	idx = findTupleInList(notFoundTuple, list)
	if idx != -1 {
		t.Errorf("Expected index -1 for not found, got %d", idx)
	}

	// Test empty list
	idx = findTupleInList(searchTuple, []*tuple.Tuple{})
	if idx != -1 {
		t.Errorf("Expected index -1 for empty list, got %d", idx)
	}
}

// TestTupleSetClearWithCollisions tests that clear properly resets collision data
func TestTupleSetClearWithCollisions(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	ts := NewTupleSet(false)

	// Add some tuples
	tuple1 := createSetOpTestTuple(desc, 1, "Alice")
	tuple2 := createSetOpTestTuple(desc, 2, "Bob")

	ts.Add(tuple1)
	ts.Add(tuple2)

	// Verify they exist
	if !ts.Contains(tuple1) {
		t.Error("Should contain tuple1 before clear")
	}

	// Clear the set
	ts.Clear()

	// Verify they're gone
	if ts.Contains(tuple1) {
		t.Error("Should not contain tuple1 after clear")
	}
	if ts.Contains(tuple2) {
		t.Error("Should not contain tuple2 after clear")
	}

	// Verify we can add them again
	added := ts.Add(tuple1)
	if !added {
		t.Error("Should be able to add tuple1 after clear")
	}
}

// TestTupleSetMultipleFieldTypes tests collision detection with various field types
func TestTupleSetMultipleFieldTypes(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType, types.IntType, types.StringType},
		[]string{"id", "name", "age", "city"},
	)

	ts := NewTupleSet(false)

	// Create tuples with same some fields but different overall
	tuple1 := createSetOpTestTuple(desc, 1, "Alice", 25, "NYC")
	tuple2 := createSetOpTestTuple(desc, 1, "Alice", 25, "LA")  // Different city
	tuple3 := createSetOpTestTuple(desc, 1, "Alice", 25, "NYC") // Same as tuple1

	// Add tuples
	added1 := ts.Add(tuple1)
	added2 := ts.Add(tuple2)
	added3 := ts.Add(tuple3)

	if !added1 {
		t.Error("First tuple should be added")
	}
	if !added2 {
		t.Error("Second tuple should be added (different city)")
	}
	if added3 {
		t.Error("Third tuple should not be added (duplicate of first)")
	}

	// Verify all are correctly tracked
	if !ts.Contains(tuple1) {
		t.Error("Should contain tuple1")
	}
	if !ts.Contains(tuple2) {
		t.Error("Should contain tuple2")
	}
	if !ts.Contains(tuple3) {
		t.Error("Should contain tuple3 (same as tuple1)")
	}
}

// TestTupleSetStressCollisions tests collision handling under stress
func TestTupleSetStressCollisions(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.IntType},
		[]string{"a", "b"},
	)

	ts := NewTupleSet(false)

	// Add many tuples with potential for collisions
	const numTuples = 1000
	var tuples []*tuple.Tuple

	// Create tuples
	for i := 0; i < numTuples; i++ {
		tup := createSetOpTestTuple(desc, i, i*2)
		tuples = append(tuples, tup)
	}

	// Add all tuples
	for _, tup := range tuples {
		added := ts.Add(tup)
		if !added {
			t.Errorf("Tuple should be added: %v", tup)
		}
	}

	// Try to add duplicates - none should be added
	for _, tup := range tuples {
		added := ts.Add(tup)
		if added {
			t.Errorf("Duplicate tuple should not be added: %v", tup)
		}
	}

	// Verify all tuples are in the set
	for _, tup := range tuples {
		if !ts.Contains(tup) {
			t.Errorf("Tuple should be in set: %v", tup)
		}
	}
}

// TestTupleSetDecrementWithCollisions tests decrement with collision handling
func TestTupleSetDecrementWithCollisions(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	ts := NewTupleSet(true) // Bag semantics

	tuple1 := createSetOpTestTuple(desc, 1, "Alice")

	// Add tuple multiple times
	ts.Add(tuple1)
	ts.Add(tuple1)
	ts.Add(tuple1)

	count := ts.GetCount(tuple1)
	if count != 3 {
		t.Errorf("Expected count 3, got %d", count)
	}

	// Decrement
	exists := ts.Decrement(tuple1)
	if !exists {
		t.Error("Tuple should still exist after first decrement")
	}

	count = ts.GetCount(tuple1)
	if count != 2 {
		t.Errorf("Expected count 2 after decrement, got %d", count)
	}

	// Decrement again
	exists = ts.Decrement(tuple1)
	if !exists {
		t.Error("Tuple should still exist after second decrement")
	}

	// Decrement to zero
	exists = ts.Decrement(tuple1)
	if exists {
		t.Error("Tuple should not exist after decrementing to zero")
	}
}

// TestTupleSetRemoveWithCollisions tests remove with collision handling
func TestTupleSetRemoveWithCollisions(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	ts := NewTupleSet(false)

	tuple1 := createSetOpTestTuple(desc, 1, "Alice")
	tuple2 := createSetOpTestTuple(desc, 2, "Bob")

	ts.Add(tuple1)
	ts.Add(tuple2)

	// Remove tuple1
	ts.Remove(tuple1)

	// Verify tuple1 is removed but tuple2 remains
	if ts.Contains(tuple1) {
		t.Error("Tuple1 should be removed")
	}
	if !ts.Contains(tuple2) {
		t.Error("Tuple2 should still exist")
	}
}

// TestHashFunctionConsistency tests that hash function is deterministic
func TestHashFunctionConsistency(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	tuple1 := createSetOpTestTuple(desc, 1, "Alice")
	tuple2 := createSetOpTestTuple(desc, 1, "Alice") // Same content

	hash1, _ := tuple1.Hash()
	hash2, _ := tuple2.Hash()

	if hash1 != hash2 {
		t.Error("Same tuples should have same hash")
	}

	// Different tuple should (probably) have different hash
	tuple3 := createSetOpTestTuple(desc, 2, "Bob")
	hash3, _ := tuple3.Hash()

	// Note: This could theoretically fail if there's a collision, but unlikely
	if hash1 == hash3 {
		t.Log("Warning: Different tuples have same hash (collision)")
	}
}

// mockIterator implements DbIterator for testing
type mockSetOpIterator struct {
	tuples    []*tuple.Tuple
	tupleDesc *tuple.TupleDescription
	pos       int
	opened    bool
}

func newMockSetOpIterator(tuples []*tuple.Tuple, desc *tuple.TupleDescription) *mockSetOpIterator {
	return &mockSetOpIterator{
		tuples:    tuples,
		tupleDesc: desc,
		pos:       -1,
		opened:    false,
	}
}

func (m *mockSetOpIterator) Open() error {
	m.opened = true
	m.pos = -1
	return nil
}

func (m *mockSetOpIterator) Close() error {
	m.opened = false
	return nil
}

func (m *mockSetOpIterator) HasNext() (bool, error) {
	if !m.opened {
		return false, nil
	}
	return m.pos+1 < len(m.tuples), nil
}

func (m *mockSetOpIterator) Next() (*tuple.Tuple, error) {
	hasNext, _ := m.HasNext()
	if !hasNext {
		return nil, nil
	}
	m.pos++
	return m.tuples[m.pos], nil
}

func (m *mockSetOpIterator) Rewind() error {
	m.pos = -1
	return nil
}

func (m *mockSetOpIterator) GetTupleDesc() *tuple.TupleDescription {
	return m.tupleDesc
}

// Helper function to create test tuples
func createSetOpTestTuple(desc *tuple.TupleDescription, values ...interface{}) *tuple.Tuple {
	tup := tuple.NewTuple(desc)
	for i, v := range values {
		var field types.Field
		switch val := v.(type) {
		case int:
			field = types.NewIntField(int64(val))
		case string:
			field = types.NewStringField(val, 50)
		}
		tup.SetField(primitives.ColumnID(i), field)
	}
	return tup
}

// Helper function to collect all tuples from an iterator
func collectTuples(iter iterator.DbIterator) ([]*tuple.Tuple, error) {
	if err := iter.Open(); err != nil {
		return nil, err
	}
	defer iter.Close()

	var results []*tuple.Tuple
	for {
		hasNext, err := iter.HasNext()
		if err != nil {
			return nil, err
		}
		if !hasNext {
			break
		}

		t, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if t != nil {
			results = append(results, t)
		}
	}
	return results, nil
}

// TestUnion tests basic UNION operation (removes duplicates)
func TestUnion(t *testing.T) {
	// Create schema: (id INT, name STRING)
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	// Left dataset: {(1, "Alice"), (2, "Bob")}
	leftTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1, "Alice"),
		createSetOpTestTuple(desc, 2, "Bob"),
	}

	// Right dataset: {(2, "Bob"), (3, "Charlie")}
	rightTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 2, "Bob"),
		createSetOpTestTuple(desc, 3, "Charlie"),
	}

	leftIter := newMockSetOpIterator(leftTuples, desc)
	rightIter := newMockSetOpIterator(rightTuples, desc)

	// Create UNION operator (removes duplicates)
	unionOp, err := NewUnion(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create UNION: %v", err)
	}

	results, err := collectTuples(unionOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: {(1, "Alice"), (2, "Bob"), (3, "Charlie")} - no duplicates
	if len(results) != 3 {
		t.Errorf("Expected 3 tuples, got %d", len(results))
	}
}

// TestUnionAll tests UNION ALL operation (keeps duplicates)
func TestUnionAll(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	leftTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1, "Alice"),
		createSetOpTestTuple(desc, 2, "Bob"),
	}

	rightTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 2, "Bob"),
		createSetOpTestTuple(desc, 3, "Charlie"),
	}

	leftIter := newMockSetOpIterator(leftTuples, desc)
	rightIter := newMockSetOpIterator(rightTuples, desc)

	// Create UNION ALL operator (keeps duplicates)
	unionAllOp, err := NewUnion(leftIter, rightIter, true)
	if err != nil {
		t.Fatalf("Failed to create UNION ALL: %v", err)
	}

	results, err := collectTuples(unionAllOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: 4 tuples (all from left and right, including duplicate)
	if len(results) != 4 {
		t.Errorf("Expected 4 tuples, got %d", len(results))
	}
}

// TestIntersect tests basic INTERSECT operation
func TestIntersect(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	// Left dataset: {(1, "Alice"), (2, "Bob"), (3, "Charlie")}
	leftTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1, "Alice"),
		createSetOpTestTuple(desc, 2, "Bob"),
		createSetOpTestTuple(desc, 3, "Charlie"),
	}

	// Right dataset: {(2, "Bob"), (3, "Charlie"), (4, "David")}
	rightTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 2, "Bob"),
		createSetOpTestTuple(desc, 3, "Charlie"),
		createSetOpTestTuple(desc, 4, "David"),
	}

	leftIter := newMockSetOpIterator(leftTuples, desc)
	rightIter := newMockSetOpIterator(rightTuples, desc)

	intersectOp, err := NewIntersect(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create INTERSECT: %v", err)
	}

	results, err := collectTuples(intersectOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: {(2, "Bob"), (3, "Charlie")} - common tuples only
	if len(results) != 2 {
		t.Errorf("Expected 2 tuples, got %d", len(results))
	}
}

// TestExcept tests basic EXCEPT operation
func TestExcept(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	// Left dataset: {(1, "Alice"), (2, "Bob"), (3, "Charlie")}
	leftTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1, "Alice"),
		createSetOpTestTuple(desc, 2, "Bob"),
		createSetOpTestTuple(desc, 3, "Charlie"),
	}

	// Right dataset: {(2, "Bob"), (4, "David")}
	rightTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 2, "Bob"),
		createSetOpTestTuple(desc, 4, "David"),
	}

	leftIter := newMockSetOpIterator(leftTuples, desc)
	rightIter := newMockSetOpIterator(rightTuples, desc)

	exceptOp, err := NewExcept(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create EXCEPT: %v", err)
	}

	results, err := collectTuples(exceptOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: {(1, "Alice"), (3, "Charlie")} - left minus right
	if len(results) != 2 {
		t.Errorf("Expected 2 tuples, got %d", len(results))
	}
}

// TestIntersectAll tests INTERSECT ALL with duplicates
func TestIntersectAll(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)

	// Left: {1, 2, 2, 3, 3, 3}
	leftTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 2),
		createSetOpTestTuple(desc, 2),
		createSetOpTestTuple(desc, 3),
		createSetOpTestTuple(desc, 3),
		createSetOpTestTuple(desc, 3),
	}

	// Right: {2, 3, 3, 4}
	rightTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 2),
		createSetOpTestTuple(desc, 3),
		createSetOpTestTuple(desc, 3),
		createSetOpTestTuple(desc, 4),
	}

	leftIter := newMockSetOpIterator(leftTuples, desc)
	rightIter := newMockSetOpIterator(rightTuples, desc)

	intersectAllOp, err := NewIntersect(leftIter, rightIter, true)
	if err != nil {
		t.Fatalf("Failed to create INTERSECT ALL: %v", err)
	}

	results, err := collectTuples(intersectAllOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: {2, 3, 3} - minimum occurrence count from both sides
	// Left has 2 twice, right has 2 once -> output 2 once
	// Left has 3 thrice, right has 3 twice -> output 3 twice
	if len(results) != 3 {
		t.Errorf("Expected 3 tuples, got %d", len(results))
	}
}

// TestExceptAll tests EXCEPT ALL with duplicates
func TestExceptAll(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)

	// Left: {1, 2, 2, 3, 3, 3}
	leftTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 2),
		createSetOpTestTuple(desc, 2),
		createSetOpTestTuple(desc, 3),
		createSetOpTestTuple(desc, 3),
		createSetOpTestTuple(desc, 3),
	}

	// Right: {2, 3, 3}
	rightTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 2),
		createSetOpTestTuple(desc, 3),
		createSetOpTestTuple(desc, 3),
	}

	leftIter := newMockSetOpIterator(leftTuples, desc)
	rightIter := newMockSetOpIterator(rightTuples, desc)

	exceptAllOp, err := NewExcept(leftIter, rightIter, true)
	if err != nil {
		t.Fatalf("Failed to create EXCEPT ALL: %v", err)
	}

	results, err := collectTuples(exceptAllOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: {1, 2, 3} - left count minus right count
	// 1 appears 1 time in left, 0 in right -> output 1 once
	// 2 appears 2 times in left, 1 in right -> output 2 once
	// 3 appears 3 times in left, 2 in right -> output 3 once
	if len(results) != 3 {
		t.Errorf("Expected 3 tuples, got %d", len(results))
	}
}

// TestSchemaMismatch tests that set operations fail with incompatible schemas
func TestSchemaMismatch(t *testing.T) {
	desc1, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	desc2, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"id"},
	)

	leftTuples := []*tuple.Tuple{createSetOpTestTuple(desc1, 1, "Alice")}
	rightTuples := []*tuple.Tuple{createSetOpTestTuple(desc2, 1)}

	leftIter := newMockSetOpIterator(leftTuples, desc1)
	rightIter := newMockSetOpIterator(rightTuples, desc2)

	// Should fail due to schema mismatch
	_, err := NewUnion(leftIter, rightIter, false)
	if err == nil {
		t.Error("Expected error for schema mismatch, got nil")
	}
}

// TestEmptyInputs tests set operations with empty inputs
func TestEmptyInputs(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)

	emptyTuples := []*tuple.Tuple{}
	nonEmptyTuples := []*tuple.Tuple{createSetOpTestTuple(desc, 1)}

	// Test UNION with empty left
	leftIter := newMockSetOpIterator(emptyTuples, desc)
	rightIter := newMockSetOpIterator(nonEmptyTuples, desc)

	unionOp, err := NewUnion(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create UNION: %v", err)
	}

	results, err := collectTuples(unionOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 tuple, got %d", len(results))
	}

	// Test INTERSECT with empty left
	leftIter = newMockSetOpIterator(emptyTuples, desc)
	rightIter = newMockSetOpIterator(nonEmptyTuples, desc)

	intersectOp, err := NewIntersect(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create INTERSECT: %v", err)
	}

	results, err = collectTuples(intersectOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 tuples, got %d", len(results))
	}
}

// TestUnionWithManyDuplicates tests UNION with multiple duplicate values
func TestUnionWithManyDuplicates(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)

	// Left: {1, 1, 1, 2, 2, 3}
	leftTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 2),
		createSetOpTestTuple(desc, 2),
		createSetOpTestTuple(desc, 3),
	}

	// Right: {1, 2, 2, 2, 4}
	rightTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 2),
		createSetOpTestTuple(desc, 2),
		createSetOpTestTuple(desc, 2),
		createSetOpTestTuple(desc, 4),
	}

	leftIter := newMockSetOpIterator(leftTuples, desc)
	rightIter := newMockSetOpIterator(rightTuples, desc)

	unionOp, err := NewUnion(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create UNION: %v", err)
	}

	results, err := collectTuples(unionOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: {1, 2, 3, 4} - all duplicates removed
	if len(results) != 4 {
		t.Errorf("Expected 4 unique tuples, got %d", len(results))
	}
}

// TestUnionAllWithManyDuplicates tests UNION ALL preserves all duplicates
func TestUnionAllWithManyDuplicates(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)

	// Left: {1, 1, 2}
	leftTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 2),
	}

	// Right: {1, 2, 2}
	rightTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 2),
		createSetOpTestTuple(desc, 2),
	}

	leftIter := newMockSetOpIterator(leftTuples, desc)
	rightIter := newMockSetOpIterator(rightTuples, desc)

	unionAllOp, err := NewUnion(leftIter, rightIter, true)
	if err != nil {
		t.Fatalf("Failed to create UNION ALL: %v", err)
	}

	results, err := collectTuples(unionAllOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: 6 tuples total (all from both sides)
	if len(results) != 6 {
		t.Errorf("Expected 6 tuples, got %d", len(results))
	}
}

// TestIntersectNoCommonElements tests INTERSECT with disjoint sets
func TestIntersectNoCommonElements(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	// Left dataset: {(1, "Alice"), (2, "Bob")}
	leftTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1, "Alice"),
		createSetOpTestTuple(desc, 2, "Bob"),
	}

	// Right dataset: {(3, "Charlie"), (4, "David")}
	rightTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 3, "Charlie"),
		createSetOpTestTuple(desc, 4, "David"),
	}

	leftIter := newMockSetOpIterator(leftTuples, desc)
	rightIter := newMockSetOpIterator(rightTuples, desc)

	intersectOp, err := NewIntersect(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create INTERSECT: %v", err)
	}

	results, err := collectTuples(intersectOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: empty result set - no common elements
	if len(results) != 0 {
		t.Errorf("Expected 0 tuples, got %d", len(results))
	}
}

// TestExceptNoCommonElements tests EXCEPT when right set has no overlap
func TestExceptNoCommonElements(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	// Left dataset: {(1, "Alice"), (2, "Bob")}
	leftTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1, "Alice"),
		createSetOpTestTuple(desc, 2, "Bob"),
	}

	// Right dataset: {(3, "Charlie"), (4, "David")}
	rightTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 3, "Charlie"),
		createSetOpTestTuple(desc, 4, "David"),
	}

	leftIter := newMockSetOpIterator(leftTuples, desc)
	rightIter := newMockSetOpIterator(rightTuples, desc)

	exceptOp, err := NewExcept(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create EXCEPT: %v", err)
	}

	results, err := collectTuples(exceptOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: all left tuples since nothing to subtract
	if len(results) != 2 {
		t.Errorf("Expected 2 tuples, got %d", len(results))
	}
}

// TestExceptAllLeftSubset tests EXCEPT when left is completely contained in right
func TestExceptAllLeftSubset(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)

	// Left: {1, 2}
	leftTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 2),
	}

	// Right: {1, 2, 3, 4}
	rightTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 2),
		createSetOpTestTuple(desc, 3),
		createSetOpTestTuple(desc, 4),
	}

	leftIter := newMockSetOpIterator(leftTuples, desc)
	rightIter := newMockSetOpIterator(rightTuples, desc)

	exceptOp, err := NewExcept(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create EXCEPT: %v", err)
	}

	results, err := collectTuples(exceptOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: empty result - all left elements are in right
	if len(results) != 0 {
		t.Errorf("Expected 0 tuples, got %d", len(results))
	}
}

// TestLargeDatasetUnion tests UNION with larger datasets
func TestLargeDatasetUnion(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)

	// Create large dataset with some overlapping values
	var leftTuples []*tuple.Tuple
	for i := 0; i < 1000; i++ {
		leftTuples = append(leftTuples, createSetOpTestTuple(desc, i))
	}

	var rightTuples []*tuple.Tuple
	for i := 500; i < 1500; i++ {
		rightTuples = append(rightTuples, createSetOpTestTuple(desc, i))
	}

	leftIter := newMockSetOpIterator(leftTuples, desc)
	rightIter := newMockSetOpIterator(rightTuples, desc)

	unionOp, err := NewUnion(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create UNION: %v", err)
	}

	results, err := collectTuples(unionOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: 1500 unique values (0-1499)
	if len(results) != 1500 {
		t.Errorf("Expected 1500 unique tuples, got %d", len(results))
	}
}

// TestLargeDatasetIntersect tests INTERSECT with larger datasets
func TestLargeDatasetIntersect(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)

	// Left: 0-999
	var leftTuples []*tuple.Tuple
	for i := 0; i < 1000; i++ {
		leftTuples = append(leftTuples, createSetOpTestTuple(desc, i))
	}

	// Right: 500-1499
	var rightTuples []*tuple.Tuple
	for i := 500; i < 1500; i++ {
		rightTuples = append(rightTuples, createSetOpTestTuple(desc, i))
	}

	leftIter := newMockSetOpIterator(leftTuples, desc)
	rightIter := newMockSetOpIterator(rightTuples, desc)

	intersectOp, err := NewIntersect(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create INTERSECT: %v", err)
	}

	results, err := collectTuples(intersectOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: 500 common values (500-999)
	if len(results) != 500 {
		t.Errorf("Expected 500 tuples, got %d", len(results))
	}
}

// TestMultiColumnSetOperations tests set operations with multiple columns
func TestMultiColumnSetOperations(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType, types.IntType},
		[]string{"id", "name", "age"},
	)

	// Left dataset
	leftTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1, "Alice", 25),
		createSetOpTestTuple(desc, 2, "Bob", 30),
		createSetOpTestTuple(desc, 3, "Charlie", 35),
	}

	// Right dataset
	rightTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 2, "Bob", 30),
		createSetOpTestTuple(desc, 3, "Charlie", 35),
		createSetOpTestTuple(desc, 4, "David", 40),
	}

	// Test UNION
	leftIter := newMockSetOpIterator(leftTuples, desc)
	rightIter := newMockSetOpIterator(rightTuples, desc)
	unionOp, err := NewUnion(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create UNION: %v", err)
	}
	results, err := collectTuples(unionOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}
	if len(results) != 4 {
		t.Errorf("UNION: Expected 4 tuples, got %d", len(results))
	}

	// Test INTERSECT
	leftIter = newMockSetOpIterator(leftTuples, desc)
	rightIter = newMockSetOpIterator(rightTuples, desc)
	intersectOp, err := NewIntersect(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create INTERSECT: %v", err)
	}
	results, err = collectTuples(intersectOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("INTERSECT: Expected 2 tuples, got %d", len(results))
	}

	// Test EXCEPT
	leftIter = newMockSetOpIterator(leftTuples, desc)
	rightIter = newMockSetOpIterator(rightTuples, desc)
	exceptOp, err := NewExcept(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create EXCEPT: %v", err)
	}
	results, err = collectTuples(exceptOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("EXCEPT: Expected 1 tuple, got %d", len(results))
	}
}

// TestIteratorRewind tests that iterators can be rewound
func TestIteratorRewind(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)

	tuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 2),
		createSetOpTestTuple(desc, 3),
	}

	mockIter := newMockSetOpIterator(tuples, desc)

	// First pass
	results1, err := collectTuples(mockIter)
	if err != nil {
		t.Fatalf("First pass failed: %v", err)
	}
	if len(results1) != 3 {
		t.Errorf("First pass: Expected 3 tuples, got %d", len(results1))
	}

	// Rewind and second pass
	if err := mockIter.Rewind(); err != nil {
		t.Fatalf("Rewind failed: %v", err)
	}
	results2, err := collectTuples(mockIter)
	if err != nil {
		t.Fatalf("Second pass failed: %v", err)
	}
	if len(results2) != 3 {
		t.Errorf("Second pass: Expected 3 tuples, got %d", len(results2))
	}
}

// TestBothInputsEmpty tests set operations when both inputs are empty
func TestBothInputsEmpty(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)

	emptyTuples := []*tuple.Tuple{}

	leftIter := newMockSetOpIterator(emptyTuples, desc)
	rightIter := newMockSetOpIterator(emptyTuples, desc)

	// Test UNION
	unionOp, err := NewUnion(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create UNION: %v", err)
	}
	results, err := collectTuples(unionOp)
	if err != nil {
		t.Fatalf("UNION failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("UNION: Expected 0 tuples, got %d", len(results))
	}

	// Test INTERSECT
	leftIter = newMockSetOpIterator(emptyTuples, desc)
	rightIter = newMockSetOpIterator(emptyTuples, desc)
	intersectOp, err := NewIntersect(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create INTERSECT: %v", err)
	}
	results, err = collectTuples(intersectOp)
	if err != nil {
		t.Fatalf("INTERSECT failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("INTERSECT: Expected 0 tuples, got %d", len(results))
	}

	// Test EXCEPT
	leftIter = newMockSetOpIterator(emptyTuples, desc)
	rightIter = newMockSetOpIterator(emptyTuples, desc)
	exceptOp, err := NewExcept(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create EXCEPT: %v", err)
	}
	results, err = collectTuples(exceptOp)
	if err != nil {
		t.Fatalf("EXCEPT failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("EXCEPT: Expected 0 tuples, got %d", len(results))
	}
}

// TestSingleElementSets tests set operations with single element sets
func TestSingleElementSets(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)

	leftTuples := []*tuple.Tuple{createSetOpTestTuple(desc, 1)}
	rightTuples := []*tuple.Tuple{createSetOpTestTuple(desc, 1)}

	// Test UNION with identical single elements
	leftIter := newMockSetOpIterator(leftTuples, desc)
	rightIter := newMockSetOpIterator(rightTuples, desc)
	unionOp, err := NewUnion(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create UNION: %v", err)
	}
	results, err := collectTuples(unionOp)
	if err != nil {
		t.Fatalf("UNION failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("UNION: Expected 1 tuple, got %d", len(results))
	}

	// Test INTERSECT with identical single elements
	leftIter = newMockSetOpIterator(leftTuples, desc)
	rightIter = newMockSetOpIterator(rightTuples, desc)
	intersectOp, err := NewIntersect(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create INTERSECT: %v", err)
	}
	results, err = collectTuples(intersectOp)
	if err != nil {
		t.Fatalf("INTERSECT failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("INTERSECT: Expected 1 tuple, got %d", len(results))
	}

	// Test EXCEPT with identical single elements
	leftIter = newMockSetOpIterator(leftTuples, desc)
	rightIter = newMockSetOpIterator(rightTuples, desc)
	exceptOp, err := NewExcept(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create EXCEPT: %v", err)
	}
	results, err = collectTuples(exceptOp)
	if err != nil {
		t.Fatalf("EXCEPT failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("EXCEPT: Expected 0 tuples, got %d", len(results))
	}
}

// TestPartialOverlaps tests set operations with partial overlapping datasets
func TestPartialOverlaps(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	// Left: {(1, "A"), (2, "B"), (3, "C"), (4, "D")}
	leftTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1, "A"),
		createSetOpTestTuple(desc, 2, "B"),
		createSetOpTestTuple(desc, 3, "C"),
		createSetOpTestTuple(desc, 4, "D"),
	}

	// Right: {(3, "C"), (4, "D"), (5, "E"), (6, "F")}
	rightTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 3, "C"),
		createSetOpTestTuple(desc, 4, "D"),
		createSetOpTestTuple(desc, 5, "E"),
		createSetOpTestTuple(desc, 6, "F"),
	}

	// Test UNION - should give all unique elements
	leftIter := newMockSetOpIterator(leftTuples, desc)
	rightIter := newMockSetOpIterator(rightTuples, desc)
	unionOp, err := NewUnion(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create UNION: %v", err)
	}
	results, err := collectTuples(unionOp)
	if err != nil {
		t.Fatalf("UNION failed: %v", err)
	}
	if len(results) != 6 {
		t.Errorf("UNION: Expected 6 tuples, got %d", len(results))
	}

	// Test INTERSECT - should give only common elements
	leftIter = newMockSetOpIterator(leftTuples, desc)
	rightIter = newMockSetOpIterator(rightTuples, desc)
	intersectOp, err := NewIntersect(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create INTERSECT: %v", err)
	}
	results, err = collectTuples(intersectOp)
	if err != nil {
		t.Fatalf("INTERSECT failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("INTERSECT: Expected 2 tuples, got %d", len(results))
	}

	// Test EXCEPT - should give left-only elements
	leftIter = newMockSetOpIterator(leftTuples, desc)
	rightIter = newMockSetOpIterator(rightTuples, desc)
	exceptOp, err := NewExcept(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create EXCEPT: %v", err)
	}
	results, err = collectTuples(exceptOp)
	if err != nil {
		t.Fatalf("EXCEPT failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("EXCEPT: Expected 2 tuples, got %d", len(results))
	}
}

// TestStringDataTypes tests set operations with string-only data
func TestStringDataTypes(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.StringType},
		[]string{"word"},
	)

	// Left: {"apple", "banana", "cherry"}
	leftTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, "apple"),
		createSetOpTestTuple(desc, "banana"),
		createSetOpTestTuple(desc, "cherry"),
	}

	// Right: {"banana", "cherry", "date", "elderberry"}
	rightTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, "banana"),
		createSetOpTestTuple(desc, "cherry"),
		createSetOpTestTuple(desc, "date"),
		createSetOpTestTuple(desc, "elderberry"),
	}

	// Test UNION
	leftIter := newMockSetOpIterator(leftTuples, desc)
	rightIter := newMockSetOpIterator(rightTuples, desc)
	unionOp, err := NewUnion(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create UNION: %v", err)
	}
	results, err := collectTuples(unionOp)
	if err != nil {
		t.Fatalf("UNION failed: %v", err)
	}
	if len(results) != 5 {
		t.Errorf("UNION: Expected 5 unique strings, got %d", len(results))
	}

	// Test INTERSECT
	leftIter = newMockSetOpIterator(leftTuples, desc)
	rightIter = newMockSetOpIterator(rightTuples, desc)
	intersectOp, err := NewIntersect(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create INTERSECT: %v", err)
	}
	results, err = collectTuples(intersectOp)
	if err != nil {
		t.Fatalf("INTERSECT failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("INTERSECT: Expected 2 common strings, got %d", len(results))
	}

	// Test EXCEPT
	leftIter = newMockSetOpIterator(leftTuples, desc)
	rightIter = newMockSetOpIterator(rightTuples, desc)
	exceptOp, err := NewExcept(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create EXCEPT: %v", err)
	}
	results, err = collectTuples(exceptOp)
	if err != nil {
		t.Fatalf("EXCEPT failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("EXCEPT: Expected 1 string, got %d", len(results))
	}
}

// TestMixedDataTypes tests set operations with various data types
func TestMixedDataTypes(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType, types.IntType, types.StringType},
		[]string{"id", "name", "age", "city"},
	)

	// Left dataset
	leftTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1, "Alice", 25, "NYC"),
		createSetOpTestTuple(desc, 2, "Bob", 30, "LA"),
		createSetOpTestTuple(desc, 3, "Charlie", 35, "Chicago"),
	}

	// Right dataset
	rightTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 2, "Bob", 30, "LA"),
		createSetOpTestTuple(desc, 3, "Charlie", 35, "Chicago"),
		createSetOpTestTuple(desc, 4, "David", 40, "SF"),
		createSetOpTestTuple(desc, 5, "Eve", 28, "Boston"),
	}

	// Test UNION
	leftIter := newMockSetOpIterator(leftTuples, desc)
	rightIter := newMockSetOpIterator(rightTuples, desc)
	unionOp, err := NewUnion(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create UNION: %v", err)
	}
	results, err := collectTuples(unionOp)
	if err != nil {
		t.Fatalf("UNION failed: %v", err)
	}
	if len(results) != 5 {
		t.Errorf("UNION: Expected 5 tuples, got %d", len(results))
	}

	// Test INTERSECT
	leftIter = newMockSetOpIterator(leftTuples, desc)
	rightIter = newMockSetOpIterator(rightTuples, desc)
	intersectOp, err := NewIntersect(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create INTERSECT: %v", err)
	}
	results, err = collectTuples(intersectOp)
	if err != nil {
		t.Fatalf("INTERSECT failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("INTERSECT: Expected 2 tuples, got %d", len(results))
	}

	// Test EXCEPT
	leftIter = newMockSetOpIterator(leftTuples, desc)
	rightIter = newMockSetOpIterator(rightTuples, desc)
	exceptOp, err := NewExcept(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create EXCEPT: %v", err)
	}
	results, err = collectTuples(exceptOp)
	if err != nil {
		t.Fatalf("EXCEPT failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("EXCEPT: Expected 1 tuple, got %d", len(results))
	}
}

// TestIntersectAllWithDuplicates tests INTERSECT ALL preserves duplicates correctly
func TestIntersectAllWithDuplicates(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)

	// Left: {1, 1, 1, 2, 2, 3}
	leftTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 2),
		createSetOpTestTuple(desc, 2),
		createSetOpTestTuple(desc, 3),
	}

	// Right: {1, 1, 2, 2, 2, 4}
	rightTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 2),
		createSetOpTestTuple(desc, 2),
		createSetOpTestTuple(desc, 2),
		createSetOpTestTuple(desc, 4),
	}

	leftIter := newMockSetOpIterator(leftTuples, desc)
	rightIter := newMockSetOpIterator(rightTuples, desc)

	intersectAllOp, err := NewIntersect(leftIter, rightIter, true)
	if err != nil {
		t.Fatalf("Failed to create INTERSECT ALL: %v", err)
	}

	results, err := collectTuples(intersectAllOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: min(3,2) ones + min(2,3) twos = 2 + 2 = 4 tuples
	if len(results) != 4 {
		t.Errorf("INTERSECT ALL: Expected 4 tuples, got %d", len(results))
	}
}

// TestExceptAllWithDuplicates tests EXCEPT ALL handles duplicates correctly
func TestExceptAllWithDuplicates(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)

	// Left: {1, 1, 1, 2, 2, 3}
	leftTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 2),
		createSetOpTestTuple(desc, 2),
		createSetOpTestTuple(desc, 3),
	}

	// Right: {1, 1, 2}
	rightTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 2),
	}

	leftIter := newMockSetOpIterator(leftTuples, desc)
	rightIter := newMockSetOpIterator(rightTuples, desc)

	exceptAllOp, err := NewExcept(leftIter, rightIter, true)
	if err != nil {
		t.Fatalf("Failed to create EXCEPT ALL: %v", err)
	}

	results, err := collectTuples(exceptAllOp)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: (3-2) ones + (2-1) twos + 1 three = 1 + 1 + 1 = 3 tuples
	if len(results) != 3 {
		t.Errorf("EXCEPT ALL: Expected 3 tuples, got %d", len(results))
	}
}

// TestChainedUnionOperations tests chaining multiple UNION operations
func TestChainedUnionOperations(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)

	// Set 1: {1, 2}
	set1 := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 2),
	}

	// Set 2: {2, 3}
	set2 := []*tuple.Tuple{
		createSetOpTestTuple(desc, 2),
		createSetOpTestTuple(desc, 3),
	}

	// Set 3: {3, 4}
	set3 := []*tuple.Tuple{
		createSetOpTestTuple(desc, 3),
		createSetOpTestTuple(desc, 4),
	}

	// First UNION: set1 ∪ set2
	iter1 := newMockSetOpIterator(set1, desc)
	iter2 := newMockSetOpIterator(set2, desc)
	union1, err := NewUnion(iter1, iter2, false)
	if err != nil {
		t.Fatalf("Failed to create first UNION: %v", err)
	}

	// Second UNION: (set1 ∪ set2) ∪ set3
	iter3 := newMockSetOpIterator(set3, desc)
	union2, err := NewUnion(union1, iter3, false)
	if err != nil {
		t.Fatalf("Failed to create second UNION: %v", err)
	}

	results, err := collectTuples(union2)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: {1, 2, 3, 4}
	if len(results) != 4 {
		t.Errorf("Chained UNION: Expected 4 unique tuples, got %d", len(results))
	}
}

// TestChainedIntersectOperations tests chaining multiple INTERSECT operations
func TestChainedIntersectOperations(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)

	// Set 1: {1, 2, 3, 4, 5}
	set1 := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 2),
		createSetOpTestTuple(desc, 3),
		createSetOpTestTuple(desc, 4),
		createSetOpTestTuple(desc, 5),
	}

	// Set 2: {2, 3, 4, 5, 6}
	set2 := []*tuple.Tuple{
		createSetOpTestTuple(desc, 2),
		createSetOpTestTuple(desc, 3),
		createSetOpTestTuple(desc, 4),
		createSetOpTestTuple(desc, 5),
		createSetOpTestTuple(desc, 6),
	}

	// Set 3: {3, 4, 5, 6, 7}
	set3 := []*tuple.Tuple{
		createSetOpTestTuple(desc, 3),
		createSetOpTestTuple(desc, 4),
		createSetOpTestTuple(desc, 5),
		createSetOpTestTuple(desc, 6),
		createSetOpTestTuple(desc, 7),
	}

	// First INTERSECT: set1 ∩ set2
	iter1 := newMockSetOpIterator(set1, desc)
	iter2 := newMockSetOpIterator(set2, desc)
	intersect1, err := NewIntersect(iter1, iter2, false)
	if err != nil {
		t.Fatalf("Failed to create first INTERSECT: %v", err)
	}

	// Second INTERSECT: (set1 ∩ set2) ∩ set3
	iter3 := newMockSetOpIterator(set3, desc)
	intersect2, err := NewIntersect(intersect1, iter3, false)
	if err != nil {
		t.Fatalf("Failed to create second INTERSECT: %v", err)
	}

	results, err := collectTuples(intersect2)
	if err != nil {
		t.Fatalf("Failed to collect tuples: %v", err)
	}

	// Expected: {3, 4, 5} - common to all three sets
	if len(results) != 3 {
		t.Errorf("Chained INTERSECT: Expected 3 tuples, got %d", len(results))
	}
}

// TestIdenticalInputSets tests set operations with identical input sets
func TestIdenticalInputSets(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	// Both sets: {(1, "A"), (2, "B"), (3, "C")}
	tuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1, "A"),
		createSetOpTestTuple(desc, 2, "B"),
		createSetOpTestTuple(desc, 3, "C"),
	}

	// Test UNION - should deduplicate
	leftIter := newMockSetOpIterator(tuples, desc)
	rightIter := newMockSetOpIterator(tuples, desc)
	unionOp, err := NewUnion(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create UNION: %v", err)
	}
	results, err := collectTuples(unionOp)
	if err != nil {
		t.Fatalf("UNION failed: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("UNION: Expected 3 tuples (deduplicated), got %d", len(results))
	}

	// Test INTERSECT - should return all elements
	leftIter = newMockSetOpIterator(tuples, desc)
	rightIter = newMockSetOpIterator(tuples, desc)
	intersectOp, err := NewIntersect(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create INTERSECT: %v", err)
	}
	results, err = collectTuples(intersectOp)
	if err != nil {
		t.Fatalf("INTERSECT failed: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("INTERSECT: Expected 3 tuples, got %d", len(results))
	}

	// Test EXCEPT - should return empty
	leftIter = newMockSetOpIterator(tuples, desc)
	rightIter = newMockSetOpIterator(tuples, desc)
	exceptOp, err := NewExcept(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create EXCEPT: %v", err)
	}
	results, err = collectTuples(exceptOp)
	if err != nil {
		t.Fatalf("EXCEPT failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("EXCEPT: Expected 0 tuples, got %d", len(results))
	}
}

// TestAsymmetricSetSizes tests operations with very different sized inputs
func TestAsymmetricSetSizes(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)

	// Small left set: {1, 2}
	leftTuples := []*tuple.Tuple{
		createSetOpTestTuple(desc, 1),
		createSetOpTestTuple(desc, 2),
	}

	// Large right set: 100 elements (50-149)
	var rightTuples []*tuple.Tuple
	for i := 50; i < 150; i++ {
		rightTuples = append(rightTuples, createSetOpTestTuple(desc, i))
	}

	// Test UNION - should have all unique elements
	leftIter := newMockSetOpIterator(leftTuples, desc)
	rightIter := newMockSetOpIterator(rightTuples, desc)
	unionOp, err := NewUnion(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create UNION: %v", err)
	}
	results, err := collectTuples(unionOp)
	if err != nil {
		t.Fatalf("UNION failed: %v", err)
	}
	if len(results) != 102 {
		t.Errorf("UNION: Expected 102 tuples, got %d", len(results))
	}

	// Test INTERSECT - should be empty (no overlap)
	leftIter = newMockSetOpIterator(leftTuples, desc)
	rightIter = newMockSetOpIterator(rightTuples, desc)
	intersectOp, err := NewIntersect(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create INTERSECT: %v", err)
	}
	results, err = collectTuples(intersectOp)
	if err != nil {
		t.Fatalf("INTERSECT failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("INTERSECT: Expected 0 tuples, got %d", len(results))
	}

	// Test EXCEPT - should return all left elements
	leftIter = newMockSetOpIterator(leftTuples, desc)
	rightIter = newMockSetOpIterator(rightTuples, desc)
	exceptOp, err := NewExcept(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create EXCEPT: %v", err)
	}
	results, err = collectTuples(exceptOp)
	if err != nil {
		t.Fatalf("EXCEPT failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("EXCEPT: Expected 2 tuples, got %d", len(results))
	}
}

// TestRightHeavySet tests when right set is much larger than left
func TestRightHeavySet(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"value"},
	)

	// Left: {1, 2, 3, 4, 5}
	var leftTuples []*tuple.Tuple
	for i := 1; i <= 5; i++ {
		leftTuples = append(leftTuples, createSetOpTestTuple(desc, i))
	}

	// Right: {3, 4, 5, ..., 102} - 100 elements with 3 overlapping
	var rightTuples []*tuple.Tuple
	for i := 3; i <= 102; i++ {
		rightTuples = append(rightTuples, createSetOpTestTuple(desc, i))
	}

	// Test UNION
	leftIter := newMockSetOpIterator(leftTuples, desc)
	rightIter := newMockSetOpIterator(rightTuples, desc)
	unionOp, err := NewUnion(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create UNION: %v", err)
	}
	results, err := collectTuples(unionOp)
	if err != nil {
		t.Fatalf("UNION failed: %v", err)
	}
	if len(results) != 102 {
		t.Errorf("UNION: Expected 102 tuples, got %d", len(results))
	}

	// Test INTERSECT - should have 3 common elements
	leftIter = newMockSetOpIterator(leftTuples, desc)
	rightIter = newMockSetOpIterator(rightTuples, desc)
	intersectOp, err := NewIntersect(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create INTERSECT: %v", err)
	}
	results, err = collectTuples(intersectOp)
	if err != nil {
		t.Fatalf("INTERSECT failed: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("INTERSECT: Expected 3 tuples, got %d", len(results))
	}

	// Test EXCEPT - should have 2 elements (1, 2)
	leftIter = newMockSetOpIterator(leftTuples, desc)
	rightIter = newMockSetOpIterator(rightTuples, desc)
	exceptOp, err := NewExcept(leftIter, rightIter, false)
	if err != nil {
		t.Fatalf("Failed to create EXCEPT: %v", err)
	}
	results, err = collectTuples(exceptOp)
	if err != nil {
		t.Fatalf("EXCEPT failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("EXCEPT: Expected 2 tuples, got %d", len(results))
	}
}
