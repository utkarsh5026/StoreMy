package setops

import (
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

// TestTupleEqualityFunction tests the tuplesEqual helper function
func TestTupleEqualityFunction(t *testing.T) {
	desc, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	tuple1 := createSetOpTestTuple(desc, 1, "Alice")
	tuple2 := createSetOpTestTuple(desc, 1, "Alice") // Same values
	tuple3 := createSetOpTestTuple(desc, 1, "Bob")   // Different name
	tuple4 := createSetOpTestTuple(desc, 2, "Alice") // Different id

	// Test equality
	if !tuplesEqual(tuple1, tuple2) {
		t.Error("Tuples with same values should be equal")
	}

	if tuplesEqual(tuple1, tuple3) {
		t.Error("Tuples with different names should not be equal")
	}

	if tuplesEqual(tuple1, tuple4) {
		t.Error("Tuples with different ids should not be equal")
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

	hash1 := hashTuple(tuple1)
	hash2 := hashTuple(tuple2)

	if hash1 != hash2 {
		t.Error("Same tuples should have same hash")
	}

	// Different tuple should (probably) have different hash
	tuple3 := createSetOpTestTuple(desc, 2, "Bob")
	hash3 := hashTuple(tuple3)

	// Note: This could theoretically fail if there's a collision, but unlikely
	if hash1 == hash3 {
		t.Log("Warning: Different tuples have same hash (collision)")
	}
}
