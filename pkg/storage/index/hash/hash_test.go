package hash

import (
	"path/filepath"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// Helper function to create a test hash file
func createTestHashFile(t *testing.T, keyType types.Type, numBuckets int) (*HashFile, string) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_hash_index.dat")

	file, err := NewHashFile(filename, keyType, numBuckets)
	if err != nil {
		t.Fatalf("Failed to create hash file: %v", err)
	}

	return file, filename
}

func TestHashIndex_InsertAndSearch(t *testing.T) {
	file, _ := createTestHashFile(t, types.IntType, 16)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.IntType, file)
	tid := primitives.NewTransactionID()

	// Create test data
	key1 := types.NewIntField(100)
	key2 := types.NewIntField(200)
	key3 := types.NewIntField(100) // Duplicate key

	rid1 := &tuple.TupleRecordID{
		PageID:   NewHashPageID(1, 0),
		TupleNum: 0,
	}
	rid2 := &tuple.TupleRecordID{
		PageID:   NewHashPageID(1, 0),
		TupleNum: 1,
	}
	rid3 := &tuple.TupleRecordID{
		PageID:   NewHashPageID(1, 0),
		TupleNum: 2,
	}

	// Test Insert
	err := hashIndex.Insert(tid, key1, rid1)
	if err != nil {
		t.Fatalf("Failed to insert key1: %v", err)
	}

	err = hashIndex.Insert(tid, key2, rid2)
	if err != nil {
		t.Fatalf("Failed to insert key2: %v", err)
	}

	err = hashIndex.Insert(tid, key3, rid3)
	if err != nil {
		t.Fatalf("Failed to insert key3: %v", err)
	}

	// Test Search - single result
	results, err := hashIndex.Search(tid, key2)
	if err != nil {
		t.Fatalf("Failed to search key2: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result for key2, got %d", len(results))
	}
	if results[0].TupleNum != 1 {
		t.Errorf("Expected tuple number 1, got %d", results[0].TupleNum)
	}

	// Test Search - multiple results (duplicate key)
	results, err = hashIndex.Search(tid, key1)
	if err != nil {
		t.Fatalf("Failed to search key1: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results for key1, got %d", len(results))
	}
}

func TestHashIndex_Delete(t *testing.T) {
	file, _ := createTestHashFile(t, types.IntType, 16)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.IntType, file)
	tid := primitives.NewTransactionID()

	// Insert test data
	key := types.NewIntField(42)
	rid := &tuple.TupleRecordID{
		PageID:   NewHashPageID(1, 0),
		TupleNum: 0,
	}

	err := hashIndex.Insert(tid, key, rid)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Verify it exists
	results, err := hashIndex.Search(tid, key)
	if err != nil {
		t.Fatalf("Failed to search: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	// Delete
	err = hashIndex.Delete(tid, key, rid)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Verify it's gone
	results, err = hashIndex.Search(tid, key)
	if err != nil {
		t.Fatalf("Failed to search after delete: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("Expected 0 results after delete, got %d", len(results))
	}
}

func TestHashIndex_StringKeys(t *testing.T) {
	file, _ := createTestHashFile(t, types.StringType, 16)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.StringType, file)
	tid := primitives.NewTransactionID()

	// Insert string keys
	key1 := types.NewStringField("Alice", 100)
	key2 := types.NewStringField("Bob", 100)
	key3 := types.NewStringField("Charlie", 100)

	rid1 := &tuple.TupleRecordID{
		PageID:   NewHashPageID(1, 0),
		TupleNum: 0,
	}
	rid2 := &tuple.TupleRecordID{
		PageID:   NewHashPageID(1, 0),
		TupleNum: 1,
	}
	rid3 := &tuple.TupleRecordID{
		PageID:   NewHashPageID(1, 0),
		TupleNum: 2,
	}

	err := hashIndex.Insert(tid, key1, rid1)
	if err != nil {
		t.Fatalf("Failed to insert Alice: %v", err)
	}

	err = hashIndex.Insert(tid, key2, rid2)
	if err != nil {
		t.Fatalf("Failed to insert Bob: %v", err)
	}

	err = hashIndex.Insert(tid, key3, rid3)
	if err != nil {
		t.Fatalf("Failed to insert Charlie: %v", err)
	}

	// Search for Bob
	results, err := hashIndex.Search(tid, key2)
	if err != nil {
		t.Fatalf("Failed to search Bob: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result for Bob, got %d", len(results))
	}
}

func TestHashIndex_OverflowPages(t *testing.T) {
	// Create a small hash file with few buckets to force overflow
	file, _ := createTestHashFile(t, types.IntType, 2)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.IntType, file)
	tid := primitives.NewTransactionID()

	// Insert many keys to force overflow pages
	numKeys := 200
	for i := 0; i < numKeys; i++ {
		key := types.NewIntField(int64(i))
		rid := &tuple.TupleRecordID{
			PageID:   NewHashPageID(1, 0),
			TupleNum: i,
		}
		err := hashIndex.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Verify all keys can be found
	for i := 0; i < numKeys; i++ {
		key := types.NewIntField(int64(i))
		results, err := hashIndex.Search(tid, key)
		if err != nil {
			t.Fatalf("Failed to search key %d: %v", i, err)
		}
		if len(results) != 1 {
			t.Errorf("Expected 1 result for key %d, got %d", i, len(results))
		}
		if results[0].TupleNum != i {
			t.Errorf("Expected tuple number %d, got %d", i, results[0].TupleNum)
		}
	}
}

func TestHashIndex_RangeSearch(t *testing.T) {
	file, _ := createTestHashFile(t, types.IntType, 16)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.IntType, file)
	tid := primitives.NewTransactionID()

	// Insert test data
	for i := 0; i < 20; i++ {
		key := types.NewIntField(int64(i * 10))
		rid := &tuple.TupleRecordID{
			PageID:   NewHashPageID(1, 0),
			TupleNum: i,
		}
		err := hashIndex.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i*10, err)
		}
	}

	// Test range search [50, 100]
	startKey := types.NewIntField(50)
	endKey := types.NewIntField(100)
	results, err := hashIndex.RangeSearch(tid, startKey, endKey)
	if err != nil {
		t.Fatalf("Failed to range search: %v", err)
	}

	// Should find keys: 50, 60, 70, 80, 90, 100 = 6 results
	if len(results) != 6 {
		t.Errorf("Expected 6 results for range [50, 100], got %d", len(results))
	}
}

func TestHashIndex_TypeMismatch(t *testing.T) {
	file, _ := createTestHashFile(t, types.IntType, 16)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.IntType, file)
	tid := primitives.NewTransactionID()

	// Try to insert wrong type
	wrongKey := types.NewStringField("wrong", 100)
	rid := &tuple.TupleRecordID{
		PageID:   NewHashPageID(1, 0),
		TupleNum: 0,
	}

	err := hashIndex.Insert(tid, wrongKey, rid)
	if err == nil {
		t.Fatal("Expected error when inserting wrong key type, got nil")
	}
}

func TestHashPage_Serialization(t *testing.T) {
	pageID := NewHashPageID(1, 0)
	page := NewHashPage(pageID, 0, types.IntType)

	// Add some entries
	entry1 := &index.IndexEntry{
		Key: types.NewIntField(100),
		RID: &tuple.TupleRecordID{
			PageID:   NewHashPageID(1, 0),
			TupleNum: 0,
		},
	}
	entry2 := &index.IndexEntry{
		Key: types.NewIntField(200),
		RID: &tuple.TupleRecordID{
			PageID:   NewHashPageID(1, 0),
			TupleNum: 1,
		},
	}

	page.AddEntry(entry1)
	page.AddEntry(entry2)

	// Serialize
	data := page.GetPageData()

	// Deserialize
	deserializedPage, err := DeserializeHashPage(data, pageID)
	if err != nil {
		t.Fatalf("Failed to deserialize page: %v", err)
	}

	// Verify
	if deserializedPage.GetNumEntries() != 2 {
		t.Errorf("Expected 2 entries, got %d", deserializedPage.GetNumEntries())
	}

	if deserializedPage.GetBucketNum() != 0 {
		t.Errorf("Expected bucket number 0, got %d", deserializedPage.GetBucketNum())
	}

	entries := deserializedPage.GetEntries()
	if !entries[0].Key.Equals(types.NewIntField(100)) {
		t.Errorf("First entry key mismatch")
	}
	if !entries[1].Key.Equals(types.NewIntField(200)) {
		t.Errorf("Second entry key mismatch")
	}
}

func TestHashFile_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_persistence.dat")

	// Create and write data
	{
		file, err := NewHashFile(filename, types.IntType, 16)
		if err != nil {
			t.Fatalf("Failed to create hash file: %v", err)
		}

		hashIndex := NewHashIndex(1, types.IntType, file)
		tid := primitives.NewTransactionID()

		// Insert data
		for i := 0; i < 10; i++ {
			key := types.NewIntField(int64(i))
			rid := &tuple.TupleRecordID{
				PageID:   NewHashPageID(1, 0),
				TupleNum: i,
			}
			err := hashIndex.Insert(tid, key, rid)
			if err != nil {
				t.Fatalf("Failed to insert key %d: %v", i, err)
			}
		}

		file.Close()
	}

	// Reopen and verify
	{
		file, err := NewHashFile(filename, types.IntType, 16)
		if err != nil {
			t.Fatalf("Failed to reopen hash file: %v", err)
		}
		defer file.Close()

		hashIndex := NewHashIndex(1, types.IntType, file)
		tid := primitives.NewTransactionID()

		// Verify all keys exist
		for i := 0; i < 10; i++ {
			key := types.NewIntField(int64(i))
			results, err := hashIndex.Search(tid, key)
			if err != nil {
				t.Fatalf("Failed to search key %d after reopen: %v", i, err)
			}
			if len(results) != 1 {
				t.Errorf("Expected 1 result for key %d after reopen, got %d", i, len(results))
			}
		}
	}
}

func TestHashPageID(t *testing.T) {
	pid1 := NewHashPageID(1, 5)
	pid2 := NewHashPageID(1, 5)
	pid3 := NewHashPageID(1, 6)
	pid4 := NewHashPageID(2, 5)

	// Test Equals
	if !pid1.Equals(pid2) {
		t.Error("Expected pid1 to equal pid2")
	}
	if pid1.Equals(pid3) {
		t.Error("Expected pid1 to not equal pid3")
	}
	if pid1.Equals(pid4) {
		t.Error("Expected pid1 to not equal pid4")
	}

	// Test GetTableID
	if pid1.GetTableID() != 1 {
		t.Errorf("Expected table ID 1, got %d", pid1.GetTableID())
	}

	// Test PageNo
	if pid1.PageNo() != 5 {
		t.Errorf("Expected page number 5, got %d", pid1.PageNo())
	}

	// Test Serialize
	serialized := pid1.Serialize()
	if len(serialized) != 2 {
		t.Errorf("Expected serialized length 2, got %d", len(serialized))
	}
	if serialized[0] != 1 || serialized[1] != 5 {
		t.Errorf("Expected [1, 5], got %v", serialized)
	}
}

func TestHashIndex_EmptySearch(t *testing.T) {
	file, _ := createTestHashFile(t, types.IntType, 16)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.IntType, file)
	tid := primitives.NewTransactionID()

	// Search for non-existent key
	key := types.NewIntField(999)
	results, err := hashIndex.Search(tid, key)
	if err != nil {
		t.Fatalf("Failed to search: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results for non-existent key, got %d", len(results))
	}
}

func TestHashIndex_Iterator(t *testing.T) {
	file, _ := createTestHashFile(t, types.IntType, 16)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.IntType, file)
	tid := primitives.NewTransactionID()

	// Test 1: Iterator on empty index
	it := file.Iterator(tid)
	err := it.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator on empty index: %v", err)
	}

	hasNext, err := it.HasNext()
	if err != nil {
		t.Fatalf("HasNext failed on empty index: %v", err)
	}
	if hasNext {
		t.Error("Expected HasNext to be false for empty index")
	}
	it.Close()

	// Test 2: Insert some data and iterate
	testData := []struct {
		key int64
		rid int
	}{
		{100, 0},
		{200, 1},
		{300, 2},
		{150, 3},
		{250, 4},
	}

	for _, td := range testData {
		key := types.NewIntField(td.key)
		rid := &tuple.TupleRecordID{
			PageID:   NewHashPageID(1, 0),
			TupleNum: td.rid,
		}
		err := hashIndex.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", td.key, err)
		}
	}

	// Test 3: Iterate over all entries
	it = file.Iterator(tid)
	err = it.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer it.Close()

	entriesFound := 0
	seenRIDs := make(map[int]bool)

	for {
		hasNext, err := it.HasNext()
		if err != nil {
			t.Fatalf("HasNext failed: %v", err)
		}
		if !hasNext {
			break
		}

		tup, err := it.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}

		if tup == nil {
			t.Fatal("Next returned nil tuple")
		}

		if tup.RecordID == nil {
			t.Fatal("Tuple has nil RecordID")
		}

		seenRIDs[tup.RecordID.TupleNum] = true
		entriesFound++
	}

	if entriesFound != len(testData) {
		t.Errorf("Expected to iterate over %d entries, got %d", len(testData), entriesFound)
	}

	// Verify all RIDs were seen
	for _, td := range testData {
		if !seenRIDs[td.rid] {
			t.Errorf("Did not find RID %d during iteration", td.rid)
		}
	}
}

// TestHashIndex_IteratorWithOverflow tests iterator with overflow pages
func TestHashIndex_IteratorWithOverflow(t *testing.T) {
	// Use small number of buckets to force overflow
	file, _ := createTestHashFile(t, types.IntType, 2)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.IntType, file)
	tid := primitives.NewTransactionID()

	// Insert many entries to create overflow pages
	numEntries := 50
	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i * 10))
		rid := &tuple.TupleRecordID{
			PageID:   NewHashPageID(1, 0),
			TupleNum: i,
		}
		err := hashIndex.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i*10, err)
		}
	}

	// Iterate and count entries
	it := file.Iterator(tid)
	err := it.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer it.Close()

	entriesFound := 0
	seenTupleNums := make(map[int]bool)

	for {
		hasNext, err := it.HasNext()
		if err != nil {
			t.Fatalf("HasNext failed: %v", err)
		}
		if !hasNext {
			break
		}

		tup, err := it.Next()
		if err != nil {
			t.Fatalf("Next failed at entry %d: %v", entriesFound, err)
		}

		if tup != nil && tup.RecordID != nil {
			seenTupleNums[tup.RecordID.TupleNum] = true
			entriesFound++
		}
	}

	if entriesFound != numEntries {
		t.Errorf("Expected to iterate over %d entries, got %d", numEntries, entriesFound)
	}

	// Verify all tuple numbers were seen
	for i := 0; i < numEntries; i++ {
		if !seenTupleNums[i] {
			t.Errorf("Did not find tuple number %d during iteration", i)
		}
	}
}

// TestHashIndex_IteratorRewind tests the Rewind functionality
func TestHashIndex_IteratorRewind(t *testing.T) {
	file, _ := createTestHashFile(t, types.IntType, 8)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.IntType, file)
	tid := primitives.NewTransactionID()

	// Insert some data
	for i := 0; i < 10; i++ {
		key := types.NewIntField(int64(i))
		rid := &tuple.TupleRecordID{
			PageID:   NewHashPageID(1, 0),
			TupleNum: i,
		}
		hashIndex.Insert(tid, key, rid)
	}

	// First iteration
	it := file.Iterator(tid)
	err := it.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer it.Close()

	firstPassCount := 0
	for {
		hasNext, _ := it.HasNext()
		if !hasNext {
			break
		}
		it.Next()
		firstPassCount++
	}

	// Rewind and iterate again
	err = it.Rewind()
	if err != nil {
		t.Fatalf("Failed to rewind iterator: %v", err)
	}

	secondPassCount := 0
	for {
		hasNext, _ := it.HasNext()
		if !hasNext {
			break
		}
		it.Next()
		secondPassCount++
	}

	if firstPassCount != secondPassCount {
		t.Errorf("Expected same count after rewind. First: %d, Second: %d", firstPassCount, secondPassCount)
	}

	if firstPassCount != 10 {
		t.Errorf("Expected 10 entries, got %d", firstPassCount)
	}
}

// TestHashIndex_IteratorEmptyBuckets tests iterator with many empty buckets
func TestHashIndex_IteratorEmptyBuckets(t *testing.T) {
	// Create index with many buckets
	file, _ := createTestHashFile(t, types.IntType, 64)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.IntType, file)
	tid := primitives.NewTransactionID()

	// Insert only a few entries (most buckets will be empty)
	numEntries := 5
	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i * 1000))
		rid := &tuple.TupleRecordID{
			PageID:   NewHashPageID(1, 0),
			TupleNum: i,
		}
		err := hashIndex.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i*1000, err)
		}
	}

	// Iterator should skip empty buckets
	it := file.Iterator(tid)
	err := it.Open()
	if err != nil {
		t.Fatalf("Failed to open iterator: %v", err)
	}
	defer it.Close()

	entriesFound := 0
	for {
		hasNext, err := it.HasNext()
		if err != nil {
			t.Fatalf("HasNext failed: %v", err)
		}
		if !hasNext {
			break
		}

		tup, err := it.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}

		if tup != nil {
			entriesFound++
		}
	}

	if entriesFound != numEntries {
		t.Errorf("Expected to find %d entries, got %d", numEntries, entriesFound)
	}
}

// Benchmark tests
func BenchmarkHashIndex_Insert(b *testing.B) {
	tmpDir := b.TempDir()
	filename := filepath.Join(tmpDir, "bench_hash_index.dat")

	file, err := NewHashFile(filename, types.IntType, 256)
	if err != nil {
		b.Fatalf("Failed to create hash file: %v", err)
	}
	defer file.Close()

	hashIndex := NewHashIndex(1, types.IntType, file)
	tid := primitives.NewTransactionID()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := types.NewIntField(int64(i))
		rid := &tuple.TupleRecordID{
			PageID:   NewHashPageID(1, 0),
			TupleNum: i,
		}
		hashIndex.Insert(tid, key, rid)
	}
}

func BenchmarkHashIndex_Search(b *testing.B) {
	tmpDir := b.TempDir()
	filename := filepath.Join(tmpDir, "bench_hash_search.dat")

	file, err := NewHashFile(filename, types.IntType, 256)
	if err != nil {
		b.Fatalf("Failed to create hash file: %v", err)
	}
	defer file.Close()

	hashIndex := NewHashIndex(1, types.IntType, file)
	tid := primitives.NewTransactionID()

	// Insert test data
	for i := 0; i < 10000; i++ {
		key := types.NewIntField(int64(i))
		rid := &tuple.TupleRecordID{
			PageID:   NewHashPageID(1, 0),
			TupleNum: i,
		}
		hashIndex.Insert(tid, key, rid)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := types.NewIntField(int64(i % 10000))
		hashIndex.Search(tid, key)
	}
}

// Additional comprehensive tests

// TestHashIndex_CollisionHandling tests that multiple keys hashing to the same bucket work correctly
func TestHashIndex_CollisionHandling(t *testing.T) {
	// Use a small number of buckets to force collisions
	file, _ := createTestHashFile(t, types.IntType, 4)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.IntType, file)
	tid := primitives.NewTransactionID()

	// Insert many keys that will collide
	numKeys := 50
	keys := make([]types.Field, numKeys)
	rids := make([]RecID, numKeys)

	for i := 0; i < numKeys; i++ {
		keys[i] = types.NewIntField(int64(i * 100))
		rids[i] = &tuple.TupleRecordID{
			PageID:   NewHashPageID(1, 0),
			TupleNum: i,
		}
		err := hashIndex.Insert(tid, keys[i], rids[i])
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Verify all keys can be found despite collisions
	for i := 0; i < numKeys; i++ {
		results, err := hashIndex.Search(tid, keys[i])
		if err != nil {
			t.Fatalf("Failed to search key %d: %v", i, err)
		}
		if len(results) != 1 {
			t.Errorf("Expected 1 result for key %d, got %d", i, len(results))
		}
		if results[0].TupleNum != i {
			t.Errorf("Expected tuple number %d, got %d", i, results[0].TupleNum)
		}
	}
}

// TestHashIndex_DuplicateKeys tests handling of multiple values for the same key
func TestHashIndex_DuplicateKeys(t *testing.T) {
	file, _ := createTestHashFile(t, types.IntType, 16)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.IntType, file)
	tid := primitives.NewTransactionID()

	key := types.NewIntField(42)

	// Insert 10 different RIDs with the same key
	numDuplicates := 10
	for i := 0; i < numDuplicates; i++ {
		rid := &tuple.TupleRecordID{
			PageID:   NewHashPageID(1, i/5),
			TupleNum: i % 5,
		}
		err := hashIndex.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Failed to insert duplicate %d: %v", i, err)
		}
	}

	// Search should return all duplicates
	results, err := hashIndex.Search(tid, key)
	if err != nil {
		t.Fatalf("Failed to search: %v", err)
	}
	if len(results) != numDuplicates {
		t.Errorf("Expected %d results, got %d", numDuplicates, len(results))
	}

	// Delete one duplicate
	ridToDelete := &tuple.TupleRecordID{
		PageID:   NewHashPageID(1, 0),
		TupleNum: 0,
	}
	err = hashIndex.Delete(tid, key, ridToDelete)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Should have one less result
	results, err = hashIndex.Search(tid, key)
	if err != nil {
		t.Fatalf("Failed to search after delete: %v", err)
	}
	if len(results) != numDuplicates-1 {
		t.Errorf("Expected %d results after delete, got %d", numDuplicates-1, len(results))
	}
}

// TestHashIndex_DeleteFromOverflow tests deletion from overflow pages
func TestHashIndex_DeleteFromOverflow(t *testing.T) {
	// Small buckets to force overflow
	file, _ := createTestHashFile(t, types.IntType, 2)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.IntType, file)
	tid := primitives.NewTransactionID()

	// Insert many entries to create overflow pages
	numKeys := 100
	for i := 0; i < numKeys; i++ {
		key := types.NewIntField(int64(i))
		rid := &tuple.TupleRecordID{
			PageID:   NewHashPageID(1, 0),
			TupleNum: i,
		}
		err := hashIndex.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Delete entries from the middle (likely in overflow pages)
	for i := 50; i < 60; i++ {
		key := types.NewIntField(int64(i))
		rid := &tuple.TupleRecordID{
			PageID:   NewHashPageID(1, 0),
			TupleNum: i,
		}
		err := hashIndex.Delete(tid, key, rid)
		if err != nil {
			t.Fatalf("Failed to delete key %d: %v", i, err)
		}
	}

	// Verify deleted entries are gone
	for i := 50; i < 60; i++ {
		key := types.NewIntField(int64(i))
		results, err := hashIndex.Search(tid, key)
		if err != nil {
			t.Fatalf("Failed to search key %d: %v", i, err)
		}
		if len(results) != 0 {
			t.Errorf("Expected 0 results for deleted key %d, got %d", i, len(results))
		}
	}

	// Verify other entries still exist
	for i := 0; i < 50; i++ {
		key := types.NewIntField(int64(i))
		results, err := hashIndex.Search(tid, key)
		if err != nil {
			t.Fatalf("Failed to search key %d: %v", i, err)
		}
		if len(results) != 1 {
			t.Errorf("Expected 1 result for key %d, got %d", i, len(results))
		}
	}
}

// TestHashIndex_BoolType tests hash index with boolean keys
func TestHashIndex_BoolType(t *testing.T) {
	file, _ := createTestHashFile(t, types.BoolType, 8)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.BoolType, file)
	tid := primitives.NewTransactionID()

	// Insert true values
	for i := 0; i < 10; i++ {
		key := types.NewBoolField(true)
		rid := &tuple.TupleRecordID{
			PageID:   NewHashPageID(1, 0),
			TupleNum: i,
		}
		err := hashIndex.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Failed to insert true key: %v", err)
		}
	}

	// Insert false values
	for i := 10; i < 20; i++ {
		key := types.NewBoolField(false)
		rid := &tuple.TupleRecordID{
			PageID:   NewHashPageID(1, 0),
			TupleNum: i,
		}
		err := hashIndex.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Failed to insert false key: %v", err)
		}
	}

	// Search for true
	trueKey := types.NewBoolField(true)
	results, err := hashIndex.Search(tid, trueKey)
	if err != nil {
		t.Fatalf("Failed to search true: %v", err)
	}
	if len(results) != 10 {
		t.Errorf("Expected 10 results for true, got %d", len(results))
	}

	// Search for false
	falseKey := types.NewBoolField(false)
	results, err = hashIndex.Search(tid, falseKey)
	if err != nil {
		t.Fatalf("Failed to search false: %v", err)
	}
	if len(results) != 10 {
		t.Errorf("Expected 10 results for false, got %d", len(results))
	}
}

// TestHashIndex_FloatType tests hash index with float keys
func TestHashIndex_FloatType(t *testing.T) {
	file, _ := createTestHashFile(t, types.FloatType, 16)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.FloatType, file)
	tid := primitives.NewTransactionID()

	// Insert float values
	testValues := []float64{3.14, 2.71828, 1.414, 0.0, -1.5, 99.99}
	for i, val := range testValues {
		key := types.NewFloat64Field(val)
		rid := &tuple.TupleRecordID{
			PageID:   NewHashPageID(1, 0),
			TupleNum: i,
		}
		err := hashIndex.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Failed to insert float key %f: %v", val, err)
		}
	}

	// Search for each value
	for i, val := range testValues {
		key := types.NewFloat64Field(val)
		results, err := hashIndex.Search(tid, key)
		if err != nil {
			t.Fatalf("Failed to search float key %f: %v", val, err)
		}
		if len(results) != 1 {
			t.Errorf("Expected 1 result for %f, got %d", val, len(results))
		}
		if results[0].TupleNum != i {
			t.Errorf("Expected tuple number %d for %f, got %d", i, val, results[0].TupleNum)
		}
	}
}

// TestHashIndex_LargeStringKeys tests with larger string keys
func TestHashIndex_LargeStringKeys(t *testing.T) {
	file, _ := createTestHashFile(t, types.StringType, 32)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.StringType, file)
	tid := primitives.NewTransactionID()

	// Create long string keys
	longStrings := []string{
		"The quick brown fox jumps over the lazy dog",
		"Pack my box with five dozen liquor jugs",
		"How vexingly quick daft zebras jump",
		"Sphinx of black quartz, judge my vow",
		"The five boxing wizards jump quickly",
	}

	for i, str := range longStrings {
		key := types.NewStringField(str, 200)
		rid := &tuple.TupleRecordID{
			PageID:   NewHashPageID(1, 0),
			TupleNum: i,
		}
		err := hashIndex.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Failed to insert long string: %v", err)
		}
	}

	// Verify all can be found
	for i, str := range longStrings {
		key := types.NewStringField(str, 200)
		results, err := hashIndex.Search(tid, key)
		if err != nil {
			t.Fatalf("Failed to search long string: %v", err)
		}
		if len(results) != 1 {
			t.Errorf("Expected 1 result for long string %d, got %d", i, len(results))
		}
	}
}

// TestHashIndex_DeleteNonExistent tests deleting entries that don't exist
func TestHashIndex_DeleteNonExistent(t *testing.T) {
	file, _ := createTestHashFile(t, types.IntType, 16)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.IntType, file)
	tid := primitives.NewTransactionID()

	// Try to delete from empty index
	key := types.NewIntField(999)
	rid := &tuple.TupleRecordID{
		PageID:   NewHashPageID(1, 0),
		TupleNum: 0,
	}

	err := hashIndex.Delete(tid, key, rid)
	if err == nil {
		t.Error("Expected error when deleting non-existent entry, got nil")
	}

	// Insert some data
	key2 := types.NewIntField(100)
	rid2 := &tuple.TupleRecordID{
		PageID:   NewHashPageID(1, 0),
		TupleNum: 1,
	}
	hashIndex.Insert(tid, key2, rid2)

	// Try to delete different RID with same key
	wrongRID := &tuple.TupleRecordID{
		PageID:   NewHashPageID(1, 0),
		TupleNum: 99,
	}
	err = hashIndex.Delete(tid, key2, wrongRID)
	if err == nil {
		t.Error("Expected error when deleting with wrong RID, got nil")
	}

	// Original entry should still exist
	results, _ := hashIndex.Search(tid, key2)
	if len(results) != 1 {
		t.Errorf("Expected original entry to still exist")
	}
}

// TestHashIndex_SequentialInsertDelete tests alternating insert and delete operations
func TestHashIndex_SequentialInsertDelete(t *testing.T) {
	file, _ := createTestHashFile(t, types.IntType, 16)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.IntType, file)
	tid := primitives.NewTransactionID()

	// Insert and delete in sequence
	for i := 0; i < 50; i++ {
		key := types.NewIntField(int64(i))
		rid := &tuple.TupleRecordID{
			PageID:   NewHashPageID(1, 0),
			TupleNum: i,
		}

		// Insert
		err := hashIndex.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}

		// Verify exists
		results, _ := hashIndex.Search(tid, key)
		if len(results) != 1 {
			t.Errorf("Expected 1 result after insert %d, got %d", i, len(results))
		}

		// Delete immediately
		err = hashIndex.Delete(tid, key, rid)
		if err != nil {
			t.Fatalf("Failed to delete key %d: %v", i, err)
		}

		// Verify gone
		results, _ = hashIndex.Search(tid, key)
		if len(results) != 0 {
			t.Errorf("Expected 0 results after delete %d, got %d", i, len(results))
		}
	}
}

// TestHashIndex_HashDistribution tests that hash function distributes keys reasonably
func TestHashIndex_HashDistribution(t *testing.T) {
	file, _ := createTestHashFile(t, types.IntType, 32)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.IntType, file)

	// Track which buckets get keys
	bucketCounts := make(map[int]int)

	// Insert many sequential keys
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := types.NewIntField(int64(i))
		bucketNum := hashIndex.hashKey(key)
		bucketCounts[bucketNum]++
	}

	// All buckets should have some keys (with high probability)
	numBuckets := 32
	emptyBuckets := 0
	for i := 0; i < numBuckets; i++ {
		if bucketCounts[i] == 0 {
			emptyBuckets++
		}
	}

	// With 1000 keys and 32 buckets, expect most buckets to have entries
	// Allow up to 10% empty buckets
	if emptyBuckets > numBuckets/10 {
		t.Errorf("Too many empty buckets: %d out of %d", emptyBuckets, numBuckets)
	}

	// Check that distribution is somewhat balanced
	avgPerBucket := numKeys / numBuckets
	for bucket, count := range bucketCounts {
		// Each bucket should have roughly avgPerBucket +/- 50%
		// This is a loose check since hash distribution varies
		if count < avgPerBucket/2 || count > avgPerBucket*2 {
			t.Logf("Warning: Bucket %d has %d keys (avg: %d)", bucket, count, avgPerBucket)
		}
	}
}

// TestHashIndex_NegativeKeys tests with negative integer keys
func TestHashIndex_NegativeKeys(t *testing.T) {
	file, _ := createTestHashFile(t, types.IntType, 16)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.IntType, file)
	tid := primitives.NewTransactionID()

	// Insert negative, zero, and positive keys
	testKeys := []int64{-100, -50, -1, 0, 1, 50, 100}
	for i, val := range testKeys {
		key := types.NewIntField(val)
		rid := &tuple.TupleRecordID{
			PageID:   NewHashPageID(1, 0),
			TupleNum: i,
		}
		err := hashIndex.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", val, err)
		}
	}

	// Verify all keys can be found
	for i, val := range testKeys {
		key := types.NewIntField(val)
		results, err := hashIndex.Search(tid, key)
		if err != nil {
			t.Fatalf("Failed to search key %d: %v", val, err)
		}
		if len(results) != 1 {
			t.Errorf("Expected 1 result for key %d, got %d", val, len(results))
		}
		if results[0].TupleNum != i {
			t.Errorf("Expected tuple number %d for key %d, got %d", i, val, results[0].TupleNum)
		}
	}
}

// TestHashIndex_RangeSearchEdgeCases tests range search edge cases
func TestHashIndex_RangeSearchEdgeCases(t *testing.T) {
	file, _ := createTestHashFile(t, types.IntType, 16)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.IntType, file)
	tid := primitives.NewTransactionID()

	// Insert keys: 10, 20, 30, 40, 50
	for i := 1; i <= 5; i++ {
		key := types.NewIntField(int64(i * 10))
		rid := &tuple.TupleRecordID{
			PageID:   NewHashPageID(1, 0),
			TupleNum: i - 1,
		}
		hashIndex.Insert(tid, key, rid)
	}

	// Test range with no matches
	results, err := hashIndex.RangeSearch(tid, types.NewIntField(60), types.NewIntField(100))
	if err != nil {
		t.Fatalf("Failed range search: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results for range with no matches, got %d", len(results))
	}

	// Test range matching exactly one key
	results, err = hashIndex.RangeSearch(tid, types.NewIntField(30), types.NewIntField(30))
	if err != nil {
		t.Fatalf("Failed range search: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 result for exact match range, got %d", len(results))
	}

	// Test range matching all keys
	results, err = hashIndex.RangeSearch(tid, types.NewIntField(0), types.NewIntField(100))
	if err != nil {
		t.Fatalf("Failed range search: %v", err)
	}
	if len(results) != 5 {
		t.Errorf("Expected 5 results for full range, got %d", len(results))
	}
}

// TestHashIndex_StressTest performs a stress test with many operations
func TestHashIndex_StressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	file, _ := createTestHashFile(t, types.IntType, 64)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.IntType, file)
	tid := primitives.NewTransactionID()

	// Insert 5000 keys
	numKeys := 5000
	for i := 0; i < numKeys; i++ {
		key := types.NewIntField(int64(i))
		rid := &tuple.TupleRecordID{
			PageID:   NewHashPageID(1, i/100),
			TupleNum: i % 100,
		}
		err := hashIndex.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Verify random subset of keys
	for i := 0; i < 100; i++ {
		keyVal := int64(i * 50)
		if keyVal >= int64(numKeys) {
			continue
		}
		key := types.NewIntField(keyVal)
		results, err := hashIndex.Search(tid, key)
		if err != nil {
			t.Fatalf("Failed to search key %d: %v", keyVal, err)
		}
		if len(results) != 1 {
			t.Errorf("Expected 1 result for key %d, got %d", keyVal, len(results))
		}
	}

	// Delete every 10th key
	for i := 0; i < numKeys; i += 10 {
		key := types.NewIntField(int64(i))
		rid := &tuple.TupleRecordID{
			PageID:   NewHashPageID(1, i/100),
			TupleNum: i % 100,
		}
		err := hashIndex.Delete(tid, key, rid)
		if err != nil {
			t.Fatalf("Failed to delete key %d: %v", i, err)
		}
	}

	// Verify deleted keys are gone
	for i := 0; i < numKeys; i += 10 {
		key := types.NewIntField(int64(i))
		results, err := hashIndex.Search(tid, key)
		if err != nil {
			t.Fatalf("Failed to search deleted key %d: %v", i, err)
		}
		if len(results) != 0 {
			t.Errorf("Expected 0 results for deleted key %d, got %d", i, len(results))
		}
	}
}

// TestHashIndex_EmptyStringKeys tests with empty string keys
func TestHashIndex_EmptyStringKeys(t *testing.T) {
	file, _ := createTestHashFile(t, types.StringType, 16)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.StringType, file)
	tid := primitives.NewTransactionID()

	// Insert empty string
	key := types.NewStringField("", 100)
	rid := &tuple.TupleRecordID{
		PageID:   NewHashPageID(1, 0),
		TupleNum: 0,
	}

	err := hashIndex.Insert(tid, key, rid)
	if err != nil {
		t.Fatalf("Failed to insert empty string: %v", err)
	}

	// Search for empty string
	results, err := hashIndex.Search(tid, key)
	if err != nil {
		t.Fatalf("Failed to search empty string: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 result for empty string, got %d", len(results))
	}
}

// TestHashIndex_MultipleTransactions tests operations with different transaction IDs
func TestHashIndex_MultipleTransactions(t *testing.T) {
	file, _ := createTestHashFile(t, types.IntType, 16)
	defer file.Close()

	hashIndex := NewHashIndex(1, types.IntType, file)

	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()

	// Insert with tid1
	key1 := types.NewIntField(100)
	rid1 := &tuple.TupleRecordID{
		PageID:   NewHashPageID(1, 0),
		TupleNum: 0,
	}
	err := hashIndex.Insert(tid1, key1, rid1)
	if err != nil {
		t.Fatalf("Failed to insert with tid1: %v", err)
	}

	// Insert with tid2
	key2 := types.NewIntField(200)
	rid2 := &tuple.TupleRecordID{
		PageID:   NewHashPageID(1, 0),
		TupleNum: 1,
	}
	err = hashIndex.Insert(tid2, key2, rid2)
	if err != nil {
		t.Fatalf("Failed to insert with tid2: %v", err)
	}

	// Search with different transactions should see the data
	results, err := hashIndex.Search(tid1, key1)
	if err != nil {
		t.Fatalf("Failed to search with tid1: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 result with tid1, got %d", len(results))
	}

	results, err = hashIndex.Search(tid2, key2)
	if err != nil {
		t.Fatalf("Failed to search with tid2: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 result with tid2, got %d", len(results))
	}
}
