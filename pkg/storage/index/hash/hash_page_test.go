package hash

import (
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

func TestNewHashPage(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 0)
	hp := NewHashPage(pageID, 5, types.IntType)

	if hp.GetBucketNum() != 5 {
		t.Errorf("Expected bucket 5, got %d", hp.GetBucketNum())
	}
	if hp.GetNumEntries() != 0 {
		t.Errorf("Expected 0 entries, got %d", hp.GetNumEntries())
	}
	if !hp.HasNoOverflowPage() {
		t.Error("New page should have no overflow page")
	}
	if hp.GetOverflowPageNum() != NoOverFlowPage {
		t.Errorf("Expected overflow page got %v", hp.GetOverflowPageNum())
	}
	if hp.IsFull() {
		t.Error("Empty page should not be full")
	}
	if hp.GetPageNo() != 0 {
		t.Errorf("Expected page number 0, got %d", hp.GetPageNo())
	}
}

func TestHashPageAddEntry(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 0)
	hp := NewHashPage(pageID, 0, types.IntType)

	// Create test entries
	entry1 := &index.IndexEntry{
		Key: types.NewIntField(100),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 5),
			TupleNum: 10,
		},
	}
	entry2 := &index.IndexEntry{
		Key: types.NewIntField(200),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 6),
			TupleNum: 15,
		},
	}

	// Add first entry
	if err := hp.AddEntry(entry1); err != nil {
		t.Fatalf("Failed to add entry1: %v", err)
	}
	if hp.GetNumEntries() != 1 {
		t.Errorf("Expected 1 entry, got %d", hp.GetNumEntries())
	}

	// Add second entry
	if err := hp.AddEntry(entry2); err != nil {
		t.Fatalf("Failed to add entry2: %v", err)
	}
	if hp.GetNumEntries() != 2 {
		t.Errorf("Expected 2 entries, got %d", hp.GetNumEntries())
	}

	// Verify entries
	entries := hp.GetEntries()
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries from GetEntries, got %d", len(entries))
	}
	if eq, _ := entries[0].Key.Compare(primitives.Equals, entry1.Key); !eq {
		t.Error("First entry key mismatch")
	}
	if eq, _ := entries[1].Key.Compare(primitives.Equals, entry2.Key); !eq {
		t.Error("Second entry key mismatch")
	}
}

func TestHashPageRemoveEntry(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 0)
	hp := NewHashPage(pageID, 0, types.IntType)

	entry1 := &index.IndexEntry{
		Key: types.NewIntField(100),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 5),
			TupleNum: 10,
		},
	}
	entry2 := &index.IndexEntry{
		Key: types.NewIntField(200),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 6),
			TupleNum: 15,
		},
	}
	entry3 := &index.IndexEntry{
		Key: types.NewIntField(300),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 7),
			TupleNum: 20,
		},
	}

	hp.AddEntry(entry1)
	hp.AddEntry(entry2)
	hp.AddEntry(entry3)

	// Remove middle entry
	if err := hp.RemoveEntry(entry2); err != nil {
		t.Fatalf("Failed to remove entry2: %v", err)
	}
	if hp.GetNumEntries() != 2 {
		t.Errorf("Expected 2 entries after removal, got %d", hp.GetNumEntries())
	}

	// Verify remaining entries
	entries := hp.GetEntries()
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(entries))
	}
	if eq, _ := entries[0].Key.Compare(primitives.Equals, entry1.Key); !eq {
		t.Error("First entry should be entry1")
	}
	if eq, _ := entries[1].Key.Compare(primitives.Equals, entry3.Key); !eq {
		t.Error("Second entry should be entry3")
	}

	// Try to remove non-existent entry
	nonExistent := &index.IndexEntry{
		Key: types.NewIntField(999),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 99),
			TupleNum: 99,
		},
	}
	if err := hp.RemoveEntry(nonExistent); err == nil {
		t.Error("Should error when removing non-existent entry")
	}
}

func TestHashPageFindEntries(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 0)
	hp := NewHashPage(pageID, 0, types.IntType)

	key := types.NewIntField(100)

	// Add multiple entries with same key (different RIDs)
	entry1 := &index.IndexEntry{
		Key: key,
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 5),
			TupleNum: 10,
		},
	}
	entry2 := &index.IndexEntry{
		Key: key,
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 6),
			TupleNum: 15,
		},
	}
	entry3 := &index.IndexEntry{
		Key: types.NewIntField(200), // Different key
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 7),
			TupleNum: 20,
		},
	}

	hp.AddEntry(entry1)
	hp.AddEntry(entry2)
	hp.AddEntry(entry3)

	// Find entries with key 100
	results := hp.FindEntries(key)
	if len(results) != 2 {
		t.Errorf("Expected 2 results for key 100, got %d", len(results))
	}
	if results[0].TupleNum != 10 {
		t.Errorf("Expected first result tuple num 10, got %d", results[0].TupleNum)
	}
	if results[1].TupleNum != 15 {
		t.Errorf("Expected second result tuple num 15, got %d", results[1].TupleNum)
	}

	// Find entries with key 200
	results = hp.FindEntries(types.NewIntField(200))
	if len(results) != 1 {
		t.Errorf("Expected 1 result for key 200, got %d", len(results))
	}

	// Find entries with non-existent key
	results = hp.FindEntries(types.NewIntField(999))
	if len(results) != 0 {
		t.Errorf("Expected 0 results for non-existent key, got %d", len(results))
	}
}

func TestHashPageOverflow(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 0)
	hp := NewHashPage(pageID, 0, types.IntType)

	// Test setting overflow page
	hp.SetOverflowPage(42)
	if hp.HasNoOverflowPage() {
		t.Error("Page should have overflow hp after setting")
	}
	if hp.GetOverflowPageNum() != 42 {
		t.Errorf("Expected overflow hp 42, got %d", hp.GetOverflowPageNum())
	}

	// Reset to no overflow
	hp.SetOverflowPage(NoOverFlowPage)
	if !hp.HasNoOverflowPage() {
		t.Error("Page should have no overflow page after reset")
	}
}

func TestHashPageFullCapacity(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 0)
	hp := NewHashPage(pageID, 0, types.IntType)

	// Add entries up to capacity
	for i := 0; i < maxHashEntriesPerPage; i++ {
		entry := &index.IndexEntry{
			Key: types.NewIntField(int64(i)),
			RID: &tuple.TupleRecordID{
				PageID:   page.NewPageDescriptor(1, 0),
				TupleNum: primitives.SlotID(i),
			},
		}
		if err := hp.AddEntry(entry); err != nil {
			t.Fatalf("Failed to add entry %d: %v", i, err)
		}
	}

	if !hp.IsFull() {
		t.Error("Page should be full after adding max entries")
	}
	if hp.GetNumEntries() != maxHashEntriesPerPage {
		t.Errorf("Expected %d entries, got %d", maxHashEntriesPerPage, hp.GetNumEntries())
	}

	// Try to add one more entry
	extraEntry := &index.IndexEntry{
		Key: types.NewIntField(9999),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 0),
			TupleNum: 9999,
		},
	}
	if err := hp.AddEntry(extraEntry); err == nil {
		t.Error("Should error when adding to full page")
	}
}

func TestHashPageSerialization(t *testing.T) {
	pageID := page.NewPageDescriptor(2, 10)
	hp := NewHashPage(pageID, 5, types.IntType)
	hp.SetOverflowPage(15)

	// Add entries
	entry1 := &index.IndexEntry{
		Key: types.NewIntField(100),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(2, 5),
			TupleNum: 10,
		},
	}
	entry2 := &index.IndexEntry{
		Key: types.NewIntField(200),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(2, 6),
			TupleNum: 15,
		},
	}

	hp.AddEntry(entry1)
	hp.AddEntry(entry2)

	// Serialize
	data := hp.GetPageData()
	if len(data) != 4096 {
		t.Errorf("Expected page data size 4096, got %d", len(data))
	}

	// Deserialize
	deserializedPage, err := DeserializeHashPage(data, pageID)
	if err != nil {
		t.Fatalf("Failed to deserialize page: %v", err)
	}

	// Verify properties
	if deserializedPage.GetBucketNum() != 5 {
		t.Errorf("Expected bucket 5, got %d", deserializedPage.GetBucketNum())
	}
	if deserializedPage.GetNumEntries() != 2 {
		t.Errorf("Expected 2 entries, got %d", deserializedPage.GetNumEntries())
	}
	if deserializedPage.GetOverflowPageNum() != 15 {
		t.Errorf("Expected overflow page 15, got %d", deserializedPage.GetOverflowPageNum())
	}
	if deserializedPage.GetPageNo() != 10 {
		t.Errorf("Expected page number 10, got %d", deserializedPage.GetPageNo())
	}

	// Verify entries
	entries := deserializedPage.GetEntries()
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(entries))
	}
	if eq, _ := entries[0].Key.Compare(primitives.Equals, entry1.Key); !eq {
		t.Error("First entry key mismatch")
	}
	if entries[0].RID.TupleNum != 10 {
		t.Error("First entry RID mismatch")
	}
	if eq, _ := entries[1].Key.Compare(primitives.Equals, entry2.Key); !eq {
		t.Error("Second entry key mismatch")
	}
	if entries[1].RID.TupleNum != 15 {
		t.Error("Second entry RID mismatch")
	}
}

func TestHashPageSerializationWithStringKeys(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 0)
	hp := NewHashPage(pageID, 3, types.StringType)

	entry1 := &index.IndexEntry{
		Key: types.NewStringField("apple", types.StringMaxSize),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 10),
			TupleNum: 5,
		},
	}
	entry2 := &index.IndexEntry{
		Key: types.NewStringField("banana", types.StringMaxSize),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 11),
			TupleNum: 7,
		},
	}

	hp.AddEntry(entry1)
	hp.AddEntry(entry2)

	// Serialize
	data := hp.GetPageData()

	// Deserialize
	deserializedPage, err := DeserializeHashPage(data, pageID)
	if err != nil {
		t.Fatalf("Failed to deserialize page with string keys: %v", err)
	}

	if deserializedPage.GetNumEntries() != 2 {
		t.Errorf("Expected 2 entries, got %d", deserializedPage.GetNumEntries())
	}

	entries := deserializedPage.GetEntries()
	if eq, _ := entries[0].Key.Compare(primitives.Equals, entry1.Key); !eq {
		t.Error("String key mismatch for first entry")
	}
	if eq, _ := entries[1].Key.Compare(primitives.Equals, entry2.Key); !eq {
		t.Error("String key mismatch for second entry")
	}
}

func TestHashPageDirtyTracking(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 0)
	hp := NewHashPage(pageID, 0, types.IntType)

	// New page has isDirty=true but dirtyTxn=nil, so IsDirty() returns nil
	// This is expected behavior - page is marked dirty but no transaction owns it yet
	if hp.IsDirty() != nil {
		t.Error("New hp IsDirty should return nil (no transaction yet)")
	}

	// Mark clean
	hp.MarkDirty(false, nil)
	if hp.IsDirty() != nil {
		t.Error("Page should be clean after marking not dirty")
	}

	// Mark dirty with transaction
	txnID := primitives.NewTransactionIDFromValue(123)
	hp.MarkDirty(true, txnID)

	if hp.IsDirty() == nil {
		t.Error("Page should be dirty after marking")
	}
	if hp.IsDirty().ID() != 123 {
		t.Errorf("Expected txn ID %d, got %d", 123, hp.IsDirty().ID())
	}

	// Verify before image was captured
	if hp.beforeImage == nil {
		t.Error("Before image should be set when marking dirty")
	}
}

func TestHashPageBeforeImage(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 0)
	hp := NewHashPage(pageID, 0, types.IntType)

	// Mark clean first
	hp.MarkDirty(false, nil)

	// Initially no before image
	if hp.GetBeforeImage() != nil {
		t.Error("Should have no before image initially")
	}

	// Mark dirty to capture before image
	txnID := primitives.NewTransactionIDFromValue(123)
	hp.MarkDirty(true, txnID)

	// Add an entry
	entry := &index.IndexEntry{
		Key: types.NewIntField(100),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 0),
			TupleNum: 0,
		},
	}
	hp.AddEntry(entry)

	// Get before image
	beforePage := hp.GetBeforeImage()
	if beforePage == nil {
		t.Fatal("Before image should be retrievable")
	}

	beforeHash := beforePage.(*HashPage)
	if beforeHash.GetNumEntries() != 0 {
		t.Error("Before image should have 0 entries")
	}
	if hp.GetNumEntries() != 1 {
		t.Error("Current page should have 1 entry")
	}
}

func TestHashPageSetBeforeImage(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 0)
	hp := NewHashPage(pageID, 0, types.IntType)

	// Mark clean
	hp.MarkDirty(false, nil)

	entry := &index.IndexEntry{
		Key: types.NewIntField(100),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 0),
			TupleNum: 0,
		},
	}
	hp.AddEntry(entry)

	// Manually set before image
	hp.SetBeforeImage()
	if hp.beforeImage == nil {
		t.Error("Before image should be set")
	}

	// Add another entry
	entry2 := &index.IndexEntry{
		Key: types.NewIntField(200),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 0),
			TupleNum: 1,
		},
	}
	hp.AddEntry(entry2)

	// Before image should have 1 entry, current has 2
	beforePage := hp.GetBeforeImage()
	beforeHash := beforePage.(*HashPage)
	if beforeHash.GetNumEntries() != 1 {
		t.Errorf("Before image should have 1 entry, got %d", beforeHash.GetNumEntries())
	}
	if hp.GetNumEntries() != 2 {
		t.Errorf("Current page should have 2 entries, got %d", hp.GetNumEntries())
	}
}

func TestHashPageGetID(t *testing.T) {
	pageID := page.NewPageDescriptor(5, 10)
	hp := NewHashPage(pageID, 0, types.IntType)

	retrievedID := hp.GetID()
	if retrievedID.GetTableID() != 5 {
		t.Errorf("Expected table ID 5, got %d", retrievedID.GetTableID())
	}
	if retrievedID.PageNo() != 10 {
		t.Errorf("Expected page number 10, got %d", retrievedID.PageNo())
	}
}

func TestHashPageEmptySerialization(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 0)
	hp := NewHashPage(pageID, 7, types.IntType)

	// Serialize empty page
	data := hp.GetPageData()

	// Deserialize
	deserializedPage, err := DeserializeHashPage(data, pageID)
	if err != nil {
		t.Fatalf("Failed to deserialize empty page: %v", err)
	}

	if deserializedPage.GetBucketNum() != 7 {
		t.Errorf("Expected bucket 7, got %d", deserializedPage.GetBucketNum())
	}
	if deserializedPage.GetNumEntries() != 0 {
		t.Errorf("Expected 0 entries, got %d", deserializedPage.GetNumEntries())
	}
	if !deserializedPage.HasNoOverflowPage() {
		t.Error("Empty page should have no overflow")
	}
}

func TestHashPageDeserializationInvalidData(t *testing.T) {
	pageID := page.NewPageDescriptor(1, 0)

	// Test with too short data
	shortData := make([]byte, 5)
	_, err := DeserializeHashPage(shortData, pageID)
	if err == nil {
		t.Error("Should error on too short data")
	}
}

func TestHashPageWithMultipleKeyTypes(t *testing.T) {
	// Test with Float keys
	pageID := page.NewPageDescriptor(1, 0)
	hp := NewHashPage(pageID, 0, types.FloatType)

	entry := &index.IndexEntry{
		Key: types.NewFloat64Field(3.14),
		RID: &tuple.TupleRecordID{
			PageID:   page.NewPageDescriptor(1, 0),
			TupleNum: 0,
		},
	}
	hp.AddEntry(entry)

	data := hp.GetPageData()
	deserializedPage, err := DeserializeHashPage(data, pageID)
	if err != nil {
		t.Fatalf("Failed to deserialize page with float keys: %v", err)
	}

	entries := deserializedPage.GetEntries()
	if len(entries) != 1 {
		t.Errorf("Expected 1 entry, got %d", len(entries))
	}
	if eq, _ := entries[0].Key.Compare(primitives.Equals, entry.Key); !eq {
		t.Error("Float key mismatch")
	}
}
