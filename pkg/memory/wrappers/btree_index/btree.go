package btreeindex

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/memory"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/index/btree"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type BTreeFile = btree.BTreeFile
type BTreePage = btree.BTreePage
type BTreePageID = btree.BTreePageID

// BTree implements the Index interface for B+Tree indexes.
// It provides an ordered index structure that supports efficient search, range queries,
// and maintains balance through automatic splitting and merging of pages.
//
// Key characteristics:
// - All data resides in leaf pages, forming a doubly-linked list for range scans
// - Internal pages contain only separator keys and child pointers for navigation
// - Pages split when full, merge when underutilized (maintaining balance)
// - Root page can be either leaf (small tree) or internal (large tree)
//
// Fields:
//   - indexID: Unique identifier for this index
//   - keyType: Type of keys stored in this index (IntType, StringType, etc.)
//   - file: Underlying file managing page allocation and I/O
//   - rootPageID: Page ID of the current root (nil if tree is empty)
//   - tx: Transaction context for concurrency control and rollback
//   - store: PageStore for buffer pool management and page locking
type BTree struct {
	indexID    int
	keyType    types.Type
	file       *BTreeFile
	rootPageID *BTreePageID
	tx         *transaction.TransactionContext
	store      *memory.PageStore
}

// NewBTree creates a new B+Tree index with the specified configuration.
// Initializes the tree structure and registers with the PageStore for proper
// buffer management and transaction support.
//
// The function:
// 1. Creates the BTree wrapper with provided parameters
// 2. Sets the file's index ID for proper page identification
// 3. Registers the file with PageStore for flush coordination
// 4. If pages exist, sets root to page 0 (B+Tree convention)
//
// Parameters:
//   - indexID: Unique identifier for this index in the database
//   - keyType: Type of keys this index will store (must be consistent)
//   - file: BTreeFile managing physical storage of pages
//   - tx: Transaction context for ACID properties
//   - store: PageStore for buffer pool and locking
//
// Returns:
//   - *BTree: Initialized B+Tree ready for operations
func NewBTree(indexID int, keyType types.Type, file *BTreeFile, tx *transaction.TransactionContext, store *memory.PageStore) *BTree {
	bt := &BTree{
		indexID: indexID,
		keyType: keyType,
		file:    file,
		tx:      tx,
		store:   store,
	}

	// Set the file's index ID to match this BTree's index ID
	file.SetIndexID(indexID)

	// Register the BTreeFile with the PageStore so it can flush pages properly
	store.RegisterDbFile(indexID, file)

	if file.NumPages() > 0 {
		bt.rootPageID = btree.NewBTreePageID(bt.indexID, 0)
	}

	return bt
}

// Insert adds a key-value pair to the B+Tree, maintaining sorted order and balance.
// This is the main entry point for all insert operations in the index.
//
// The insertion process:
// 1. Validates key type matches index configuration
// 2. Finds the appropriate leaf page by traversing from root
// 3. If leaf has space: directly inserts maintaining sort order
// 4. If leaf is full: triggers split operation that may propagate upward
//
// Key behaviors:
// - Duplicate (key, RID) pairs are rejected with error
// - Empty tree gets first entry inserted in root leaf
// - Splits may cascade up to root, potentially increasing tree height
//
// Parameters:
//   - key: The index key to insert (must match index keyType)
//   - rid: Tuple record identifier pointing to actual row data
//
// Returns:
//   - error: Returns error if key type mismatch, page access fails, or duplicate exists
func (bt *BTree) Insert(key types.Field, rid *tuple.TupleRecordID) error {
	if key.Type() != bt.keyType {
		return fmt.Errorf("key type mismatch: expected %v, got %v", bt.keyType, key.Type())
	}

	entry := index.NewIndexEntry(key, rid)
	rootPage, err := bt.getRootPage(transaction.ReadWrite)
	if err != nil {
		return fmt.Errorf("failed to get root page: %w", err)
	}

	if rootPage.GetNumEntries() == 0 && rootPage.IsLeafPage() {
		return bt.insertIntoLeaf(rootPage, entry)
	}

	leafPage, err := bt.findLeafPage(rootPage, key)
	if err != nil {
		return fmt.Errorf("failed to find leaf page: %w", err)
	}

	if leafPage.IsFull() {
		return bt.insertAndSplitLeaf(leafPage, key, rid)
	}

	return bt.insertIntoLeaf(leafPage, entry)
}

// Delete removes a key-value pair from the B+Tree, maintaining balance through merging.
// Finds the appropriate leaf page and removes the specified entry if it exists.
//
// The deletion process:
// 1. Validates key type matches index configuration
// 2. Traverses tree to find leaf page containing the key
// 3. Removes the entry from the leaf if found
// 4. May trigger merge/redistribution if page becomes full
//
// Note: The tid parameter is unused as transaction context is stored in bt.tx.
// This parameter exists for interface compatibility.
//
// Parameters:
//   - tid: Transaction ID (unused - bt.tx provides context)
//   - key: The index key to delete
//   - rid: The specific record ID to delete (allows multiple entries per key)
//
// Returns:
//   - error: Returns error if key type mismatch, page access fails, or entry not found
func (bt *BTree) Delete(key types.Field, rid *tuple.TupleRecordID) error {
	if key.Type() != bt.keyType {
		return fmt.Errorf("key type mismatch: expected %v, got %v", bt.keyType, key.Type())
	}

	rootPage, err := bt.getRootPage(transaction.ReadWrite)
	if err != nil {
		return fmt.Errorf("failed to get root page: %w", err)
	}

	entry := index.NewIndexEntry(key, rid)
	leafPage, err := bt.findLeafPage(rootPage, key)
	if err != nil {
		return fmt.Errorf("failed to find leaf page: %w", err)
	}

	return bt.deleteFromLeaf(leafPage, entry)
}

// Search finds all tuple locations (RIDs) for a given key using exact match.
// This is a point lookup operation that returns all rows with the specified key.
//
// The search process:
// 1. Validates key type matches index configuration
// 2. Traverses tree from root to appropriate leaf page
// 3. Scans leaf entries for all matches (handles duplicate keys)
// 4. Returns empty slice if key not found (not an error)
//
// B+Tree guarantees:
// - O(log N) traversal time where N is number of keys
// - All matching entries are in a single leaf page
// - Results maintain insertion order for duplicate keys
//
// Parameters:
//   - key: The key to search for (must match index keyType)
//
// Returns:
//   - []*tuple.TupleRecordID: Slice of all matching record IDs (empty if none found)
//   - error: Returns error only if key type mismatch or page access fails
func (bt *BTree) Search(key types.Field) ([]*tuple.TupleRecordID, error) {
	if key.Type() != bt.keyType {
		return nil, fmt.Errorf("key type mismatch: expected %v, got %v", bt.keyType, key.Type())
	}

	rootPage, err := bt.getRootPage(transaction.ReadOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to get root page: %w", err)
	}

	if rootPage.GetNumEntries() == 0 {
		return []*tuple.TupleRecordID{}, nil
	}

	leafPage, err := bt.findLeafPage(rootPage, key)
	if err != nil {
		return nil, fmt.Errorf("failed to find leaf page: %w", err)
	}

	var results []*tuple.TupleRecordID
	for _, entry := range leafPage.Entries {
		if entry.Key.Equals(key) {
			results = append(results, entry.RID)
		}
	}
	return results, nil
}

// RangeSearch finds all tuples where key is in the inclusive range [startKey, endKey].
// Leverages the B+Tree's sorted leaf chain to efficiently scan a range of keys.
//
// The range scan process:
// 1. Validates both keys match index type
// 2. Finds leaf page containing startKey
// 3. Scans forward through leaf chain collecting matching entries
// 4. Stops when endKey is exceeded or leaf chain ends
//
// Efficiency characteristics:
// - O(log N) to find starting leaf
// - O(M) to scan M matching entries across leaf pages
// - Uses doubly-linked leaf chain (NextLeaf pointers)
// - More efficient than repeated point lookups for ranges
//
// Note: The tid parameter is unused as transaction context is stored in bt.tx.
// This parameter exists for interface compatibility.
//
// Parameters:
//   - tid: Transaction ID (unused - bt.tx provides context)
//   - startKey: Lower bound of range (inclusive)
//   - endKey: Upper bound of range (inclusive)
//
// Returns:
//   - []*tuple.TupleRecordID: All record IDs with keys in [startKey, endKey]
//   - error: Returns error if key types mismatch or page access fails
func (bt *BTree) RangeSearch(startKey, endKey types.Field) ([]*tuple.TupleRecordID, error) {
	if startKey.Type() != bt.keyType || endKey.Type() != bt.keyType {
		return nil, fmt.Errorf("key type mismatch")
	}

	rootPage, err := bt.getRootPage(transaction.ReadOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to get root page: %w", err)
	}

	if rootPage.GetNumEntries() == 0 {
		return []*tuple.TupleRecordID{}, nil
	}

	leafPage, err := bt.findLeafPage(rootPage, startKey)
	if err != nil {
		return nil, fmt.Errorf("failed to find start leaf page: %w", err)
	}

	var results []*tuple.TupleRecordID

	for leafPage != nil {
		for _, entry := range leafPage.Entries {
			geStart, _ := entry.Key.Compare(primitives.GreaterThanOrEqual, startKey)
			leEnd, _ := entry.Key.Compare(primitives.LessThanOrEqual, endKey)

			if geStart && leEnd {
				results = append(results, entry.RID)
			} else if !leEnd {
				return results, nil
			}
		}

		if !leafPage.HasNextLeaf() {
			break
		}

		_, nextLeaf := leafPage.Leaves()
		nextPageID := btree.NewBTreePageID(bt.indexID, nextLeaf)
		leafPage, err = bt.getPage(nextPageID, transaction.ReadOnly)
		if err != nil {
			return nil, fmt.Errorf("failed to read next leaf page: %w", err)
		}
	}

	return results, nil
}

// GetIndexType returns the type identifier for this index implementation.
// Used by the query planner to determine available operations and optimization strategies.
//
// Returns:
//   - index.IndexType: Always returns index.BTreeIndex
func (bt *BTree) GetIndexType() index.IndexType {
	return index.BTreeIndex
}

// GetKeyType returns the data type of keys stored in this index.
// Used for type checking during insert/search operations and query planning.
//
// Returns:
//   - types.Type: The configured key type (IntType, StringType, etc.)
func (bt *BTree) GetKeyType() types.Type {
	return bt.keyType
}

// Close releases all resources held by the index and unregister it from PageStore.
// Should be called when the index is no longer needed to prevent resource leaks.
func (bt *BTree) Close() error {
	bt.store.UnregisterDbFile(bt.indexID)
	return bt.file.Close()
}

// getRootPage retrieves or creates the root page of the B+Tree.
// The root is always page 0 in the B+Tree file by convention.
//
// Behavior:
// - If root exists: fetches it with specified permissions
// - If tree is empty: allocates new root as leaf page (single-page tree)
// - New root is immediately marked dirty for transaction tracking
//
// Parameters:
//   - perm: Read-only or read-write access permissions
//
// Returns:
//   - *BTreePage: The root page of the tree
//   - error: Returns error if page fetch/allocation fails
func (bt *BTree) getRootPage(perm transaction.Permissions) (*BTreePage, error) {
	if bt.rootPageID != nil {
		return bt.getPage(bt.rootPageID, perm)
	}

	root, err := bt.file.AllocatePage(bt.tx.ID, bt.keyType, true, -1)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate root page: %w", err)
	}

	bt.rootPageID = root.GetBTreePageID()
	if err := bt.addDirtyPage(root, memory.InsertOperation); err != nil {
		return nil, fmt.Errorf("failed to mark root page as dirty: %w", err)
	}

	return root, nil
}

// findLeafPage navigates from root to the leaf page that should contain the given key.
// Implements recursive tree traversal following B+Tree invariants.
//
// Traversal algorithm:
// 1. If current page is leaf: return it (base case)
// 2. Find appropriate child pointer using key comparisons
// 3. Recursively descend to child page
// 4. Repeat until leaf is reached
//
// B+Tree navigation rules:
// - In internal nodes: children[i] has keys >= children[i].Key and < children[i+1].Key
// - Leftmost child (children[0]) has no key, contains keys smaller than all others
// - All actual data (RIDs) resides only in leaf pages
//
// Parameters:
//   - currentPage: Current page in traversal (starts with root)
//   - key: The key being searched for
//
// Returns:
//   - *BTreePage: The leaf page where the key belongs (may or may not contain it)
//   - error: Returns error if child page read fails or structure is invalid
func (bt *BTree) findLeafPage(currentPage *BTreePage, key types.Field) (*BTreePage, error) {
	if currentPage.IsLeafPage() {
		return currentPage, nil
	}

	childPID := bt.findChildPointer(currentPage, key)
	if childPID == nil {
		return nil, fmt.Errorf("failed to find child pointer for key")
	}

	childPage, err := bt.getPage(childPID, transaction.ReadOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to read child page: %w", err)
	}

	return bt.findLeafPage(childPage, key)
}

// findChildPointer finds the appropriate child pointer for a given key in an internal node.
// Implements the B+Tree separator key logic to determine which subtree contains the key.
//
// Internal node structure: [P0, K1, P1, K2, P2, ..., Kn, Pn]
// Where:
// - P0 has no associated key (leftmost pointer)
// - Pi (i >= 1) contains keys >= Ki and < Ki+1
// - Pn contains keys >= Kn (rightmost pointer)
//
// Search algorithm:
// - Scans from rightmost child backward
// - Returns first child where key >= child's separator key
// - If no match, returns leftmost child (P0)
//
// Parameters:
//   - internalPage: The internal node to search within
//   - key: The key to find appropriate subtree for
//
// Returns:
//   - *BTreePageID: Page ID of the child that should contain the key
//   - nil: If page is not internal or has no children (should not happen)
func (bt *BTree) findChildPointer(internalPage *BTreePage, key types.Field) *BTreePageID {
	if !internalPage.IsInternalPage() || len(internalPage.Children()) == 0 {
		return nil
	}

	children := internalPage.Children()

	// In B+Tree internal nodes:
	// children[0] contains keys < children[1].Key
	// children[i] contains keys >= children[i].Key and < children[i+1].Key
	for i := len(children) - 1; i >= 1; i-- {
		childPtr := children[i]
		if ge, _ := key.Compare(primitives.GreaterThanOrEqual, childPtr.Key); ge {
			return childPtr.ChildPID
		}
	}

	return children[0].ChildPID
}

// compareKeys compares two keys and returns standard comparison result.
// Helper function used throughout tree operations for consistent key ordering.
//
// Parameters:
//   - k1: First key to compare
//   - k2: Second key to compare
//
// Returns:
//   - int: -1 if k1 < k2, 0 if k1 == k2, 1 if k1 > k2
func compareKeys(k1, k2 types.Field) int {
	if k1.Equals(k2) {
		return 0
	}
	if lt, _ := k1.Compare(primitives.LessThan, k2); lt {
		return -1
	}
	return 1
}

// updateParentKey updates the separator key in the parent node after the child's first key changes.
// Called when a leaf's minimum key changes due to insertion at position 0 or deletion.
//
// Why this is needed:
// - Parent separator keys must accurately reflect child subtree boundaries
// - When a leaf's first key changes, the separator in parent becomes stale
// - This maintains B+Tree invariant: parent[i].Key == min(child[i])
//
// The function:
// 1. Checks if child is root (no parent to update)
// 2. Fetches parent page in read-write mode
// 3. Locates child pointer in parent's children array
// 4. Updates the separator key associated with that pointer
// 5. Marks parent page as dirty
//
// Special cases:
// - If child is at index 0 (leftmost), no update needed (P0 has no key)
// - If child not found in parent, returns error (structural inconsistency)
//
// Parameters:
//   - child: The child page whose first key changed
//   - newKey: The new minimum key in the child
//
// Returns:
//   - error: Returns error if parent read fails, child not found, or dirty marking fails
func (bt *BTree) updateParentKey(child *BTreePage, newKey types.Field) error {
	if child.IsRoot() {
		return nil
	}

	parID := btree.NewBTreePageID(bt.indexID, child.Parent())
	parent, err := bt.getPage(parID, transaction.ReadWrite)
	if err != nil {
		return fmt.Errorf("failed to read parent page: %w", err)
	}

	children := parent.Children()

	// Find the child pointer and update its key
	// Note: children[0] has no key in B+tree, so we start from index 1
	childID := child.GetBTreePageID()
	for i := 1; i < len(children); i++ {
		if children[i].ChildPID.Equals(childID) {
			parent.UpdateChildrenKey(i, newKey)
			return bt.addDirtyPage(parent, memory.UpdateOperation)
		}
	}

	// If we get here, the child might be at index 0, which doesn't have a separator key
	// Check if it's the first child
	if len(children) > 0 && children[0].ChildPID.Equals(childID) {

		return nil
	}

	return fmt.Errorf("child not found in parent")
}

// getPage retrieves a page from the buffer pool with proper locking and type checking.
// Wrapper around PageStore.GetPage that handles BTree-specific type casting.
//
// The function:
// 1. Requests page from PageStore (may fetch from disk or buffer pool)
// 2. Acquires appropriate lock based on permissions (shared or exclusive)
// 3. Verifies page is actually a BTreePage (type safety)
// 4. Returns typed BTreePage reference
//
// Parameters:
//   - pageID: Identifier of the page to retrieve
//   - perm: Read-only or read-write access mode
//
// Returns:
//   - *BTreePage: The requested page, properly locked and typed
//   - error: Returns error if page fetch fails or type is incorrect
func (bt *BTree) getPage(pageID *BTreePageID, perm transaction.Permissions) (*BTreePage, error) {
	p, err := bt.store.GetPage(bt.tx, bt.file, pageID, perm)
	if err != nil {
		return nil, fmt.Errorf("failed to get page: %w", err)
	}

	btreePage, ok := p.(*BTreePage)
	if !ok {
		return nil, fmt.Errorf("page is not a BTreePage")
	}

	return btreePage, nil
}

// addDirtyPage marks a page as modified within the current transaction.
// Critical for transaction support - enables rollback and proper commit behavior.
//
// The function:
// 1. Notifies PageStore of page modification
// 2. Records operation type (insert/update/delete) for logging
// 3. Stores before-image for potential rollback
// 4. Queues page for eventual write during commit
//
// Operation types matter for:
// - Recovery: Understanding what changed in log records
// - Rollback: Knowing how to undo the operation
// - Statistics: Tracking modification patterns
//
// Parameters:
//   - p: The page that was modified
//   - op: Type of operation performed (InsertOperation, UpdateOperation, DeleteOperation)
//
// Returns:
//   - error: Returns error if PageStore notification fails
func (bt *BTree) addDirtyPage(p *BTreePage, op memory.OperationType) error {
	return bt.store.HandlePageChange(bt.tx, op, func() ([]page.Page, error) {
		return []page.Page{p}, nil
	})
}

// getSiblingPage retrieves a sibling page (left or right) of the current page.
// Used during merge and redistribution operations to maintain tree balance.
//
// Sibling relationships:
// - Left sibling: direction = -1 (currIdx - 1)
// - Right sibling: direction = +1 (currIdx + 1)
// - Siblings share the same parent node
//
// Parameters:
//   - parent: The parent page containing child pointers
//   - currIdx: Index of current page in parent's children array
//   - direction: -1 for left sibling, +1 for right sibling
//
// Returns:
//   - *BTreePage: The requested sibling page (locked for read-write)
//   - error: Returns error if sibling doesn't exist or page fetch fails
func (bt *BTree) getSiblingPage(parent *BTreePage, currIDx, direction int) (*BTreePage, error) {
	children := parent.Children()
	sibPageID := children[currIDx+direction].ChildPID
	return bt.getPage(sibPageID, transaction.ReadWrite)
}
