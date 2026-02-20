package btreeindex

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/memory"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/page"
	"storemy/pkg/types"
	"sync"
)

type BTreeFile = index.BTreeFile
type BTreePage = index.BTreePage

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
//   - rootMu: Protects rootPageID from concurrent reads/writes
type BTree struct {
	indexID    primitives.FileID
	keyType    types.Type
	file       *BTreeFile
	rootPageID *page.PageDescriptor
	rootMu     sync.RWMutex
	tx         *transaction.TransactionContext
	store      *memory.PageStore
}

// NewBTree creates a new B+Tree index with the specified configuration.
// Initializes the tree structure and registers with the PageStore for proper
// buffer management and transaction support.
//
// The indexID parameter should match the file's natural ID (from BaseFile.GetID()).
// This ensures proper coordination between the catalog, physical storage, and page management.
//
// The function:
// 1. Creates the BTree wrapper with provided parameters
// 2. Registers the file with PageStore for flush coordination
// 3. If pages exist, sets root to page 0 (B+Tree convention)
//
// Parameters:
//   - indexID: Unique identifier for this index (should match file.GetID())
//   - keyType: Type of keys this index will store (must be consistent)
//   - file: BTreeFile managing physical storage of pages
//   - tx: Transaction context for ACID properties
//   - store: PageStore for buffer pool and locking
//
// Returns:
//   - *BTree: Initialized B+Tree ready for operations
func NewBTree(indexID primitives.FileID, keyType types.Type, file *BTreeFile, tx *transaction.TransactionContext, store *memory.PageStore) *BTree {
	bt := &BTree{
		indexID: indexID,
		keyType: keyType,
		file:    file,
		tx:      tx,
		store:   store,
	}

	// Set the index ID on the file to ensure page ID validation works correctly
	file.SetIndexID(indexID)

	// Register the BTreeFile with the PageStore so it can flush pages properly
	store.RegisterDbFile(primitives.FileID(indexID), file)

	if file.NumPages() > 0 {
		bt.rootPageID = page.NewPageDescriptor(bt.indexID, 0)
	}

	return bt
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
	err := bt.file.Close()
	bt.store.UnregisterDbFile(bt.indexID)
	return err
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
	bt.rootMu.RLock()
	rootPageID := bt.rootPageID
	bt.rootMu.RUnlock()

	if rootPageID != nil {
		return bt.getPage(rootPageID, perm)
	}

	root, err := bt.file.AllocatePage(bt.tx.ID, bt.keyType, true, primitives.InvalidPageNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate root page: %w", err)
	}

	if err := bt.addDirtyPage(root, memory.InsertOperation); err != nil {
		return nil, fmt.Errorf("failed to mark root page as dirty: %w", err)
	}

	bt.rootMu.Lock()
	bt.rootPageID = root.GetID()
	bt.rootMu.Unlock()

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
func (bt *BTree) findLeafPage(currentPage *BTreePage, key types.Field, perm transaction.Permissions) (*BTreePage, error) {
	if currentPage.IsLeafPage() {
		return currentPage, nil
	}

	childPID := bt.findChildPointer(currentPage, key)
	if childPID == nil {
		return nil, fmt.Errorf("failed to find child pointer for key")
	}

	childPage, err := bt.getPage(childPID, perm)
	if err != nil {
		return nil, fmt.Errorf("failed to read child page: %w", err)
	}

	return bt.findLeafPage(childPage, key, perm)
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
//   - *page.PageDescriptor: Page ID of the child that should contain the key
//   - nil: If page is not internal or has no children (should not happen)
func (bt *BTree) findChildPointer(internalPage *BTreePage, key types.Field) *page.PageDescriptor {
	if !internalPage.IsInternalPage() || len(internalPage.InternalPages) == 0 {
		return nil
	}

	children := internalPage.InternalPages

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

	parID := page.NewPageDescriptor(bt.indexID, child.ParentPage)
	parent, err := bt.getPage(parID, transaction.ReadWrite)
	if err != nil {
		return fmt.Errorf("failed to read parent page: %w", err)
	}

	children := parent.InternalPages

	// Find the child pointer and update its key
	// Note: children[0] has no key in B+tree, so we start from index 1
	childID := child.GetID()
	for i := 1; i < len(children); i++ {
		if children[i].ChildPID.Equals(childID) {
			if err := parent.UpdateChildrenKey(i, newKey); err != nil {
				return fmt.Errorf("failed to update parent key: %w", err)
			}
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
func (bt *BTree) getPage(pageID *page.PageDescriptor, perm transaction.Permissions) (*BTreePage, error) {
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
	children := parent.InternalPages
	sibPageID := children[currIDx+direction].ChildPID
	return bt.getPage(sibPageID, transaction.ReadWrite)
}
