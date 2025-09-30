// Package memory provides in-memory caching implementations for database pages.
package memory

import (
	"fmt"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"sync"
)

// PageCache defines the interface for caching database pages in memory.
// It is responsible ONLY for storing and retrieving pages in memory.
// It knows nothing about transactions, locks, or durability.
type PageCache interface {
	// Get retrieves a page from the cache by its page ID.
	// Returns the page and true if found, or nil page and false if not found.
	Get(pid tuple.PageID) (page.Page, bool)

	// Put stores a page in the cache with the given page ID.
	// Returns an error if the page cannot be stored (e.g., cache is full).
	// If the page already exists, it should be updated.
	Put(pid tuple.PageID, p page.Page) error

	// Remove removes a page from the cache by its page ID.
	// Does nothing if the page doesn't exist.
	Remove(pid tuple.PageID)

	// Size returns the current number of pages in the cache.
	Size() int

	// Clear removes all pages from the cache.
	Clear()

	// GetAll returns a slice of all page IDs currently in the cache.
	// The order of page IDs is not guaranteed.
	GetAll() []tuple.PageID
}

// node represents a single node in the doubly linked list
type node struct {
	pid  tuple.PageID
	page page.Page
	prev *node
	next *node
}

// LRUPageCache implements an LRU (Least Recently Used) eviction policy for the page cache.
// This implementation uses a doubly linked list combined with a hash map to achieve O(1)
// operations for all cache operations.
//
// The cache is thread-safe and uses a read-write mutex to allow concurrent reads
// while ensuring exclusive access for writes.
//
// When the cache reaches maximum capacity, attempting to add new pages will return an error
// rather than automatically evicting the least recently used page.
type LRUPageCache struct {
	maxSize int                    // Maximum number of pages the cache can hold
	cache   map[tuple.PageID]*node // Map for O(1) page lookup
	head    *node                  // Dummy head node (most recently used end)
	tail    *node                  // Dummy tail node (least recently used end)
	mutex   sync.RWMutex           // Read-write mutex for thread safety
}

// NewLRUPageCache creates a new LRU page cache with the specified maximum size.
func NewLRUPageCache(maxSize int) *LRUPageCache {
	// Create dummy head and tail nodes
	head := &node{}
	tail := &node{}
	head.next = tail
	tail.prev = head

	return &LRUPageCache{
		maxSize: maxSize,
		cache:   make(map[tuple.PageID]*node),
		head:    head,
		tail:    tail,
	}
}

// addToFront adds a node right after the head (marks as most recently used) - O(1)
func (c *LRUPageCache) addToFront(n *node) {
	n.prev = c.head
	n.next = c.head.next
	c.head.next.prev = n
	c.head.next = n
}

// removeNode removes a node from the linked list - O(1)
func (c *LRUPageCache) removeNode(n *node) {
	n.prev.next = n.next
	n.next.prev = n.prev
}

// moveToFront moves an existing node to the front (marks as most recently used) - O(1)
func (c *LRUPageCache) moveToFront(n *node) {
	c.removeNode(n)
	c.addToFront(n)
}

// Get retrieves a page from the cache by its page ID.
// If the page is found, it is marked as recently used by moving it to the front.
func (c *LRUPageCache) Get(pid tuple.PageID) (page.Page, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if n, exists := c.cache[pid]; exists {
		c.moveToFront(n)
		return n.page, true
	}
	return nil, false
}

// Put stores a page in the cache with the given page ID.
// If the page already exists, it updates the existing page and marks it as recently used.
// If the cache is at maximum capacity and the page doesn't exist, returns an error.
func (c *LRUPageCache) Put(pid tuple.PageID, p page.Page) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if n, exists := c.cache[pid]; exists {
		n.page = p
		c.moveToFront(n)
		return nil
	}

	if len(c.cache) >= c.maxSize {
		return fmt.Errorf("cache full, cannot add page")
	}

	newNode := &node{
		pid:  pid,
		page: p,
	}
	c.cache[pid] = newNode
	c.addToFront(newNode)
	return nil
}

// Remove removes a page from the cache by its page ID.
// Does nothing if the page doesn't exist in the cache.
func (c *LRUPageCache) Remove(pid tuple.PageID) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if n, exists := c.cache[pid]; exists {
		delete(c.cache, pid)
		c.removeNode(n)
	}
}

// Size returns the current number of pages stored in the cache.
func (c *LRUPageCache) Size() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return len(c.cache)
}

// Clear removes all pages from the cache and resets it to an empty state.
// After calling Clear, the cache will have size 0 and contain no pages.
func (c *LRUPageCache) Clear() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.cache = make(map[tuple.PageID]*node)
	c.head.next = c.tail
	c.tail.prev = c.head
}

// GetAll returns a slice containing all page IDs currently stored in the cache.
// The page IDs are returned in LRU order (least recently used first).
func (c *LRUPageCache) GetAll() []tuple.PageID {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	pids := make([]tuple.PageID, 0, len(c.cache))
	current := c.tail.prev
	for current != c.head {
		pids = append(pids, current.pid)
		current = current.prev
	}

	return pids
}
