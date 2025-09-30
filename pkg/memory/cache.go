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
//
// Implementations should be thread-safe and handle concurrent access properly.
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

// LRUPageCache implements an LRU (Least Recently Used) eviction policy for the page cache.
// This implementation maintains a single responsibility: maintain an ordered cache with LRU eviction.
//
// The cache is thread-safe and uses a read-write mutex to allow concurrent reads
// while ensuring exclusive access for writes. Pages are tracked in both a map for
// O(1) access and a slice for maintaining LRU order.
//
// When the cache reaches maximum capacity, attempting to add new pages will return an error
// rather than automatically evicting the least recently used page.
type LRUPageCache struct {
	maxSize     int                        // Maximum number of pages the cache can hold
	cache       map[tuple.PageID]page.Page // Map for O(1) page lookup
	accessOrder []tuple.PageID             // Slice tracking access order, most recently used at the end
	mutex       sync.RWMutex               // Read-write mutex for thread safety
}

// NewLRUPageCache creates a new LRU page cache with the specified maximum size.
func NewLRUPageCache(maxSize int) *LRUPageCache {
	return &LRUPageCache{
		maxSize:     maxSize,
		cache:       make(map[tuple.PageID]page.Page),
		accessOrder: make([]tuple.PageID, 0, maxSize),
	}
}

// Get retrieves a page from the cache by its page ID.
// If the page is found, it is marked as recently used by updating the access order.
//
// Parameters:
//   - pid: The page ID to look up.
//
// Returns:
//   - page.Page: The cached page if found, nil otherwise.
//   - bool: true if the page was found, false otherwise.
func (c *LRUPageCache) Get(pid tuple.PageID) (page.Page, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	p, exists := c.cache[pid]
	if exists {
		c.updateAccessOrder(pid)
	}
	return p, exists
}

// Put stores a page in the cache with the given page ID.
// If the page already exists, it updates the existing page and marks it as recently used.
// If the cache is at maximum capacity and the page doesn't exist, returns an error.
func (c *LRUPageCache) Put(pid tuple.PageID, p page.Page) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, exists := c.cache[pid]; exists {
		c.cache[pid] = p
		c.updateAccessOrder(pid)
		return nil
	}

	if len(c.cache) >= c.maxSize {
		return fmt.Errorf("cache full, cannot add page")
	}

	c.cache[pid] = p
	c.accessOrder = append(c.accessOrder, pid)
	return nil
}

// Remove removes a page from the cache by its page ID.
// Does nothing if the page doesn't exist in the cache.
func (c *LRUPageCache) Remove(pid tuple.PageID) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	delete(c.cache, pid)
	c.removeFromAccessOrder(pid)
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

	c.cache = make(map[tuple.PageID]page.Page)
	c.accessOrder = make([]tuple.PageID, 0, c.maxSize)
}

// GetAll returns a slice containing all page IDs currently stored in the cache.
// The order of page IDs in the returned slice is not guaranteed and may vary
// between calls.
func (c *LRUPageCache) GetAll() []tuple.PageID {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	pids := make([]tuple.PageID, 0, len(c.cache))
	for pid := range c.cache {
		pids = append(pids, pid)
	}
	return pids
}

// updateAccessOrder moves the given page ID to the end of the access order slice,
// marking it as the most recently used page. If the page ID doesn't exist in the
// access order, it is simply appended.
func (c *LRUPageCache) updateAccessOrder(pid tuple.PageID) {
	for i, pageID := range c.accessOrder {
		if pageID.Equals(pid) {
			c.accessOrder = append(c.accessOrder[:i], c.accessOrder[i+1:]...)
			break
		}
	}
	c.accessOrder = append(c.accessOrder, pid)
}

// removeFromAccessOrder removes the given page ID from the access order slice.
// If the page ID doesn't exist in the access order, this method does nothing.
func (c *LRUPageCache) removeFromAccessOrder(pid tuple.PageID) {
	for i, pageID := range c.accessOrder {
		if pageID.Equals(pid) {
			c.accessOrder = append(c.accessOrder[:i], c.accessOrder[i+1:]...)
			return
		}
	}
}
