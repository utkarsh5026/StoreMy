package memory

import (
	"container/list"
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"sync"
)

// PageCache defines the interface for caching database pages in memory.
// It is responsible ONLY for storing and retrieving pages in memory.
// It knows nothing about transactions, locks, or durability.
type PageCache interface {
	Get(pid primitives.PageID) (page.Page, bool)

	Put(pid primitives.PageID, p page.Page) error

	Remove(pid primitives.PageID)

	Size() int

	Clear()

	GetAll() []primitives.PageID
}

// cacheEntry represents a cache entry containing the page ID and page data
type cacheEntry struct {
	pid  primitives.PageID
	page page.Page
}

// LRUPageCache implements an LRU (Least Recently Used) eviction policy for the page cache.
// This implementation uses Go's built-in container/list (doubly linked list) combined with
// a hash map to achieve O(1) operations for all cache operations.
// Note: Uses HashCode() as map key instead of PageID directly to avoid pointer equality issues.
type LRUPageCache struct {
	maxSize int
	cache   map[int]*list.Element // map from HashCode to list element
	lru     *list.List
	mutex   sync.RWMutex
}

// NewLRUPageCache creates a new LRU page cache with the specified maximum size.
func NewLRUPageCache(maxSize int) *LRUPageCache {
	return &LRUPageCache{
		maxSize: maxSize,
		cache:   make(map[int]*list.Element),
		lru:     list.New(),
	}
}

// Get retrieves a page from the cache by its page ID.
// If the page is found, it is marked as recently used by moving it to the front.
func (c *LRUPageCache) Get(pid primitives.PageID) (page.Page, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	hashCode := pid.HashCode()
	if elem, exists := c.cache[hashCode]; exists {
		c.lru.MoveToFront(elem)
		entry := elem.Value.(*cacheEntry)
		return entry.page, true
	}
	return nil, false
}

// Put stores a page in the cache with the given page ID.
// If the page already exists, it updates the existing page and marks it as recently used.
// If the cache is at maximum capacity and the page doesn't exist, returns an error.
func (c *LRUPageCache) Put(pid primitives.PageID, p page.Page) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	hashCode := pid.HashCode()
	if elem, exists := c.cache[hashCode]; exists {
		entry := elem.Value.(*cacheEntry)
		entry.page = p
		c.lru.MoveToFront(elem)
		return nil
	}

	if len(c.cache) >= c.maxSize {
		return fmt.Errorf("cache full, cannot add page")
	}

	entry := &cacheEntry{
		pid:  pid,
		page: p,
	}

	elem := c.lru.PushFront(entry)
	c.cache[hashCode] = elem
	return nil
}

// Remove removes a page from the cache by its page ID.
// Does nothing if the page doesn't exist in the cache.
func (c *LRUPageCache) Remove(pid primitives.PageID) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	hashCode := pid.HashCode()
	if elem, exists := c.cache[hashCode]; exists {
		delete(c.cache, hashCode)
		c.lru.Remove(elem)
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

	c.cache = make(map[int]*list.Element)
	c.lru.Init()
}

// GetAll returns a slice containing all page IDs currently stored in the cache.
// The page IDs are returned in LRU order (least recently used first).
func (c *LRUPageCache) GetAll() []primitives.PageID {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	pids := make([]primitives.PageID, 0, len(c.cache))
	for elem := c.lru.Back(); elem != nil; elem = elem.Prev() {
		entry := elem.Value.(*cacheEntry)
		pids = append(pids, entry.pid)
	}

	return pids
}
