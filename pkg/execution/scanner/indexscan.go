package scanner

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/iterator"
	"storemy/pkg/memory"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// IndexScan implements an index scan operator that uses an index to efficiently
// locate tuples matching a search condition, rather than scanning the entire table.
//
// IndexScan is a fundamental access method for indexed lookups in query execution.
// It uses either equality search (for hash and btree indexes) or range search
// (for btree indexes only) to find matching tuple locations, then fetches the
// actual tuples from the heap file.
//
// Performance characteristics:
//   - Equality search: O(1) for hash index, O(log n) for btree
//   - Range search: O(log n + k) for btree, where k is number of matching tuples
//   - Much faster than sequential scan when selectivity is low
type IndexScan struct {
	base       *iterator.BaseIterator
	tx         *transaction.TransactionContext
	idx        index.Index
	heapFile   *heap.HeapFile
	store      *memory.PageStore
	tupleDesc  *tuple.TupleDescription
	scanType   IndexScanType
	searchKey  types.Field // For equality search
	startKey   types.Field // For range search
	endKey     types.Field // For range search
	resultRIDs []RecID     // Record IDs returned by index
	currentPos int         // Current position in resultRIDs
}

// IndexScanType indicates the type of index scan being performed
type IndexScanType int

const (
	// EqualityScan performs an exact match search (e.g., WHERE id = 5)
	EqualityScan IndexScanType = iota
	// RangeScan performs a range search (e.g., WHERE age >= 18 AND age <= 65)
	RangeScan
)

type RecID = *tuple.TupleRecordID

// IndexScanConfig holds the core infrastructure dependencies for creating an IndexScan.
// These are the common required components that don't change based on scan type.
type IndexScanConfig struct {
	Tx       *transaction.TransactionContext
	Index    index.Index
	HeapFile *heap.HeapFile
	Store    *memory.PageStore
}

// validate checks that all required fields in the config are non-nil.
func (cfg *IndexScanConfig) validate() error {
	if cfg.Store == nil {
		return fmt.Errorf("page store cannot be nil")
	}
	if cfg.Index == nil {
		return fmt.Errorf("index cannot be nil")
	}
	if cfg.HeapFile == nil {
		return fmt.Errorf("heap file cannot be nil")
	}
	return nil
}

// NewIndexEqualityScan creates a new index scan operator for equality predicates.
//
// Parameters:
//   - cfg: Configuration containing transaction, index, heap file, and page store
//   - searchKey: The exact key value to search for (e.g., 5 for "WHERE id = 5")
//
// Returns a configured IndexScan operator or an error if initialization fails.
func NewIndexEqualityScan(cfg IndexScanConfig, searchKey types.Field) (*IndexScan, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	is := createIndexScan(&cfg, EqualityScan)
	is.searchKey = searchKey

	is.base = iterator.NewBaseIterator(is.readNext)
	return is, nil
}

// NewIndexRangeScan creates a new index scan operator for range predicates
//
// Parameters:
//   - cfg: Configuration containing transaction, index, heap file, and page store
//   - startKey: Lower bound of the range (inclusive)
//   - endKey: Upper bound of the range (inclusive)
func NewIndexRangeScan(cfg IndexScanConfig, start, end types.Field) (*IndexScan, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	is := createIndexScan(&cfg, RangeScan)
	is.startKey = start
	is.endKey = end

	is.base = iterator.NewBaseIterator(is.readNext)
	return is, nil
}

// createIndexScan creates IndexScan from config
func createIndexScan(cfg *IndexScanConfig, scanType IndexScanType) *IndexScan {
	return &IndexScan{
		tx:        cfg.Tx,
		idx:       cfg.Index,
		heapFile:  cfg.HeapFile,
		store:     cfg.Store,
		tupleDesc: cfg.HeapFile.GetTupleDesc(),
		scanType:  scanType,
	}
}

// Open initializes the IndexScan operator by performing the index lookup.
// This is where the actual index search happens - we get all matching RecordIDs
// from the index, then fetch tuples from the heap file during iteration.
//
// Returns an error if the index search fails.
func (is *IndexScan) Open() error {
	var err error
	var rids []RecID

	switch is.scanType {
	case EqualityScan:
		rids, err = is.idx.Search(is.searchKey)
		if err != nil {
			return fmt.Errorf("index equality search failed: %w", err)
		}

	case RangeScan:
		rids, err = is.idx.RangeSearch(is.startKey, is.endKey)
		if err != nil {
			return fmt.Errorf("index range search failed: %w", err)
		}

	default:
		return fmt.Errorf("invalid scan type: %d", is.scanType)
	}

	is.resultRIDs = rids
	is.currentPos = 0
	is.base.MarkOpened()
	return nil
}

// Close releases resources associated with the IndexScan operator.
// Note: This does NOT close the index or heap file, as they're managed
// externally and may be shared across multiple scans.
func (is *IndexScan) Close() error {
	is.resultRIDs = nil
	is.currentPos = 0
	return is.base.Close()
}

// GetTupleDesc returns the tuple description (schema) for tuples produced by this scan.
// This is the schema of the table being scanned, not the index.
func (is *IndexScan) GetTupleDesc() *tuple.TupleDescription {
	return is.tupleDesc
}

// HasNext checks if there are more tuples available in the index scan.
func (is *IndexScan) HasNext() (bool, error) {
	return is.base.HasNext()
}

// Next retrieves the next tuple from the index scan.
func (is *IndexScan) Next() (*tuple.Tuple, error) {
	return is.base.Next()
}

// readNext is the internal method that fetches tuples using RecordIDs from the index.
// It iterates through the result set and fetches corresponding tuples from the heap file,
// skipping over any deleted tuples (nil entries).
//
// Returns:
//   - The next tuple if available
//   - nil when all tuples have been read
//   - An error if tuple fetch fails
func (is *IndexScan) readNext() (*tuple.Tuple, error) {
	for is.currentPos < len(is.resultRIDs) {
		rid := is.resultRIDs[is.currentPos]
		is.currentPos++

		tup, err := is.fetchTupleByRID(rid)
		if err != nil {
			return nil, err
		}

		if tup != nil {
			return tup, nil
		}
	}

	return nil, nil
}

// fetchTupleByRID retrieves a tuple from the heap file using its RecordID.
// This method handles page fetching and tuple extraction.
//
// Returns:
//   - The tuple at the given RID, or nil if the tuple has been deleted
//   - An error if page fetch or tuple extraction fails
func (is *IndexScan) fetchTupleByRID(rid RecID) (*tuple.Tuple, error) {
	pageID := rid.PageID.(*page.PageDescriptor)
	page, err := is.store.GetPageReadOnly(is.tx, is.heapFile, pageID)
	if err != nil {
		return nil, fmt.Errorf("failed to get page %v: %w", pageID, err)
	}

	hp, ok := page.(*heap.HeapPage)
	if !ok {
		return nil, fmt.Errorf("expected HeapPage, got %T", page)
	}

	tup, err := hp.GetTupleAt(rid.TupleNum)
	if err != nil {
		return nil, fmt.Errorf("failed to get tuple at slot %d: %w", rid.TupleNum, err)
	}

	return tup, nil
}

// Rewind resets the IndexScan operator to the beginning of the result set.
// This allows the scan to be re-executed without performing another index lookup.
func (is *IndexScan) Rewind() error {
	is.currentPos = 0
	is.base.ClearCache()
	return nil
}
