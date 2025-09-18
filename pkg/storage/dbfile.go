package storage

import "storemy/pkg/tuple"

// DbFileIterator defines the interface for iterating over tuples in a database file
type DbFileIterator interface {
	// Open prepares the iterator for use
	Open() error

	// HasNext returns true if there are more tuples available
	HasNext() (bool, error)

	// Next returns the next tuple in the iteration
	Next() (*tuple.Tuple, error)

	// Rewind resets the iterator to the beginning
	Rewind() error

	// Close releases any resources held by the iterator
	Close() error
}

type DbFile interface {
	ReadPage(pid tuple.PageID) (Page, error)

	WritePage(p Page) error

	AddTuple(tid *TransactionID, t *tuple.Tuple) ([]Page, error)

	DeleteTuple(tid *TransactionID, t *tuple.Tuple) (Page, error)

	Iterator(tid *TransactionID) DbFileIterator

	GetID() int

	GetTupleDesc() *tuple.TupleDescription
}
