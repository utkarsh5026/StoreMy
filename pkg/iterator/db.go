package iterator

import (
	"storemy/pkg/tuple"
)

// DbIterator defines the contract for all database iterators in the execution engine.
// It provides a standardized interface for traversing through collections of tuples
// from various data sources such as tables, indexes, or intermediate query results.
type DbIterator interface {
	// Open initializes the iterator and prepares it for tuple retrieval.
	// This method must be called before any other iterator operations.
	// Multiple calls to Open() on an already opened iterator should be idempotent.
	Open() error

	// HasNext checks if there are more tuples available without consuming them.
	// This method provides lookahead capability and can be called multiple times
	// without advancing the iterator position. The iterator must be opened before calling.
	HasNext() (bool, error)

	// Next retrieves and returns the next tuple from the iterator, advancing the position.
	// The iterator must be opened and have available tuples before calling this method.
	// Use HasNext() to check availability before calling Next().
	Next() (*tuple.Tuple, error)

	// Rewind resets the iterator position to the beginning of the data sequence.
	// After rewinding, the next call to Next() should return the first tuple again.
	// The iterator must be opened before calling this method.
	Rewind() error

	// Close releases all resources associated with the iterator and marks it as closed.
	// After closing, the iterator cannot be used until reopened with Open().
	// Calling Close() on an already closed iterator should be safe and idempotent.
	Close() error

	// GetTupleDesc returns the schema description for tuples produced by this iterator.
	// The tuple description defines the structure, types, and metadata of the tuples
	// that will be returned by Next(). This method can be called regardless of iterator state.
	GetTupleDesc() *tuple.TupleDescription
}

// ForEach applies a processing function to each tuple in the iterator.
// The iteration stops early if processFunc returns an error.
// The iterator must be opened before calling this method.
//
// Example:
//
//	err := iterator.ForEach(iter, func(t *tuple.Tuple) error {
//	    fmt.Println(t)
//	    return nil
//	})
func ForEach(iter DbIterator, processFunc func(*tuple.Tuple) error) error {
	for {
		hasNext, err := iter.HasNext()
		if err != nil {
			return err
		}
		if !hasNext {
			break
		}

		tup, err := iter.Next()
		if err != nil {
			return err
		}
		if tup == nil {
			continue
		}

		if err := processFunc(tup); err != nil {
			return err
		}
	}

	return nil
}

// Filter returns all tuples that satisfy the predicate function.
// The predicate should return true for tuples to include.
//
// Example:
//
//	filtered, err := iterator.Filter(iter, func(t *tuple.Tuple) (bool, error) {
//	    field, _ := t.GetField(0)
//	    return field.(*types.IntField).Value > 10, nil
//	})
func Filter(iter DbIterator, predicate func(*tuple.Tuple) (bool, error)) ([]*tuple.Tuple, error) {
	var results []*tuple.Tuple

	for {
		hasNext, err := iter.HasNext()
		if err != nil {
			return nil, err
		}
		if !hasNext {
			break
		}

		tup, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if tup == nil {
			continue
		}

		matches, err := predicate(tup)
		if err != nil {
			return nil, err
		}
		if matches {
			results = append(results, tup)
		}
	}

	return results, nil
}

// Map transforms each tuple using the provided function and returns the results.
// Nil tuples returned by the transform function are excluded from the results.
//
// Example:
//
//	transformed, err := iterator.Map(iter, func(t *tuple.Tuple) (*tuple.Tuple, error) {
//	    // Create new tuple with modified values
//	    return newTuple, nil
//	})
func Map(iter DbIterator, transform func(*tuple.Tuple) (*tuple.Tuple, error)) ([]*tuple.Tuple, error) {
	var results []*tuple.Tuple

	for {
		hasNext, err := iter.HasNext()
		if err != nil {
			return nil, err
		}
		if !hasNext {
			break
		}

		tup, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if tup == nil {
			continue
		}

		transformed, err := transform(tup)
		if err != nil {
			return nil, err
		}
		if transformed != nil {
			results = append(results, transformed)
		}
	}

	return results, nil
}

// Take returns up to n tuples from the iterator.
// If the iterator has fewer than n tuples, all available tuples are returned.
//
// Example:
//
//	first10, err := iterator.Take(iter, 10)
func Take(iter DbIterator, n int) ([]*tuple.Tuple, error) {
	tuples := make([]*tuple.Tuple, 0, n)

	for range n {
		hasNext, err := iter.HasNext()
		if err != nil {
			return nil, err
		}
		if !hasNext {
			break
		}

		tup, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if tup != nil {
			tuples = append(tuples, tup)
		}
	}

	return tuples, nil
}

// Skip consumes and discards n tuples from the iterator.
// Returns an error if the iterator fails before skipping n tuples.
// Returns nil if the iterator exhausts before n tuples are skipped.
//
// Example:
//
//	err := iterator.Skip(iter, 5) // Skip first 5 tuples
func Skip(iter DbIterator, n int) error {
	for range n {
		hasNext, err := iter.HasNext()
		if err != nil {
			return err
		}
		if !hasNext {
			break
		}

		_, err = iter.Next()
		if err != nil {
			return err
		}
	}

	return nil
}

// Reduce accumulates a value by applying a function to each tuple.
// The accumulator function receives the current accumulated value and the next tuple.
//
// Example:
//
//	sum, err := iterator.Reduce(iter, 0, func(acc int, t *tuple.Tuple) (int, error) {
//	    field, _ := t.GetField(0)
//	    return acc + int(field.(*types.IntField).Value), nil
//	})
func Reduce[T any](iter DbIterator, initial T, accumulator func(T, *tuple.Tuple) (T, error)) (T, error) {
	result := initial

	for {
		hasNext, err := iter.HasNext()
		if err != nil {
			return result, err
		}
		if !hasNext {
			break
		}

		tup, err := iter.Next()
		if err != nil {
			return result, err
		}
		if tup == nil {
			continue
		}

		result, err = accumulator(result, tup)
		if err != nil {
			return result, err
		}
	}

	return result, nil
}
