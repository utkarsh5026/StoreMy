package iterator

import "storemy/pkg/tuple"

// Iterate is a generic helper function that encapsulates the common iteration pattern.
// It handles HasNext/Next logic and skips nil tuples automatically.
// The processFunc receives each tuple and can control iteration flow:
// - Return (false, nil) to stop iteration early
// - Return (true, nil) to continue
// - Return (_, error) to stop with error
//
// This function works with any iterator that implements TupleIterator (DbIterator, DbFileIterator, etc.)
func Iterate(iter TupleIterator, processFunc func(*tuple.Tuple) (continueLooping bool, err error)) error {
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

		shouldContinue, err := processFunc(tup)
		if err != nil {
			return err
		}
		if !shouldContinue {
			break
		}
	}

	return nil
}

// ForEach applies a processing function to each tuple in the iterator.
// The iteration stops early if processFunc returns an error.
// The iterator must be opened before calling this method.
//
// Works with both DbIterator and DbFileIterator.
func ForEach(iter TupleIterator, processFunc func(*tuple.Tuple) error) error {
	return Iterate(iter, func(tup *tuple.Tuple) (bool, error) {
		err := processFunc(tup)
		return true, err
	})
}

// Filter returns all tuples that satisfy the predicate function.
// The predicate should return true for tuples to include.
//
// Works with both DbIterator and DbFileIterator.
func Filter(iter TupleIterator, predicate func(*tuple.Tuple) (bool, error)) ([]*tuple.Tuple, error) {
	var results []*tuple.Tuple

	err := Iterate(iter, func(tup *tuple.Tuple) (bool, error) {
		matches, err := predicate(tup)
		if err != nil {
			return false, err
		}
		if matches {
			results = append(results, tup)
		}
		return true, nil
	})

	return results, err
}

// Map transforms each tuple using the provided function and returns the results.
// Nil tuples returned by the transform function are excluded from the results.
//
// Works with both DbIterator and DbFileIterator.
func Map(iter TupleIterator, transform func(*tuple.Tuple) (*tuple.Tuple, error)) ([]*tuple.Tuple, error) {
	var results []*tuple.Tuple

	err := Iterate(iter, func(tup *tuple.Tuple) (bool, error) {
		transformed, err := transform(tup)
		if err != nil {
			return false, err
		}
		if transformed != nil {
			results = append(results, transformed)
		}
		return true, nil
	})

	return results, err
}

// Take returns up to n tuples from the iterator.
// If the iterator has fewer than n tuples, all available tuples are returned.
//
// Works with both DbIterator and DbFileIterator.
func Take(iter TupleIterator, n int) ([]*tuple.Tuple, error) {
	tuples := make([]*tuple.Tuple, 0, n)

	err := Iterate(iter, func(tup *tuple.Tuple) (bool, error) {
		tuples = append(tuples, tup)
		return len(tuples) < n, nil
	})

	return tuples, err
}

// Skip consumes and discards n tuples from the iterator.
// Returns an error if the iterator fails before skipping n tuples.
// Returns nil if the iterator exhausts before n tuples are skipped.
//
// Works with both DbIterator and DbFileIterator.
func Skip(iter TupleIterator, n int) error {
	count := 0
	return Iterate(iter, func(tup *tuple.Tuple) (bool, error) {
		count++
		return count < n, nil
	})
}

// Reduce accumulates a value by applying a function to each tuple.
// The accumulator function receives the current accumulated value and the next tuple.
//
// Works with both DbIterator and DbFileIterator.
func Reduce[T any](iter TupleIterator, initial T, accumulator func(T, *tuple.Tuple) (T, error)) (T, error) {
	result := initial

	err := Iterate(iter, func(tup *tuple.Tuple) (bool, error) {
		var err error
		result, err = accumulator(result, tup)
		return true, err
	})

	return result, err
}

// Count returns the total number of tuples in the iterator.
// Note: This consumes the entire iterator.
//
// Works with both DbIterator and DbFileIterator.
func Count(iter TupleIterator) (int, error) {
	return Reduce(iter, 0, func(count int, _ *tuple.Tuple) (int, error) {
		return count + 1, nil
	})
}

// Collect returns all tuples from the iterator as a slice.
// Note: This consumes the entire iterator and loads all tuples into memory.
//
// Works with both DbIterator and DbFileIterator.
func Collect(iter TupleIterator) ([]*tuple.Tuple, error) {
	var results []*tuple.Tuple

	err := Iterate(iter, func(tup *tuple.Tuple) (bool, error) {
		results = append(results, tup)
		return true, nil
	})

	return results, err
}
