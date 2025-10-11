package functools

import "fmt"

// ...existing Map, Filter, Reduce functions...

// MapWithError transforms elements with error-returning function
// Use this when transformation can fail (e.g., parsing, I/O, validation)
func MapWithError[T any, R any](slice []T, fn func(T) (R, error)) ([]R, error) {
	if slice == nil {
		return nil, nil
	}
	result := make([]R, 0, len(slice))
	for i, v := range slice {
		r, err := fn(v)
		if err != nil {
			return nil, fmt.Errorf("map failed at index %d: %w", i, err)
		}
		result = append(result, r)
	}
	return result, nil
}

// FilterWithError filters with error-returning predicate
// Use when predicate evaluation can fail
func FilterWithError[T any](slice []T, predicate func(T) (bool, error)) ([]T, error) {
	if slice == nil {
		return nil, nil
	}
	result := make([]T, 0, len(slice))
	for i, v := range slice {
		keep, err := predicate(v)
		if err != nil {
			return nil, fmt.Errorf("filter predicate failed at index %d: %w", i, err)
		}
		if keep {
			result = append(result, v)
		}
	}
	return result, nil
}

// ReduceWithError reduces with error-returning accumulator
// Use when accumulation logic can fail
func ReduceWithError[T any, R any](slice []T, initialValue R, fn func(R, T) (R, error)) (R, error) {
	result := initialValue
	for i, v := range slice {
		var err error
		result, err = fn(result, v)
		if err != nil {
			var zero R
			return zero, fmt.Errorf("reduce failed at index %d: %w", i, err)
		}
	}
	return result, nil
}

// Map - pure transformation, no errors
func Map[T any, R any](slice []T, fn func(T) R) []R {
	if slice == nil {
		return nil
	}
	result := make([]R, len(slice))
	for i, v := range slice {
		result[i] = fn(v)
	}
	return result
}

// Filter - predicate testing, no errors
func Filter[T any](slice []T, predicate func(T) bool) []T {
	if slice == nil {
		return nil
	}
	result := make([]T, 0, len(slice))
	for _, v := range slice {
		if predicate(v) {
			result = append(result, v)
		}
	}
	return result
}

// Reduce - accumulation, no errors
func Reduce[T any, R any](slice []T, initialValue R, fn func(R, T) R) R {
	result := initialValue
	for _, v := range slice {
		result = fn(result, v)
	}
	return result
}
