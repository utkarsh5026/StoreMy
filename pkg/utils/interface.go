package utils

import "reflect"

// IsNilInterface checks if an interface value wraps a nil pointer.
//
// In Go, an interface holding a nil pointer is NOT equal to nil:
//
//	var ptr *SomeType = nil
//	var iface SomeInterface = ptr
//	iface == nil  // false (interface has type info)
//	IsNilInterface(iface)  // true (detects nil pointer inside)
//
// This helper uses reflection to properly detect when an interface
// wraps a nil value, which is a common gotcha in Go programming.
//
// Parameters:
//   - i: The interface value to check
//
// Returns:
//   - true if i is nil OR if i wraps a nil pointer/value
//   - false if i contains a valid non-nil value
func IsNilInterface(i any) bool {
	if i == nil {
		return true
	}

	v := reflect.ValueOf(i)

	switch v.Kind() {
	case reflect.Ptr, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func, reflect.Interface:
		return v.IsNil()
	default:
		return false
	}
}
