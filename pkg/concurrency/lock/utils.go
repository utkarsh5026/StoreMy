package lock

// updateOrDelete updates the map with the new slice, or deletes the key if the slice is empty.
// This maintains map cleanliness by avoiding storage of empty slices.
func updateOrDelete[K comparable, V any](m map[K][]V, key K, newSlice []V) {
	if len(newSlice) > 0 {
		m[key] = newSlice
	} else {
		delete(m, key)
	}
}
