package utils

// HashString creates a simple hash of a string that fits in int32 range
func HashString(s string) int {
	hash := int32(0)
	for _, c := range s {
		hash = 31*hash + int32(c)
	}
	return int(hash)
}
