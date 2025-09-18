package utils

// HashString creates a simple hash of a string
func HashString(s string) int {
	hash := 0
	for _, c := range s {
		hash = 31*hash + int(c)
	}
	return hash
}
