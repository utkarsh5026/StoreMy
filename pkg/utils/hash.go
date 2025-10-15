package utils

import (
	"hash/fnv"
)

// HashString creates a hash of a string using FNV-1a algorithm.
// FNV-1a provides good distribution and is fast for small inputs like file paths.
// Returns a 32-bit hash value cast to int for compatibility.
func HashString(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}
