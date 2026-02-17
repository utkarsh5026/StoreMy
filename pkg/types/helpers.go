package types

import (
	"cmp"
	"encoding/binary"
	"hash/fnv"
	"io"
	"storemy/pkg/primitives"
)

// compareOrdered performs a comparison between two ordered values using the given predicate.
func compareOrdered[T cmp.Ordered](a, b T, op primitives.Predicate) bool {
	switch op {
	case primitives.Equals:
		return a == b
	case primitives.LessThan:
		return a < b
	case primitives.GreaterThan:
		return a > b
	case primitives.LessThanOrEqual:
		return a <= b
	case primitives.GreaterThanOrEqual:
		return a >= b
	case primitives.NotEqual:
		return a != b
	default:
		return false
	}
}

// fnvHash computes an FNV-1a hash of the given byte slice.
func fnvHash(data []byte) primitives.HashCode {
	h := fnv.New32a()
	_, _ = h.Write(data)
	return primitives.HashCode(h.Sum32())
}

// serializeUint32 writes a uint32 value to the writer in big-endian byte order.
func serializeUint32(w io.Writer, v uint32) error {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	_, err := w.Write(b)
	return err
}

// serializeUint64 writes a uint64 value to the writer in big-endian byte order.
func serializeUint64(w io.Writer, v uint64) error {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	_, err := w.Write(b)
	return err
}

// readBytes reads exactly size bytes from the reader.
func readBytes(r io.Reader, size uint32) ([]byte, error) {
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// toBytes32 converts a uint32 value to a 4-byte big-endian slice.
func toBytes32(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

// toBytes64 converts a uint64 value to an 8-byte big-endian slice.
func toBytes64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
