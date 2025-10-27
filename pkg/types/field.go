package types

import (
	"io"
	"storemy/pkg/primitives"
)

// Field represents a generic field that can be serialized, compared, and hashed.
// It provides methods for type checking, string representation, equality, and comparison operations.
type Field interface {
	// Serialize writes the field's data to the provided writer.
	Serialize(w io.Writer) error

	// Compare evaluates the field against another field using the given primitives.Predicate operation.
	// Returns true if the comparison holds, or an error if the operation is invalid.
	Compare(op primitives.Predicate, other Field) (bool, error)

	// Type returns the type of the field.
	Type() Type

	// String returns a string representation of the field.
	String() string

	// Equals checks if the field is equal to another field.
	Equals(other Field) bool

	// Hash computes a hash value for the field.
	// Returns the hash as a uint32, or an error if hashing fails.
	Hash() (primitives.HashCode, error)

	// Length returns the length of the field's data in bytes.
	// This is useful for serialization and storage purposes.
	Length() uint32
}
