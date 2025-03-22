package fields

// Field is the interface for all field types in the database
type Field interface {
	// Type returns the type of this field
	Type() Type

	// String returns a string representation of this field
	String() string

	// Compare compares this field with another using the given predicate
	Compare(op Predicate, value Field) (bool, error)

	// Serialize converts this field to a byte slice for storage
	Serialize() []byte
}
