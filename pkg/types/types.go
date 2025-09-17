package types

type Type int

const (
	IntType Type = iota
	StringType
)

// String returns a string representation of the type
func (t Type) String() string {
	switch t {
	case IntType:
		return "INT_TYPE"
	case StringType:
		return "STRING_TYPE"
	default:
		return "UNKNOWN_TYPE"
	}
}
