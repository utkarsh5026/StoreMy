package types

type Type int

const (
	IntType Type = iota
	StringType
	BoolType
)

// String returns a string representation of the type
func (t Type) String() string {
	switch t {
	case IntType:
		return "INT_TYPE"
	case StringType:
		return "STRING_TYPE"
	case BoolType:
		return "BOOL_TYPE"
	default:
		return "UNKNOWN_TYPE"
	}
}

func (t Type) Size() uint32 {
	switch t {
	case IntType:
		return 4
	case StringType:
		return 4 + StringMaxSize // 4 bytes for length + max string size
	case BoolType:
		return 1
	default:
		return 0
	}
}
