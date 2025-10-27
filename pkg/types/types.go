package types

type Type int

const (
	IntType Type = iota // Backward compatibility, equivalent to Int64Type
	Int32Type
	Int64Type
	Uint32Type
	Uint64Type
	StringType
	BoolType
	FloatType
)

// String returns a string representation of the type
func (t Type) String() string {
	switch t {
	case IntType:
		return "INT_TYPE"
	case Int32Type:
		return "INT32_TYPE"
	case Int64Type:
		return "INT64_TYPE"
	case Uint32Type:
		return "UINT32_TYPE"
	case Uint64Type:
		return "UINT64_TYPE"
	case StringType:
		return "STRING_TYPE"
	case BoolType:
		return "BOOL_TYPE"
	case FloatType:
		return "FLOAT_TYPE"
	default:
		return "UNKNOWN_TYPE"
	}
}

func (t Type) Size() uint32 {
	switch t {
	case IntType:
		return 8
	case Int32Type:
		return 4
	case Int64Type:
		return 8
	case Uint32Type:
		return 4
	case Uint64Type:
		return 8
	case FloatType:
		return 8
	case StringType:
		return 4 + StringMaxSize // 4 bytes for length + max string size
	case BoolType:
		return 1
	default:
		return 0
	}
}
