package fields

// Type represents the data type of a field in a database
type Type int

const (
	IntType Type = iota
	StringType
)

// Predicate represents comparison operations for fields
type Predicate int

const (
	Equals Predicate = iota
	NotEquals
	GreaterThan
	LessThan
	GreaterThanOrEqual
	LessThanOrEqual
	Like // Only for StringField
)
