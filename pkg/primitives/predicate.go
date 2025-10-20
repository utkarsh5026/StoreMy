package primitives

// Predicate represents a comparison operator used in SQL WHERE clauses and JOIN conditions.
// It defines the relationship between two values in a filter expression.
type Predicate int

const (
	// Equals represents the = operator for equality comparison
	Equals Predicate = iota

	// LessThan represents the < operator
	LessThan

	// GreaterThan represents the > operator
	GreaterThan

	// LessThanOrEqual represents the <= operator
	LessThanOrEqual

	// GreaterThanOrEqual represents the >= operator
	GreaterThanOrEqual

	// NotEqual represents the != operator for inequality comparison
	NotEqual

	// NotEqualsBracket represents the <> operator (alternative notation for NotEqual)
	NotEqualsBracket

	// Like represents the LIKE operator for pattern matching
	Like
)

// String returns the SQL string representation of the predicate operator.
// This is used for query planning, error messages, and debugging output.
func (p Predicate) String() string {
	switch p {
	case Equals:
		return "="

	case LessThan:
		return "<"

	case GreaterThan:
		return ">"

	case LessThanOrEqual:
		return "<="

	case GreaterThanOrEqual:
		return ">="

	case NotEqual:
		return "!="

	case NotEqualsBracket:
		return "<>"

	case Like:
		return "LIKE"

	default:
		return "UNKNOWN"
	}
}
