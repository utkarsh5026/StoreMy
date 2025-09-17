package types

type Predicate int

const (
	Equals Predicate = iota
	LessThan
	GreaterThan
	LessThanOrEqual
	GreaterThanOrEqual
	NotEqual
	Like
)

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

	case Like:
		return "LIKE"

	default:
		return "UNKNOWN"
	}
}
