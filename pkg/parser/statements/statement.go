package statements

type StatementType int

const (
	Select StatementType = iota
	Insert
	Update
	Delete
	CreateTable
	DropTable
	Transaction
)

func (st StatementType) String() string {
	switch st {
	case Select:
		return "SELECT"
	case Insert:
		return "INSERT"
	case Update:
		return "UPDATE"
	case Delete:
		return "DELETE"
	case CreateTable:
		return "CREATE TABLE"
	case DropTable:
		return "DROP TABLE"
	case Transaction:
		return "TRANSACTION"
	default:
		return "UNKNOWN"
	}
}

type Statement interface {
	GetType() StatementType
	String() string
}
