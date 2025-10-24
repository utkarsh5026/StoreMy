package statements

type StatementType int

const (
	Select StatementType = iota
	Insert
	Update
	Delete
	CreateTable
	DropTable
	CreateIndex
	DropIndex
	Transaction
	Explain
	ShowIndexes
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
	case CreateIndex:
		return "CREATE INDEX"
	case DropIndex:
		return "DROP INDEX"
	case Transaction:
		return "TRANSACTION"
	case Explain:
		return "EXPLAIN"
	case ShowIndexes:
		return "SHOW INDEXES"
	default:
		return "UNKNOWN"
	}
}

// IsDML returns true if the statement type is a DML operation (INSERT, UPDATE, DELETE, SELECT)
func (st StatementType) IsDML() bool {
	return st == Select || st == Insert || st == Update || st == Delete
}

// IsDDL returns true if the statement type is a DDL operation (CREATE, DROP)
func (st StatementType) IsDDL() bool {
	return st == CreateTable || st == DropTable || st == CreateIndex || st == DropIndex
}

// Statement is the interface that all SQL statements must implement
type Statement interface {
	// GetType returns the type of the statement
	GetType() StatementType
	// String returns a string representation of the statement
	String() string
	// Validate checks if the statement is valid and returns an error if not
	Validate() error
}
