package lexer

type TokenType int

const (
	CREATE TokenType = iota
	TABLE
	DROP
	IF
	NOT
	EXISTS
	PRIMARY
	KEY
	DEFAULT
	NULL
	SELECT
	FROM
	WHERE
	JOIN
	ON
	GROUP
	ORDER
	BY
	LIMIT
	OFFSET

	ASC
	DESC
	AND
	OR

	INSERT
	INTO
	VALUES
	UPDATE
	SET
	DELETE

	BEGIN
	COMMIT
	ROLLBACK
	TRANSACTION

	INT
	VARCHAR
	TEXT
	BOOLEAN
	FLOAT
	OPERATOR
	STRING
	IDENTIFIER

	COMMA
	SEMICOLON
	LPAREN
	RPAREN

	INVALID
	EOF
)

func (t TokenType) String() string {
	switch t {
	case CREATE:
		return "CREATE"
	case TABLE:
		return "TABLE"
	case DROP:
		return "DROP"
	case IF:
		return "IF"
	case NOT:
		return "NOT"
	case EXISTS:
		return "EXISTS"
	case PRIMARY:
		return "PRIMARY"
	case KEY:
		return "KEY"
	case DEFAULT:
		return "DEFAULT"
	case NULL:
		return "NULL"
	case SELECT:
		return "SELECT"
	case FROM:
		return "FROM"
	case WHERE:
		return "WHERE"
	case JOIN:
		return "JOIN"
	case ON:
		return "ON"
	case GROUP:
		return "GROUP"
	case ORDER:
		return "ORDER"
	case BY:
		return "BY"
	case LIMIT:
		return "LIMIT"
	case OFFSET:
		return "OFFSET"
	case ASC:
		return "ASC"
	case DESC:
		return "DESC"
	case AND:
		return "AND"
	case OR:
		return "OR"
	case INSERT:
		return "INSERT"
	case INTO:
		return "INTO"
	case VALUES:
		return "VALUES"
	case UPDATE:
		return "UPDATE"
	case SET:
		return "SET"
	case DELETE:
		return "DELETE"
	case BEGIN:
		return "BEGIN"
	case COMMIT:
		return "COMMIT"
	case ROLLBACK:
		return "ROLLBACK"
	case TRANSACTION:
		return "TRANSACTION"
	case INT:
		return "INT"
	case VARCHAR:
		return "VARCHAR"
	case TEXT:
		return "TEXT"
	case BOOLEAN:
		return "BOOLEAN"
	case FLOAT:
		return "FLOAT"
	case OPERATOR:
		return "OPERATOR"
	case STRING:
		return "STRING"
	case IDENTIFIER:
		return "IDENTIFIER"
	case COMMA:
		return "COMMA"
	case SEMICOLON:
		return "SEMICOLON"
	case LPAREN:
		return "LPAREN"
	case RPAREN:
		return "RPAREN"
	case INVALID:
		return "INVALID"
	case EOF:
		return "EOF"
	default:
		return "UNKNOWN"
	}
}

type Token struct {
	Type     TokenType
	Value    string
	Position int
}
