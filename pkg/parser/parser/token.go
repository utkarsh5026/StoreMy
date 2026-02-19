package parser

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
	AUTO_INCREMENT
	SELECT
	DISTINCT
	FROM
	WHERE
	JOIN
	INNER
	LEFT
	RIGHT
	OUTER
	ON
	GROUP
	ORDER
	BY
	LIMIT
	OFFSET
	INDEX
	USING
	HASH
	BTREE
	UNION
	INTERSECT
	EXCEPT
	ALL

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

	EXPLAIN
	ANALYZE
	FORMAT

	SHOW
	INDEXES

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
	ASTERISK

	INVALID
	EOF
)

var tokenTypeNames = map[TokenType]string{
	CREATE:         "CREATE",
	TABLE:          "TABLE",
	DROP:           "DROP",
	IF:             "IF",
	NOT:            "NOT",
	EXISTS:         "EXISTS",
	PRIMARY:        "PRIMARY",
	KEY:            "KEY",
	DEFAULT:        "DEFAULT",
	NULL:           "NULL",
	AUTO_INCREMENT: "AUTO_INCREMENT",
	SELECT:         "SELECT",
	DISTINCT:       "DISTINCT",
	FROM:           "FROM",
	WHERE:          "WHERE",
	JOIN:           "JOIN",
	INNER:          "INNER",
	LEFT:           "LEFT",
	RIGHT:          "RIGHT",
	OUTER:          "OUTER",
	ON:             "ON",
	GROUP:          "GROUP",
	ORDER:          "ORDER",
	BY:             "BY",
	LIMIT:          "LIMIT",
	OFFSET:         "OFFSET",
	INDEX:          "INDEX",
	USING:          "USING",
	HASH:           "HASH",
	BTREE:          "BTREE",
	UNION:          "UNION",
	INTERSECT:      "INTERSECT",
	EXCEPT:         "EXCEPT",
	ALL:            "ALL",
	ASC:            "ASC",
	DESC:           "DESC",
	AND:            "AND",
	OR:             "OR",
	INSERT:         "INSERT",
	INTO:           "INTO",
	VALUES:         "VALUES",
	UPDATE:         "UPDATE",
	SET:            "SET",
	DELETE:         "DELETE",
	BEGIN:          "BEGIN",
	COMMIT:         "COMMIT",
	ROLLBACK:       "ROLLBACK",
	TRANSACTION:    "TRANSACTION",
	EXPLAIN:        "EXPLAIN",
	ANALYZE:        "ANALYZE",
	FORMAT:         "FORMAT",
	SHOW:           "SHOW",
	INDEXES:        "INDEXES",
	INT:            "INT",
	VARCHAR:        "VARCHAR",
	TEXT:           "TEXT",
	BOOLEAN:        "BOOLEAN",
	FLOAT:          "FLOAT",
	OPERATOR:       "OPERATOR",
	STRING:         "STRING",
	IDENTIFIER:     "IDENTIFIER",
	COMMA:          "COMMA",
	SEMICOLON:      "SEMICOLON",
	LPAREN:         "LPAREN",
	RPAREN:         "RPAREN",
	ASTERISK:       "ASTERISK",
	INVALID:        "INVALID",
	EOF:            "EOF",
}

func (t TokenType) String() string {
	if name, ok := tokenTypeNames[t]; ok {
		return name
	}
	return "UNKNOWN"
}

type Token struct {
	Type     TokenType
	Value    string
	Position int
}
