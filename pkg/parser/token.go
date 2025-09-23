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
)

type Token struct {
	Type     TokenType
	Value    string
	Position int
}
