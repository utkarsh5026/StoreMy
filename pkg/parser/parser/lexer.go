package parser

import (
	"strings"
	"unicode"
)

// Lexer performs lexical analysis on SQL input strings, breaking them into a
// sequence of tokens. It converts the input to uppercase during initialization
// to enable case-insensitive keyword matching.
type Lexer struct {
	input  string
	pos    int
	length int
}

// NewLexer creates a new Lexer for the given SQL input string.
// The input is trimmed and converted to uppercase for case-insensitive parsing.
func NewLexer(input string) *Lexer {
	processedInput := strings.ToUpper(strings.TrimSpace(input))
	return &Lexer{
		input:  processedInput,
		pos:    0,
		length: len(processedInput),
	}
}

// SetPos sets the lexer's current position to the given value.
// The position is only updated if it falls within valid bounds [0, length).
func (l *Lexer) SetPos(pos int) {
	if pos >= 0 && pos < l.length {
		l.pos = pos
	}
}

// NextToken scans and returns the next token from the input.
// It skips leading whitespace and returns an EOF token when the input is exhausted.
func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	if l.pos >= l.length {
		return Token{
			Type:     EOF,
			Value:    "",
			Position: l.pos,
		}
	}

	start := l.pos
	ch := l.input[l.pos]

	switch {
	case ch == ',':
		l.pos++
		return l.createToken(COMMA, ",", start)
	case ch == ';':
		l.pos++
		return l.createToken(SEMICOLON, ";", start)
	case ch == '(':
		l.pos++
		return l.createToken(LPAREN, "(", start)
	case ch == ')':
		l.pos++
		return l.createToken(RPAREN, ")", start)
	case ch == '*':
		l.pos++
		return l.createToken(ASTERISK, "*", start)
	case ch == '=' || ch == '<' || ch == '>' || ch == '!':
		return l.readOperator(start)
	case ch == '\'' || ch == '"':
		return l.readString(start)
	case unicode.IsDigit(rune(ch)):
		return l.readNumber(start)
	case unicode.IsLetter(rune(ch)) || ch == '_':
		return l.readIdentifier(start)
	default:
		l.pos++
		return l.createToken(INVALID, string(ch), start)
	}
}

// skipWhitespace advances the position past any whitespace characters.
func (l *Lexer) skipWhitespace() {
	for l.pos < l.length && unicode.IsSpace(rune(l.input[l.pos])) {
		l.pos++
	}
}

// readOperator reads a comparison operator token (=, <, >, !=, <=, >=, <>, etc.).
func (l *Lexer) readOperator(start int) Token {
	op := ""
	for l.pos < l.length && strings.ContainsRune("=<>!", rune(l.input[l.pos])) {
		op += string(l.input[l.pos])
		l.pos++
	}
	return l.createToken(OPERATOR, op, start)
}

// readString reads a quoted string literal delimited by single or double quotes.
func (l *Lexer) readString(start int) Token {
	quote := l.input[l.pos]
	l.pos++ // Skip opening quote

	value := ""
	for l.pos < l.length && l.input[l.pos] != quote {
		value += string(l.input[l.pos])
		l.pos++
	}

	if l.pos < l.length {
		l.pos++
	}

	return l.createToken(STRING, value, start)
}

// readNumber reads an integer numeric literal.
func (l *Lexer) readNumber(start int) Token {
	value := ""
	for l.pos < l.length && unicode.IsDigit(rune(l.input[l.pos])) {
		value += string(l.input[l.pos])
		l.pos++
	}
	return l.createToken(INT, value, start)
}

// createToken constructs a Token with the given type, value, and starting position.
func (l *Lexer) createToken(t TokenType, value string, start int) Token {
	return Token{
		Type:     t,
		Value:    value,
		Position: start,
	}
}

// readIdentifier reads an identifier or keyword token. It matches the value
// against known SQL keywords and returns the appropriate token type, falling
// back to IDENTIFIER for unrecognized words.
func (l *Lexer) readIdentifier(start int) Token {
	value := ""
	for l.pos < l.length && (unicode.IsLetter(rune(l.input[l.pos])) || unicode.IsDigit(rune(l.input[l.pos])) || l.input[l.pos] == '_' || l.input[l.pos] == '.') {
		value += string(l.input[l.pos])
		l.pos++
	}

	switch value {
	case "SELECT":
		return l.createToken(SELECT, value, start)
	case "DISTINCT":
		return l.createToken(DISTINCT, value, start)
	case "FROM":
		return l.createToken(FROM, value, start)
	case "WHERE":
		return l.createToken(WHERE, value, start)
	case "JOIN":
		return l.createToken(JOIN, value, start)
	case "INNER":
		return l.createToken(INNER, value, start)
	case "LEFT":
		return l.createToken(LEFT, value, start)
	case "RIGHT":
		return l.createToken(RIGHT, value, start)
	case "OUTER":
		return l.createToken(OUTER, value, start)
	case "ON":
		return l.createToken(ON, value, start)
	case "GROUP":
		return l.createToken(GROUP, value, start)
	case "BY":
		return l.createToken(BY, value, start)
	case "ORDER":
		return l.createToken(ORDER, value, start)
	case "LIMIT":
		return l.createToken(LIMIT, value, start)
	case "OFFSET":
		return l.createToken(OFFSET, value, start)
	case "ASC":
		return l.createToken(ASC, value, start)
	case "DESC":
		return l.createToken(DESC, value, start)
	case "AND":
		return l.createToken(AND, value, start)
	case "OR":
		return l.createToken(OR, value, start)

	case "CREATE":
		return l.createToken(CREATE, value, start)
	case "TABLE":
		return l.createToken(TABLE, value, start)
	case "DROP":
		return l.createToken(DROP, value, start)
	case "IF":
		return l.createToken(IF, value, start)
	case "NOT":
		return l.createToken(NOT, value, start)
	case "EXISTS":
		return l.createToken(EXISTS, value, start)
	case "PRIMARY":
		return l.createToken(PRIMARY, value, start)
	case "KEY":
		return l.createToken(KEY, value, start)
	case "INDEX":
		return l.createToken(INDEX, value, start)
	case "USING":
		return l.createToken(USING, value, start)
	case "HASH":
		return l.createToken(HASH, value, start)
	case "BTREE":
		return l.createToken(BTREE, value, start)
	case "UNION":
		return l.createToken(UNION, value, start)
	case "INTERSECT":
		return l.createToken(INTERSECT, value, start)
	case "EXCEPT":
		return l.createToken(EXCEPT, value, start)
	case "ALL":
		return l.createToken(ALL, value, start)
	case "DEFAULT":
		return l.createToken(DEFAULT, value, start)
	case "NULL":
		return l.createToken(NULL, value, start)
	case "AUTO_INCREMENT":
		return l.createToken(AUTO_INCREMENT, value, start)

	case "INSERT":
		return l.createToken(INSERT, value, start)
	case "INTO":
		return l.createToken(INTO, value, start)
	case "VALUES":
		return l.createToken(VALUES, value, start)
	case "UPDATE":
		return l.createToken(UPDATE, value, start)
	case "SET":
		return l.createToken(SET, value, start)
	case "DELETE":
		return l.createToken(DELETE, value, start)

	case "BEGIN":
		return l.createToken(BEGIN, value, start)
	case "COMMIT":
		return l.createToken(COMMIT, value, start)
	case "ROLLBACK":
		return l.createToken(ROLLBACK, value, start)
	case "TRANSACTION":
		return l.createToken(TRANSACTION, value, start)

	case "EXPLAIN":
		return l.createToken(EXPLAIN, value, start)
	case "ANALYZE":
		return l.createToken(ANALYZE, value, start)
	case "FORMAT":
		return l.createToken(FORMAT, value, start)

	case "SHOW":
		return l.createToken(SHOW, value, start)
	case "INDEXES":
		return l.createToken(INDEXES, value, start)

	// Data type keywords
	case "INT", "INTEGER":
		return l.createToken(INT, value, start)
	case "VARCHAR", "STRING":
		return l.createToken(VARCHAR, value, start)
	case "TEXT":
		return l.createToken(TEXT, value, start)
	case "BOOLEAN", "BOOL":
		return l.createToken(BOOLEAN, value, start)
	case "FLOAT", "REAL", "DOUBLE":
		return l.createToken(FLOAT, value, start)

	default:
		return l.createToken(IDENTIFIER, value, start)
	}
}
