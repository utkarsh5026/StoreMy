package parser

import (
	"strings"
	"unicode"
)

type Lexer struct {
	input  string
	pos    int
	length int
}

func NewLexer(input string) *Lexer {
	return &Lexer{
		input:  strings.ToUpper(strings.TrimSpace(input)),
		pos:    0,
		length: len(input),
	}
}

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
		return createToken(COMMA, ",", start)
	case ch == ';':
		l.pos++
		return createToken(SEMICOLON, ";", start)
	case ch == '(':
		l.pos++
		return createToken(LPAREN, "(", start)
	case ch == ')':
		l.pos++
		return createToken(RPAREN, ")", start)
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
		return createToken(INVALID, string(ch), start)
	}
}

func (l *Lexer) skipWhitespace() {
	for l.pos < l.length && unicode.IsSpace(rune(l.input[l.pos])) {
		l.pos++
	}
}

func (l *Lexer) readOperator(start int) Token {
	op := ""
	for l.pos < l.length && strings.ContainsRune("=<>!", rune(l.input[l.pos])) {
		op += string(l.input[l.pos])
		l.pos++
	}
	return createToken(OPERATOR, op, start)
}

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

	return createToken(STRING, value, start)
}

func (l *Lexer) readNumber(start int) Token {
	value := ""
	for l.pos < l.length && unicode.IsDigit(rune(l.input[l.pos])) {
		value += string(l.input[l.pos])
		l.pos++
	}
	return createToken(INT, value, start)
}

func createToken(t TokenType, value string, start int) Token {
	return Token{
		Type:     t,
		Value:    value,
		Position: start,
	}
}

func (l *Lexer) readIdentifier(start int) Token {
	value := ""
	for l.pos < l.length && (unicode.IsLetter(rune(l.input[l.pos])) || unicode.IsDigit(rune(l.input[l.pos])) || l.input[l.pos] == '_' || l.input[l.pos] == '.') {
		value += string(l.input[l.pos])
		l.pos++
	}

	switch value {
	case "SELECT":
		return createToken(SELECT, value, start)
	case "FROM":
		return createToken(FROM, value, start)
	case "WHERE":
		return createToken(WHERE, value, start)
	case "JOIN":
		return createToken(JOIN, value, start)
	case "ON":
		return createToken(ON, value, start)
	case "GROUP":
		return createToken(GROUP, value, start)
	case "BY":
		return createToken(BY, value, start)
	case "ORDER":
		return createToken(ORDER, value, start)
	case "ASC":
		return createToken(ASC, value, start)
	case "DESC":
		return createToken(DESC, value, start)
	case "AND":
		return createToken(AND, value, start)
	case "OR":
		return createToken(OR, value, start)

	case "CREATE":
		return createToken(CREATE, value, start)
	case "TABLE":
		return createToken(TABLE, value, start)
	case "DROP":
		return createToken(DROP, value, start)
	case "IF":
		return createToken(IF, value, start)
	case "NOT":
		return createToken(NOT, value, start)
	case "EXISTS":
		return createToken(EXISTS, value, start)
	case "PRIMARY":
		return createToken(PRIMARY, value, start)
	case "KEY":
		return createToken(KEY, value, start)
	case "DEFAULT":
		return createToken(DEFAULT, value, start)
	case "NULL":
		return createToken(NULL, value, start)

	case "INSERT":
		return createToken(INSERT, value, start)
	case "INTO":
		return createToken(INTO, value, start)
	case "VALUES":
		return createToken(VALUES, value, start)
	case "UPDATE":
		return createToken(UPDATE, value, start)
	case "SET":
		return createToken(SET, value, start)
	case "DELETE":
		return createToken(DELETE, value, start)

	case "BEGIN":
		return createToken(BEGIN, value, start)
	case "COMMIT":
		return createToken(COMMIT, value, start)
	case "ROLLBACK":
		return createToken(ROLLBACK, value, start)
	case "TRANSACTION":
		return createToken(TRANSACTION, value, start)

	// Data type keywords
	case "INT", "INTEGER":
		return createToken(INT, value, start)
	case "VARCHAR", "STRING":
		return createToken(VARCHAR, value, start)
	case "TEXT":
		return createToken(TEXT, value, start)
	case "BOOLEAN", "BOOL":
		return createToken(BOOLEAN, value, start)
	case "FLOAT", "REAL", "DOUBLE":
		return createToken(FLOAT, value, start)

	default:
		return createToken(IDENTIFIER, value, start)
	}
}
