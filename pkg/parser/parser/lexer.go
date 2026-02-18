package parser

import (
	"strings"
	"unicode"
)

// keywords maps uppercase SQL keyword strings to their token types.
var keywords = map[string]TokenType{
	"SELECT":         SELECT,
	"DISTINCT":       DISTINCT,
	"FROM":           FROM,
	"WHERE":          WHERE,
	"JOIN":           JOIN,
	"INNER":          INNER,
	"LEFT":           LEFT,
	"RIGHT":          RIGHT,
	"OUTER":          OUTER,
	"ON":             ON,
	"GROUP":          GROUP,
	"BY":             BY,
	"ORDER":          ORDER,
	"LIMIT":          LIMIT,
	"OFFSET":         OFFSET,
	"ASC":            ASC,
	"DESC":           DESC,
	"AND":            AND,
	"OR":             OR,
	"CREATE":         CREATE,
	"TABLE":          TABLE,
	"DROP":           DROP,
	"IF":             IF,
	"NOT":            NOT,
	"EXISTS":         EXISTS,
	"PRIMARY":        PRIMARY,
	"KEY":            KEY,
	"INDEX":          INDEX,
	"USING":          USING,
	"HASH":           HASH,
	"BTREE":          BTREE,
	"UNION":          UNION,
	"INTERSECT":      INTERSECT,
	"EXCEPT":         EXCEPT,
	"ALL":            ALL,
	"DEFAULT":        DEFAULT,
	"NULL":           NULL,
	"AUTO_INCREMENT": AUTO_INCREMENT,
	"INSERT":         INSERT,
	"INTO":           INTO,
	"VALUES":         VALUES,
	"UPDATE":         UPDATE,
	"SET":            SET,
	"DELETE":         DELETE,
	"BEGIN":          BEGIN,
	"COMMIT":         COMMIT,
	"ROLLBACK":       ROLLBACK,
	"TRANSACTION":    TRANSACTION,
	"EXPLAIN":        EXPLAIN,
	"ANALYZE":        ANALYZE,
	"FORMAT":         FORMAT,
	"SHOW":           SHOW,
	"INDEXES":        INDEXES,
	"INT":            INT,
	"INTEGER":        INT,
	"VARCHAR":        VARCHAR,
	"STRING":         VARCHAR,
	"TEXT":           TEXT,
	"BOOLEAN":        BOOLEAN,
	"BOOL":           BOOLEAN,
	"FLOAT":          FLOAT,
	"REAL":           FLOAT,
	"DOUBLE":         FLOAT,
}

// singleCharTokens maps single-byte punctuation to their token types.
var singleCharTokens = map[byte]TokenType{
	',': COMMA,
	';': SEMICOLON,
	'(': LPAREN,
	')': RPAREN,
	'*': ASTERISK,
}

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
		return Token{Type: EOF, Value: "", Position: l.pos}
	}

	start := l.pos
	ch := l.input[l.pos]

	if tt, ok := singleCharTokens[ch]; ok {
		l.pos++
		return l.createToken(tt, string(ch), start)
	}

	switch {
	case l.isOperatorChar(ch):
		return l.readOperator(start)
	case l.isQuoteChar(ch):
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

func (l *Lexer) isOperatorChar(ch byte) bool {
	return ch == '=' || ch == '<' || ch == '>' || ch == '!'
}

func (l *Lexer) isQuoteChar(ch byte) bool {
	return ch == '\'' || ch == '"'
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
	for l.pos < l.length && l.isIdentChar(l.input[l.pos]) {
		value += string(l.input[l.pos])
		l.pos++
	}
	if tt, ok := keywords[value]; ok {
		return l.createToken(tt, value, start)
	}
	return l.createToken(IDENTIFIER, value, start)
}

func (l *Lexer) isIdentChar(ch byte) bool {
	return unicode.IsLetter(rune(ch)) || unicode.IsDigit(rune(ch)) || ch == '_' || ch == '.'
}
