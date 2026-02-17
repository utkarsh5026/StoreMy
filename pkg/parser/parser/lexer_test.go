package parser

import (
	"testing"
)

func TestNewLexer(t *testing.T) {
	input := "  select * from table  "
	lexer := NewLexer(input)

	if lexer.input != "SELECT * FROM TABLE" {
		t.Errorf("Expected input to be trimmed and uppercase, got %s", lexer.input)
	}
	if lexer.pos != 0 {
		t.Errorf("Expected pos to be 0, got %d", lexer.pos)
	}
	if lexer.length != len("SELECT * FROM TABLE") {
		t.Errorf("Expected length to be %d, got %d", len("SELECT * FROM TABLE"), lexer.length)
	}
}

func TestLexerKeywords(t *testing.T) {
	tests := []struct {
		input    string
		expected []Token
	}{
		{
			input: "SELECT FROM WHERE",
			expected: []Token{
				{Type: SELECT, Value: "SELECT", Position: 0},
				{Type: FROM, Value: "FROM", Position: 7},
				{Type: WHERE, Value: "WHERE", Position: 12},
				{Type: EOF, Value: "", Position: 17},
			},
		},
		{
			input: "CREATE TABLE DROP",
			expected: []Token{
				{Type: CREATE, Value: "CREATE", Position: 0},
				{Type: TABLE, Value: "TABLE", Position: 7},
				{Type: DROP, Value: "DROP", Position: 13},
				{Type: EOF, Value: "", Position: 17},
			},
		},
		{
			input: "INSERT INTO VALUES",
			expected: []Token{
				{Type: INSERT, Value: "INSERT", Position: 0},
				{Type: INTO, Value: "INTO", Position: 7},
				{Type: VALUES, Value: "VALUES", Position: 12},
				{Type: EOF, Value: "", Position: 18},
			},
		},
		{
			input: "UPDATE SET DELETE",
			expected: []Token{
				{Type: UPDATE, Value: "UPDATE", Position: 0},
				{Type: SET, Value: "SET", Position: 7},
				{Type: DELETE, Value: "DELETE", Position: 11},
				{Type: EOF, Value: "", Position: 17},
			},
		},
		{
			input: "BEGIN COMMIT ROLLBACK TRANSACTION",
			expected: []Token{
				{Type: BEGIN, Value: "BEGIN", Position: 0},
				{Type: COMMIT, Value: "COMMIT", Position: 6},
				{Type: ROLLBACK, Value: "ROLLBACK", Position: 13},
				{Type: TRANSACTION, Value: "TRANSACTION", Position: 22},
				{Type: EOF, Value: "", Position: 33},
			},
		},
		{
			input: "JOIN ON GROUP BY ORDER ASC DESC",
			expected: []Token{
				{Type: JOIN, Value: "JOIN", Position: 0},
				{Type: ON, Value: "ON", Position: 5},
				{Type: GROUP, Value: "GROUP", Position: 8},
				{Type: BY, Value: "BY", Position: 14},
				{Type: ORDER, Value: "ORDER", Position: 17},
				{Type: ASC, Value: "ASC", Position: 23},
				{Type: DESC, Value: "DESC", Position: 27},
				{Type: EOF, Value: "", Position: 31},
			},
		},
		{
			input: "AND OR NOT IF EXISTS",
			expected: []Token{
				{Type: AND, Value: "AND", Position: 0},
				{Type: OR, Value: "OR", Position: 4},
				{Type: NOT, Value: "NOT", Position: 7},
				{Type: IF, Value: "IF", Position: 11},
				{Type: EXISTS, Value: "EXISTS", Position: 14},
				{Type: EOF, Value: "", Position: 20},
			},
		},
		{
			input: "PRIMARY KEY DEFAULT NULL",
			expected: []Token{
				{Type: PRIMARY, Value: "PRIMARY", Position: 0},
				{Type: KEY, Value: "KEY", Position: 8},
				{Type: DEFAULT, Value: "DEFAULT", Position: 12},
				{Type: NULL, Value: "NULL", Position: 20},
				{Type: EOF, Value: "", Position: 24},
			},
		},
	}

	for _, test := range tests {
		lexer := NewLexer(test.input)
		for i, expected := range test.expected {
			token := lexer.NextToken()
			if token.Type != expected.Type {
				t.Errorf("Test %s, token %d: expected type %d, got %d", test.input, i, expected.Type, token.Type)
			}
			if token.Value != expected.Value {
				t.Errorf("Test %s, token %d: expected value %s, got %s", test.input, i, expected.Value, token.Value)
			}
			if token.Position != expected.Position {
				t.Errorf("Test %s, token %d: expected position %d, got %d", test.input, i, expected.Position, token.Position)
			}
		}
	}
}

func TestLexerDataTypes(t *testing.T) {
	tests := []struct {
		input    string
		expected []Token
	}{
		{
			input: "INT INTEGER",
			expected: []Token{
				{Type: INT, Value: "INT", Position: 0},
				{Type: INT, Value: "INTEGER", Position: 4},
				{Type: EOF, Value: "", Position: 11},
			},
		},
		{
			input: "VARCHAR STRING",
			expected: []Token{
				{Type: VARCHAR, Value: "VARCHAR", Position: 0},
				{Type: VARCHAR, Value: "STRING", Position: 8},
				{Type: EOF, Value: "", Position: 14},
			},
		},
		{
			input: "TEXT BOOLEAN BOOL",
			expected: []Token{
				{Type: TEXT, Value: "TEXT", Position: 0},
				{Type: BOOLEAN, Value: "BOOLEAN", Position: 5},
				{Type: BOOLEAN, Value: "BOOL", Position: 13},
				{Type: EOF, Value: "", Position: 17},
			},
		},
		{
			input: "FLOAT REAL DOUBLE",
			expected: []Token{
				{Type: FLOAT, Value: "FLOAT", Position: 0},
				{Type: FLOAT, Value: "REAL", Position: 6},
				{Type: FLOAT, Value: "DOUBLE", Position: 11},
				{Type: EOF, Value: "", Position: 17},
			},
		},
	}

	for _, test := range tests {
		lexer := NewLexer(test.input)
		for i, expected := range test.expected {
			token := lexer.NextToken()
			if token.Type != expected.Type {
				t.Errorf("Test %s, token %d: expected type %d, got %d", test.input, i, expected.Type, token.Type)
			}
			if token.Value != expected.Value {
				t.Errorf("Test %s, token %d: expected value %s, got %s", test.input, i, expected.Value, token.Value)
			}
		}
	}
}

func TestLexerPunctuation(t *testing.T) {
	tests := []struct {
		input    string
		expected []Token
	}{
		{
			input: ",;()",
			expected: []Token{
				{Type: COMMA, Value: ",", Position: 0},
				{Type: SEMICOLON, Value: ";", Position: 1},
				{Type: LPAREN, Value: "(", Position: 2},
				{Type: RPAREN, Value: ")", Position: 3},
				{Type: EOF, Value: "", Position: 4},
			},
		},
		{
			input: "( table1 , table2 ) ;",
			expected: []Token{
				{Type: LPAREN, Value: "(", Position: 0},
				{Type: IDENTIFIER, Value: "TABLE1", Position: 2},
				{Type: COMMA, Value: ",", Position: 9},
				{Type: IDENTIFIER, Value: "TABLE2", Position: 11},
				{Type: RPAREN, Value: ")", Position: 18},
				{Type: SEMICOLON, Value: ";", Position: 20},
				{Type: EOF, Value: "", Position: 21},
			},
		},
	}

	for _, test := range tests {
		lexer := NewLexer(test.input)
		for i, expected := range test.expected {
			token := lexer.NextToken()
			if token.Type != expected.Type {
				t.Errorf("Test %s, token %d: expected type %d, got %d", test.input, i, expected.Type, token.Type)
			}
			if token.Value != expected.Value {
				t.Errorf("Test %s, token %d: expected value %s, got %s", test.input, i, expected.Value, token.Value)
			}
			if token.Position != expected.Position {
				t.Errorf("Test %s, token %d: expected position %d, got %d", test.input, i, expected.Position, token.Position)
			}
		}
	}
}

func TestLexerAsterisk(t *testing.T) {
	tests := []struct {
		input    string
		expected []Token
	}{
		{
			input: "*",
			expected: []Token{
				{Type: ASTERISK, Value: "*", Position: 0},
				{Type: EOF, Value: "", Position: 1},
			},
		},
		{
			input: "SELECT * FROM",
			expected: []Token{
				{Type: SELECT, Value: "SELECT", Position: 0},
				{Type: ASTERISK, Value: "*", Position: 7},
				{Type: FROM, Value: "FROM", Position: 9},
				{Type: EOF, Value: "", Position: 13},
			},
		},
		{
			input: "SELECT*FROM",
			expected: []Token{
				{Type: SELECT, Value: "SELECT", Position: 0},
				{Type: ASTERISK, Value: "*", Position: 6},
				{Type: FROM, Value: "FROM", Position: 7},
				{Type: EOF, Value: "", Position: 11},
			},
		},
		{
			input: "SELECT * , name FROM",
			expected: []Token{
				{Type: SELECT, Value: "SELECT", Position: 0},
				{Type: ASTERISK, Value: "*", Position: 7},
				{Type: COMMA, Value: ",", Position: 9},
				{Type: IDENTIFIER, Value: "NAME", Position: 11},
				{Type: FROM, Value: "FROM", Position: 16},
				{Type: EOF, Value: "", Position: 20},
			},
		},
	}

	for _, test := range tests {
		lexer := NewLexer(test.input)
		for i, expected := range test.expected {
			token := lexer.NextToken()
			if token.Type != expected.Type {
				t.Errorf("Test %s, token %d: expected type %s (%d), got %s (%d)", test.input, i, expected.Type.String(), expected.Type, token.Type.String(), token.Type)
			}
			if token.Value != expected.Value {
				t.Errorf("Test %s, token %d: expected value %s, got %s", test.input, i, expected.Value, token.Value)
			}
			if token.Position != expected.Position {
				t.Errorf("Test %s, token %d: expected position %d, got %d", test.input, i, expected.Position, token.Position)
			}
		}
	}
}

func TestLexerOperators(t *testing.T) {
	tests := []struct {
		input    string
		expected []Token
	}{
		{
			input: "= != < > <= >=",
			expected: []Token{
				{Type: OPERATOR, Value: "=", Position: 0},
				{Type: OPERATOR, Value: "!=", Position: 2},
				{Type: OPERATOR, Value: "<", Position: 5},
				{Type: OPERATOR, Value: ">", Position: 7},
				{Type: OPERATOR, Value: "<=", Position: 9},
				{Type: OPERATOR, Value: ">=", Position: 12},
				{Type: EOF, Value: "", Position: 14},
			},
		},
	}

	for _, test := range tests {
		lexer := NewLexer(test.input)
		for i, expected := range test.expected {
			token := lexer.NextToken()
			if token.Type != expected.Type {
				t.Errorf("Test %s, token %d: expected type %d, got %d", test.input, i, expected.Type, token.Type)
			}
			if token.Value != expected.Value {
				t.Errorf("Test %s, token %d: expected value %s, got %s", test.input, i, expected.Value, token.Value)
			}
			if token.Position != expected.Position {
				t.Errorf("Test %s, token %d: expected position %d, got %d", test.input, i, expected.Position, token.Position)
			}
		}
	}
}

func TestLexerStrings(t *testing.T) {
	tests := []struct {
		input    string
		expected []Token
	}{
		{
			input: "'hello world'",
			expected: []Token{
				{Type: STRING, Value: "HELLO WORLD", Position: 0},
				{Type: EOF, Value: "", Position: 13},
			},
		},
		{
			input: "\"hello world\"",
			expected: []Token{
				{Type: STRING, Value: "HELLO WORLD", Position: 0},
				{Type: EOF, Value: "", Position: 13},
			},
		},
		{
			input: "'test' \"another\"",
			expected: []Token{
				{Type: STRING, Value: "TEST", Position: 0},
				{Type: STRING, Value: "ANOTHER", Position: 7},
				{Type: EOF, Value: "", Position: 16},
			},
		},
		{
			input: "''",
			expected: []Token{
				{Type: STRING, Value: "", Position: 0},
				{Type: EOF, Value: "", Position: 2},
			},
		},
	}

	for _, test := range tests {
		lexer := NewLexer(test.input)
		for i, expected := range test.expected {
			token := lexer.NextToken()
			if token.Type != expected.Type {
				t.Errorf("Test %s, token %d: expected type %d, got %d", test.input, i, expected.Type, token.Type)
			}
			if token.Value != expected.Value {
				t.Errorf("Test %s, token %d: expected value %s, got %s", test.input, i, expected.Value, token.Value)
			}
			if token.Position != expected.Position {
				t.Errorf("Test %s, token %d: expected position %d, got %d", test.input, i, expected.Position, token.Position)
			}
		}
	}
}

func TestLexerNumbers(t *testing.T) {
	tests := []struct {
		input    string
		expected []Token
	}{
		{
			input: "123",
			expected: []Token{
				{Type: INT, Value: "123", Position: 0},
				{Type: EOF, Value: "", Position: 3},
			},
		},
		{
			input: "0 999 42",
			expected: []Token{
				{Type: INT, Value: "0", Position: 0},
				{Type: INT, Value: "999", Position: 2},
				{Type: INT, Value: "42", Position: 6},
				{Type: EOF, Value: "", Position: 8},
			},
		},
	}

	for _, test := range tests {
		lexer := NewLexer(test.input)
		for i, expected := range test.expected {
			token := lexer.NextToken()
			if token.Type != expected.Type {
				t.Errorf("Test %s, token %d: expected type %d, got %d", test.input, i, expected.Type, token.Type)
			}
			if token.Value != expected.Value {
				t.Errorf("Test %s, token %d: expected value %s, got %s", test.input, i, expected.Value, token.Value)
			}
			if token.Position != expected.Position {
				t.Errorf("Test %s, token %d: expected position %d, got %d", test.input, i, expected.Position, token.Position)
			}
		}
	}
}

func TestLexerIdentifiers(t *testing.T) {
	tests := []struct {
		input    string
		expected []Token
	}{
		{
			input: "user_id table1 my_table",
			expected: []Token{
				{Type: IDENTIFIER, Value: "USER_ID", Position: 0},
				{Type: IDENTIFIER, Value: "TABLE1", Position: 8},
				{Type: IDENTIFIER, Value: "MY_TABLE", Position: 15},
				{Type: EOF, Value: "", Position: 23},
			},
		},
		{
			input: "schema.table",
			expected: []Token{
				{Type: IDENTIFIER, Value: "SCHEMA.TABLE", Position: 0},
				{Type: EOF, Value: "", Position: 12},
			},
		},
		{
			input: "_underscore start_with_underscore",
			expected: []Token{
				{Type: IDENTIFIER, Value: "_UNDERSCORE", Position: 0},
				{Type: IDENTIFIER, Value: "START_WITH_UNDERSCORE", Position: 12},
				{Type: EOF, Value: "", Position: 33},
			},
		},
	}

	for _, test := range tests {
		lexer := NewLexer(test.input)
		for i, expected := range test.expected {
			token := lexer.NextToken()
			if token.Type != expected.Type {
				t.Errorf("Test %s, token %d: expected type %d, got %d", test.input, i, expected.Type, token.Type)
			}
			if token.Value != expected.Value {
				t.Errorf("Test %s, token %d: expected value %s, got %s", test.input, i, expected.Value, token.Value)
			}
			if token.Position != expected.Position {
				t.Errorf("Test %s, token %d: expected position %d, got %d", test.input, i, expected.Position, token.Position)
			}
		}
	}
}

func TestLexerWhitespace(t *testing.T) {
	input := "SELECT   FROM   WHERE"
	expected := []Token{
		{Type: SELECT, Value: "SELECT", Position: 0},
		{Type: FROM, Value: "FROM", Position: 9},
		{Type: WHERE, Value: "WHERE", Position: 16},
		{Type: EOF, Value: "", Position: 21},
	}

	lexer := NewLexer(input)
	for i, expectedToken := range expected {
		token := lexer.NextToken()
		if token.Type != expectedToken.Type {
			t.Errorf("Token %d: expected type %d, got %d", i, expectedToken.Type, token.Type)
		}
		if token.Value != expectedToken.Value {
			t.Errorf("Token %d: expected value %s, got %s", i, expectedToken.Value, token.Value)
		}
		if token.Position != expectedToken.Position {
			t.Errorf("Token %d: expected position %d, got %d", i, expectedToken.Position, token.Position)
		}
	}
}

func TestLexerInvalidToken(t *testing.T) {
	input := "@#$"
	expected := []Token{
		{Type: INVALID, Value: "@", Position: 0},
		{Type: INVALID, Value: "#", Position: 1},
		{Type: INVALID, Value: "$", Position: 2},
		{Type: EOF, Value: "", Position: 3},
	}

	lexer := NewLexer(input)
	for i, expectedToken := range expected {
		token := lexer.NextToken()
		if token.Type != expectedToken.Type {
			t.Errorf("Token %d: expected type %d, got %d", i, expectedToken.Type, token.Type)
		}
		if token.Value != expectedToken.Value {
			t.Errorf("Token %d: expected value %s, got %s", i, expectedToken.Value, token.Value)
		}
		if token.Position != expectedToken.Position {
			t.Errorf("Token %d: expected position %d, got %d", i, expectedToken.Position, token.Position)
		}
	}
}

func TestLexerComplexSQL(t *testing.T) {
	input := "SELECT name, age FROM users WHERE age > 18 AND name = 'John';"
	expected := []Token{
		{Type: SELECT, Value: "SELECT", Position: 0},
		{Type: IDENTIFIER, Value: "NAME", Position: 7},
		{Type: COMMA, Value: ",", Position: 11},
		{Type: IDENTIFIER, Value: "AGE", Position: 13},
		{Type: FROM, Value: "FROM", Position: 17},
		{Type: IDENTIFIER, Value: "USERS", Position: 22},
		{Type: WHERE, Value: "WHERE", Position: 28},
		{Type: IDENTIFIER, Value: "AGE", Position: 34},
		{Type: OPERATOR, Value: ">", Position: 38},
		{Type: INT, Value: "18", Position: 40},
		{Type: AND, Value: "AND", Position: 43},
		{Type: IDENTIFIER, Value: "NAME", Position: 47},
		{Type: OPERATOR, Value: "=", Position: 52},
		{Type: STRING, Value: "JOHN", Position: 54},
		{Type: SEMICOLON, Value: ";", Position: 60},
		{Type: EOF, Value: "", Position: 61},
	}

	lexer := NewLexer(input)
	for i, expectedToken := range expected {
		token := lexer.NextToken()
		if token.Type != expectedToken.Type {
			t.Errorf("Token %d: expected type %d, got %d", i, expectedToken.Type, token.Type)
		}
		if token.Value != expectedToken.Value {
			t.Errorf("Token %d: expected value %s, got %s", i, expectedToken.Value, token.Value)
		}
		if token.Position != expectedToken.Position {
			t.Errorf("Token %d: expected position %d, got %d", i, expectedToken.Position, token.Position)
		}
	}
}

func TestLexerSelectStarQueries(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []Token
	}{
		{
			name:  "Simple SELECT * FROM",
			input: "SELECT * FROM users",
			expected: []Token{
				{Type: SELECT, Value: "SELECT", Position: 0},
				{Type: ASTERISK, Value: "*", Position: 7},
				{Type: FROM, Value: "FROM", Position: 9},
				{Type: IDENTIFIER, Value: "USERS", Position: 14},
				{Type: EOF, Value: "", Position: 19},
			},
		},
		{
			name:  "SELECT * with JOIN",
			input: "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
			expected: []Token{
				{Type: SELECT, Value: "SELECT", Position: 0},
				{Type: ASTERISK, Value: "*", Position: 7},
				{Type: FROM, Value: "FROM", Position: 9},
				{Type: IDENTIFIER, Value: "USERS", Position: 14},
				{Type: JOIN, Value: "JOIN", Position: 20},
				{Type: IDENTIFIER, Value: "ORDERS", Position: 25},
				{Type: ON, Value: "ON", Position: 32},
				{Type: IDENTIFIER, Value: "USERS.ID", Position: 35},
				{Type: OPERATOR, Value: "=", Position: 44},
				{Type: IDENTIFIER, Value: "ORDERS.USER_ID", Position: 46},
				{Type: EOF, Value: "", Position: 60},
			},
		},
		{
			name:  "SELECT * with WHERE",
			input: "SELECT * FROM users WHERE id > 10",
			expected: []Token{
				{Type: SELECT, Value: "SELECT", Position: 0},
				{Type: ASTERISK, Value: "*", Position: 7},
				{Type: FROM, Value: "FROM", Position: 9},
				{Type: IDENTIFIER, Value: "USERS", Position: 14},
				{Type: WHERE, Value: "WHERE", Position: 20},
				{Type: IDENTIFIER, Value: "ID", Position: 26},
				{Type: OPERATOR, Value: ">", Position: 29},
				{Type: INT, Value: "10", Position: 31},
				{Type: EOF, Value: "", Position: 33},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewLexer(test.input)
			for i, expectedToken := range test.expected {
				token := lexer.NextToken()
				if token.Type != expectedToken.Type {
					t.Errorf("Token %d: expected type %s (%d), got %s (%d)", i, expectedToken.Type.String(), expectedToken.Type, token.Type.String(), token.Type)
				}
				if token.Value != expectedToken.Value {
					t.Errorf("Token %d: expected value %s, got %s", i, expectedToken.Value, token.Value)
				}
				if token.Position != expectedToken.Position {
					t.Errorf("Token %d: expected position %d, got %d", i, expectedToken.Position, token.Position)
				}
			}
		})
	}
}

func TestLexerEmptyInput(t *testing.T) {
	lexer := NewLexer("")
	token := lexer.NextToken()

	if token.Type != EOF {
		t.Errorf("Expected EOF token, got %d", token.Type)
	}
	if token.Value != "" {
		t.Errorf("Expected empty value, got %s", token.Value)
	}
	if token.Position != 0 {
		t.Errorf("Expected position 0, got %d", token.Position)
	}
}

func TestLexerUnterminatedString(t *testing.T) {
	input := "'unterminated"
	lexer := NewLexer(input)
	token := lexer.NextToken()

	if token.Type != STRING {
		t.Errorf("Expected STRING token, got %d", token.Type)
	}
	if token.Value != "UNTERMINATED" {
		t.Errorf("Expected 'UNTERMINATED', got %s", token.Value)
	}
}
