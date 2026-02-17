// Package lexer implements the tokenizer for StoreMy's SQL dialect.
//
// The lexer converts a raw SQL string into a stream of typed tokens that the
// parser can consume. All input is normalized to upper-case during construction
// so that keyword matching is case-insensitive.
//
// # Usage
//
//	l := lexer.NewLexer("SELECT * FROM users WHERE id = 1")
//	for {
//	    tok := l.NextToken()
//	    if tok.Type == lexer.EOF {
//	        break
//	    }
//	    fmt.Printf("type=%d value=%q\n", tok.Type, tok.Value)
//	}
//
// # Token types
//
// Keywords (CREATE, SELECT, INSERT, UPDATE, DELETE, DROP, …), literals
// (INTEGER_LITERAL, FLOAT_LITERAL, STRING_LITERAL), identifiers (IDENTIFIER),
// operators and punctuation (+, -, *, /, =, <, >, (, ), COMMA, SEMICOLON, …),
// and the sentinel EOF token are all defined as TokenType constants.
//
// # Normalization
//
// NewLexer trims surrounding whitespace and upper-cases the entire input before
// tokenizing. String literal contents are preserved as-is (minus the surrounding
// single quotes). Identifiers that are not reserved keywords are emitted as
// IDENTIFIER tokens with their upper-cased value.
package lexer
