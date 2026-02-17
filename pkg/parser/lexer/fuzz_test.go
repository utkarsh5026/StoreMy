package lexer

import "testing"

func FuzzLexer(f *testing.F) {
	// Seed corpus: valid SQL fragments and edge cases that exercise
	// different code paths in the tokenizer.
	seeds := []string{
		"SELECT * FROM users",
		"INSERT INTO t (a, b) VALUES (1, 'hello')",
		"CREATE TABLE foo (id INTEGER PRIMARY KEY)",
		"UPDATE users SET name = 'alice' WHERE id = 1",
		"DELETE FROM orders WHERE total > 100",
		"SELECT COUNT(*) FROM items GROUP BY category HAVING COUNT(*) > 5",
		"DROP TABLE IF EXISTS temp",
		"CREATE INDEX idx_name ON users (name)",
		"EXPLAIN SELECT * FROM users",
		"SHOW INDEXES FROM users",
		// Edge cases
		"",
		"   ",
		"SELECT",
		"'unclosed string",
		"123abc",
		"--",
		"/*",
		"SELECT 1.5e10",
		"SELECT 'it''s fine'",
		"SELECT NULL",
		"(((())))",
		"\x00\x01\x02",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		l := NewLexer(input)
		// Drain all tokens; the lexer must never panic regardless of input.
		for i := 0; i < 10_000; i++ {
			if l.NextToken().Type == EOF {
				break
			}
		}
	})
}
