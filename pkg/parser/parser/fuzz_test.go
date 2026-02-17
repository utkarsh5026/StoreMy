package parser

import "testing"

func FuzzParseStatement(f *testing.F) {
	// Seed corpus: representative statements plus common malformed inputs.
	seeds := []string{
		"SELECT * FROM users WHERE id = 1",
		"SELECT id, name FROM users ORDER BY name ASC LIMIT 10",
		"SELECT COUNT(*) FROM orders GROUP BY status HAVING COUNT(*) > 2",
		"INSERT INTO t (a, b) VALUES (1, 'x')",
		"INSERT INTO t (a) VALUES (1), (2), (3)",
		"UPDATE users SET age = 30 WHERE id = 5",
		"DELETE FROM logs WHERE created_at < '2024-01-01'",
		"CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
		"CREATE INDEX idx_email ON users (email)",
		"DROP TABLE IF EXISTS temp",
		"DROP INDEX idx_old",
		"EXPLAIN SELECT * FROM users",
		"SHOW INDEXES FROM users",
		// Truncated / malformed
		"SELECT",
		"INSERT INTO",
		"CREATE TABLE",
		"DROP",
		"",
		"SELECT * FROM",
		"WHERE id = 1",
		"VALUES (1, 2",
		"SELECT 1 FROM t JOIN",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		// ParseStatement must never panic on arbitrary input.
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("ParseStatement panicked on %q: %v", input, r)
			}
		}()
		_, _ = ParseStatement(input)
	})
}
