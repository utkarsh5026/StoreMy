package ui

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

var (
	keywords = []string{
		"SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER",
		"ON", "AS", "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
		"CREATE", "TABLE", "DROP", "ALTER", "ADD", "COLUMN", "PRIMARY", "KEY",
		"FOREIGN", "REFERENCES", "INDEX", "UNIQUE", "NOT", "NULL", "DEFAULT",
		"AND", "OR", "IN", "EXISTS", "BETWEEN", "LIKE", "LIMIT", "OFFSET",
		"ORDER", "BY", "GROUP", "HAVING", "DISTINCT", "ALL", "UNION", "EXCEPT",
		"INTERSECT", "CASE", "WHEN", "THEN", "ELSE", "END", "BEGIN", "COMMIT",
		"ROLLBACK", "TRANSACTION", "IF", "INT", "VARCHAR", "TEXT", "FLOAT",
		"BOOLEAN", "DATE", "TIMESTAMP", "WITH", "RECURSIVE", "VIEW",
	}

	functions = []string{
		"COUNT", "SUM", "AVG", "MIN", "MAX", "LENGTH", "SUBSTR", "UPPER",
		"LOWER", "TRIM", "CONCAT", "ROUND", "ABS", "NOW", "CURRENT_DATE",
		"CURRENT_TIME", "COALESCE", "CAST", "CONVERT",
	}

	operators = []string{
		"=", "<>", "!=", "<", ">", "<=", ">=", "+", "-", "*", "/", "%",
	}
)

// SQLHighlighter provides syntax highlighting for SQL queries
type SQLHighlighter struct {
	keywords      map[string]bool
	functions     map[string]bool
	operators     map[string]bool
	keywordStyle  lipgloss.Style
	functionStyle lipgloss.Style
	stringStyle   lipgloss.Style
	numberStyle   lipgloss.Style
	operatorStyle lipgloss.Style
	commentStyle  lipgloss.Style
}

func NewSQLHighlighter() *SQLHighlighter {
	h := &SQLHighlighter{
		keywords:  make(map[string]bool),
		functions: make(map[string]bool),
		operators: make(map[string]bool),
	}

	for _, kw := range keywords {
		h.keywords[kw] = true
		h.keywords[strings.ToLower(kw)] = true
	}

	for _, fn := range functions {
		h.functions[fn] = true
		h.functions[strings.ToLower(fn)] = true
	}

	for _, op := range operators {
		h.operators[op] = true
	}

	h.keywordStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color("#FF79C6")).
		Bold(true)

	h.functionStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color("#8BE9FD")).
		Bold(true)

	h.stringStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color("#F1FA8C"))

	h.numberStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color("#BD93F9"))

	h.operatorStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color("#FFB86C"))

	h.commentStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color("#6272A4")).
		Italic(true)

	return h
}

func (h *SQLHighlighter) Highlight(sql string) string {
	words := strings.Fields(sql)
	highlighted := make([]string, 0, len(words))

	for _, word := range words {
		cleanWord := strings.TrimSuffix(strings.TrimSuffix(word, ","), ";")

		if h.keywords[cleanWord] {
			highlighted = append(highlighted, h.keywordStyle.Render(word))
		} else if h.functions[cleanWord] {
			highlighted = append(highlighted, h.functionStyle.Render(word))
		} else if strings.HasPrefix(word, "'") && strings.HasSuffix(word, "'") {
			highlighted = append(highlighted, h.stringStyle.Render(word))
		} else if isNumeric(word) {
			highlighted = append(highlighted, h.numberStyle.Render(word))
		} else if h.operators[word] {
			highlighted = append(highlighted, h.operatorStyle.Render(word))
		} else if strings.HasPrefix(word, "--") {
			highlighted = append(highlighted, h.commentStyle.Render(word))
		} else {
			highlighted = append(highlighted, word)
		}
	}

	return strings.Join(highlighted, " ")
}

// isNumeric checks if a string represents a number
func isNumeric(s string) bool {
	for _, c := range s {
		if !strings.ContainsRune("0123456789.-", c) {
			return false
		}
	}
	return s != ""
}
