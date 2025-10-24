package statements

// ExplainOptions contains options for the EXPLAIN command
type ExplainOptions struct {
	Analyze bool   // If true, actually execute the query and show real statistics
	Format  string // Output format: "TEXT", "JSON", etc. (default: "TEXT")
}

// ExplainStatement represents an EXPLAIN command
type ExplainStatement struct {
	BaseStatement
	Statement Statement      // The underlying statement to explain
	Options   ExplainOptions // Options for the explain command
}

// NewExplainStatement creates a new EXPLAIN statement
func NewExplainStatement(stmt Statement, options ExplainOptions) *ExplainStatement {
	return &ExplainStatement{
		BaseStatement: NewBaseStatement(Explain),
		Statement:     stmt,
		Options:       options,
	}
}

// GetStatement returns the underlying statement being explained
func (es *ExplainStatement) GetStatement() Statement {
	return es.Statement
}

// GetOptions returns the explain options
func (es *ExplainStatement) GetOptions() ExplainOptions {
	return es.Options
}

// String returns a string representation of the EXPLAIN statement
func (es *ExplainStatement) String() string {
	result := "EXPLAIN"
	if es.Options.Analyze {
		result += " ANALYZE"
	}
	if es.Options.Format != "" && es.Options.Format != "TEXT" {
		result += " FORMAT " + es.Options.Format
	}
	result += " " + es.Statement.String()
	return result
}

// Validate checks if the EXPLAIN statement is valid
func (es *ExplainStatement) Validate() error {
	if es.Statement == nil {
		return NewValidationError(Explain, "Statement", "EXPLAIN statement must have an underlying statement")
	}
	return es.Statement.Validate()
}
