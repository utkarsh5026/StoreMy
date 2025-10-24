package ddl

import "fmt"

// DDLResult represents the outcome of DDL operations (CREATE, DROP, ALTER).
type DDLResult struct {
	Success bool
	Message string
}

func (r *DDLResult) String() string {
	return fmt.Sprintf("DDL Result - Success: %t, Message: %s", r.Success, r.Message)
}
