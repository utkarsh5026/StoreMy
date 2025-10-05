package statements

import "fmt"

type ValidationError struct {
	Statement StatementType
	Field     string
	Message   string
}

func (ve *ValidationError) Error() string {
	return fmt.Sprintf("%s validation error: %s - %s", ve.Statement.String(), ve.Field, ve.Message)
}

func NewValidationError(stmt StatementType, field, message string) *ValidationError {
	return &ValidationError{
		Statement: stmt,
		Field:     field,
		Message:   message,
	}
}

type Validator interface {
	Validate() error
}
