package error

import (
	"fmt"
	"runtime"
	"strings"
)

// ErrorCategory classifies errors by their nature and appropriate handling strategy.
// This classification helps determine whether an error should trigger retries,
// user notifications, or system alerts.
type ErrorCategory int

const (
	// ErrCategoryUser represents errors caused by invalid user input or operations.
	// Examples: invalid SQL syntax, constraint violations, permission denied.
	// These errors are typically fixable by modifying the user's request.
	ErrCategoryUser ErrorCategory = iota

	// ErrCategoryTransient represents temporary errors that might succeed on retry.
	// Examples: lock timeouts, network interruptions, temporary resource exhaustion.
	// Clients should implement exponential backoff retry logic for these errors.
	ErrCategoryTransient

	// ErrCategorySystem represents errors requiring administrator intervention.
	// Examples: disk full, configuration errors, missing files, permission issues.
	// These errors typically cannot be resolved by retrying or changing user input.
	ErrCategorySystem

	// ErrCategoryData represents errors related to data corruption or integrity.
	// Examples: checksum failures, invalid page formats, corrupted indexes.
	// These errors may require database repair or recovery procedures.
	ErrCategoryData

	// ErrCategoryConcurrency represents errors from concurrent transaction conflicts.
	// Examples: deadlocks, serialization failures, lock conflicts.
	// These errors are often resolved by transaction retry with proper backoff.
	ErrCategoryConcurrency
)

// DBError represents a structured database error with rich context information.
type DBError struct {
	// Code is a unique identifier for this error type (e.g., "DEADLOCK_DETECTED", "PAGE_CORRUPTED").
	Code string

	// Category classifies the error for appropriate handling strategy.
	Category ErrorCategory

	// Message is a human-readable description of what went wrong.
	Message string

	// Detail provides additional context about the specific error instance.
	// Example: "Table 'users' already exists" where Message might be "Relation already exists".
	Detail string

	// Hint suggests how the user might fix or work around this error.
	// Example: "Try using CREATE TABLE IF NOT EXISTS instead".
	Hint string

	// Operation identifies the database operation that was being performed when the error occurred.
	// Examples: "InsertTuple", "SeqScan", "CreateTable", "AcquireLock".
	Operation string

	// Component identifies the system component where the error originated.
	// Examples: "PageStore", "LockManager", "QueryPlanner", "HeapPage".
	Component string

	// Cause is the underlying error that triggered this database error.
	// This enables error chaining while preserving the original error context.
	Cause error

	// Stack contains the call stack where this error was created.
	// Used for debugging and is automatically captured in New() and Wrap().
	Stack []uintptr
}

// New creates a new DBError with the specified code, category, and message.
func New(category ErrorCategory, code, message string) *DBError {
	err := &DBError{
		Code:     code,
		Category: category,
		Message:  message,
		Stack:    captureStack(),
	}
	return err
}

// Wrap wraps an existing error with database-specific context information.
// If the error is already a DBError, it enriches the existing error with
// operation and component context (only if not already set).
func Wrap(err error, code, operation, component string) *DBError {
	if err == nil {
		return nil
	}

	if dbErr, ok := err.(*DBError); ok {
		if dbErr.Operation == "" {
			dbErr.Operation = operation
		}
		if dbErr.Component == "" {
			dbErr.Component = component
		}
		return dbErr
	}

	return &DBError{
		Code:      code,
		Category:  ErrCategorySystem,
		Message:   err.Error(),
		Operation: operation,
		Component: component,
		Cause:     err,
		Stack:     captureStack(),
	}
}

// captureStack captures the current call stack for debugging purposes.
// It skips the first 3 frames to exclude captureStack, New/Wrap, and the
// immediate caller, focusing on the actual error origin.
func captureStack() []uintptr {
	const depth = 32
	var pcs [depth]uintptr
	n := runtime.Callers(3, pcs[:])
	return pcs[0:n]
}

// Error implements the standard Go error interface
//
// The format follows the pattern:
// [ERROR_CODE] Message: Detail (operation: Operation, component: Component) caused by: underlying error
func (e *DBError) Error() string {
	var b strings.Builder

	b.WriteString(fmt.Sprintf("[%s] %s", e.Code, e.Message))

	if e.Detail != "" {
		b.WriteString(fmt.Sprintf(": %s", e.Detail))
	}

	if e.Operation != "" {
		b.WriteString(fmt.Sprintf(" (operation: %s", e.Operation))
		if e.Component != "" {
			b.WriteString(fmt.Sprintf(", component: %s", e.Component))
		}
		b.WriteString(")")
	}

	if e.Cause != nil {
		b.WriteString(fmt.Sprintf(" caused by: %v", e.Cause))
	}

	return b.String()
}

// Unwrap returns the underlying cause error, enabling error chain traversal
// with Go's standard error handling functions like errors.Is and errors.As.
func (e *DBError) Unwrap() error {
	return e.Cause
}

// FormatStack returns a human-readable stack trace for debugging purposes.
func (e *DBError) FormatStack() string {
	if len(e.Stack) == 0 {
		return ""
	}

	var b strings.Builder
	frames := runtime.CallersFrames(e.Stack)

	b.WriteString("Stack trace:\n")
	for {
		f, more := frames.Next()
		b.WriteString(fmt.Sprintf("  %s\n    %s:%d\n",
			f.Function, f.File, f.Line))
		if !more {
			break
		}
	}

	return b.String()
}
