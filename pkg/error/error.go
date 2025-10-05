package error

import (
	"fmt"
	"runtime"
	"strings"
)

// ErrorCategory helps classify errors for handling
type ErrorCategory int

const (
	// User errors - fixable by changing input
	ErrCategoryUser ErrorCategory = iota
	// Transient errors - retry might succeed
	ErrCategoryTransient
	// System errors - need administrator intervention
	ErrCategorySystem
	// Data errors - possible corruption
	ErrCategoryData
	// Concurrency errors - transaction conflicts
	ErrCategoryConcurrency
)

type DBError struct {
	Code     string
	Category ErrorCategory
	Message  string
	Detail   string // Additional context
	Hint     string // Suggestion for fixing

	// Context about where error occurred
	Operation string // e.g., "InsertTuple", "SeqScan"
	Component string // e.g., "PageStore", "LockManager"

	// Error chain
	Cause error

	// Stack trace (only in debug mode)
	Stack []uintptr
}

func New(code string, category ErrorCategory, message string) *DBError {
	err := &DBError{
		Code:     code,
		Category: category,
		Message:  message,
		Stack:    captureStack(),
	}
	return err
}

// Wrap wraps an existing error with database context
func Wrap(err error, code string, operation, component string) *DBError {
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

func captureStack() []uintptr {
	const depth = 32
	var pcs [depth]uintptr
	n := runtime.Callers(3, pcs[:])
	return pcs[0:n]
}

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

func (e *DBError) Unwrap() error {
	return e.Cause
}

// FormatStack returns formatted stack trace
func (e *DBError) FormatStack() string {
	if len(e.Stack) == 0 {
		return ""
	}

	var b strings.Builder
	frames := runtime.CallersFrames(e.Stack)

	b.WriteString("Stack trace:\n")
	for {
		frame, more := frames.Next()
		b.WriteString(fmt.Sprintf("  %s\n    %s:%d\n",
			frame.Function, frame.File, frame.Line))
		if !more {
			break
		}
	}

	return b.String()
}
