package dml

import (
	"fmt"
	"storemy/pkg/tuple"
	"strings"
)

// findFieldIndex resolves a join field name to its index in the tuple schema.
// Handles qualified names (table.field) by extracting just the field part.
func findFieldIndex(fieldName string, td *tuple.TupleDescription) (int, error) {
	name := extractFieldName(fieldName)
	idx, err := td.FindFieldIndex(name)
	if err != nil {
		return -1, fmt.Errorf("join field %s not found: %w", fieldName, err)
	}

	return idx, nil
}

// extractFieldName extracts the field name from a qualified name.
// Handles both simple (field) and qualified (table.field) names.
func extractFieldName(qualifiedName string) string {
	parts := strings.Split(qualifiedName, ".")
	return parts[len(parts)-1]
}
