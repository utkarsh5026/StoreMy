package catalog

import (
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

func getIntField(tup *tuple.Tuple, index int) int {
	field, _ := tup.GetField(index)
	return int(field.(*types.IntField).Value)
}

func getStringField(tup *tuple.Tuple, index int) string {
	field, _ := tup.GetField(index)
	return field.String()
}

// fieldToString converts a types.Field to a string representation
func fieldToString(field types.Field) string {
	if field == nil {
		return ""
	}
	return field.String()
}

// stringToField converts a string representation back to a types.Field
// This is a simplified version - it only stores the string value
// The actual field type information would need to be retrieved separately
func stringToField(value string) types.Field {
	if value == "" {
		return nil
	}
	return types.NewStringField(value, len(value))
}
