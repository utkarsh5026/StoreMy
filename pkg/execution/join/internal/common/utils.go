package common

import (
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
)

// ExtractJoinKey extracts and stringifies the join key from a tuple.
func ExtractJoinKey(t *tuple.Tuple, fieldIndex primitives.ColumnID) (string, error) {
	field, err := t.GetField(fieldIndex)
	if err != nil || field == nil {
		return "", fmt.Errorf("invalid join key at field %d", fieldIndex)
	}
	return field.String(), nil
}

// CombineAndBuffer combines matching tuples and adds to buffer.
func CombineAndBuffer(buffer *JoinMatchBuffer, left, right *tuple.Tuple) error {
	combined, err := tuple.CombineTuples(left, right)
	if err != nil {
		return err
	}
	buffer.Add(combined)
	return nil
}
