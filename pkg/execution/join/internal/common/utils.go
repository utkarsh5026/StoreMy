package common

import (
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
)

// ExtractJoinKey extracts and hashes the join key from a tuple.
func ExtractJoinKey(t *tuple.Tuple, fieldIndex primitives.ColumnID) (primitives.HashCode, error) {
	field, err := t.GetField(fieldIndex)
	if err != nil || field == nil {
		return 0, fmt.Errorf("invalid join key at field %d", fieldIndex)
	}
	return field.Hash()
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
