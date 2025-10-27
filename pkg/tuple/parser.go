package tuple

import (
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/types"
	"time"
)

// Parser provides a fluent interface for parsing tuples sequentially
// It mirrors the Builder pattern but for reading instead of writing
type Parser struct {
	tuple        *Tuple
	currentIndex primitives.ColumnID
	err          error
}

// NewParser creates a new tuple parser for the given tuple
func NewParser(t *Tuple) *Parser {
	return &Parser{
		tuple:        t,
		currentIndex: 0,
		err:          nil,
	}
}

// ExpectFields validates that the tuple has the expected number of fields
// This should be called immediately after NewParser for validation
func (p *Parser) ExpectFields(count int) *Parser {
	if p.err != nil {
		return p
	}
	if p.tuple.TupleDesc.NumFields() != primitives.ColumnID(count) {
		p.err = fmt.Errorf("invalid tuple: expected %d fields, got %d",
			count, p.tuple.TupleDesc.NumFields())
	}
	return p
}

// ReadInt reads an integer field at the current index and advances (backward compatible)
func (p *Parser) ReadInt() int {
	if p.err != nil {
		return 0
	}
	if p.currentIndex >= p.tuple.TupleDesc.NumFields() {
		p.err = fmt.Errorf("read beyond tuple bounds at field %d", p.currentIndex)
		return 0
	}

	field, err := p.tuple.GetField(p.currentIndex)
	if err != nil {
		p.err = fmt.Errorf("field %d: %w", p.currentIndex, err)
		return 0
	}

	intField, ok := field.(*types.IntField)
	if !ok {
		p.err = fmt.Errorf("field %d: expected IntField, got %T", p.currentIndex, field)
		return 0
	}

	p.currentIndex++
	return int(intField.Value)
}

// ReadInt32 reads a 32-bit signed integer field at the current index and advances
func (p *Parser) ReadInt32() int32 {
	if p.err != nil {
		return 0
	}
	if p.currentIndex >= p.tuple.TupleDesc.NumFields() {
		p.err = fmt.Errorf("read beyond tuple bounds at field %d", p.currentIndex)
		return 0
	}

	field, err := p.tuple.GetField(p.currentIndex)
	if err != nil {
		p.err = fmt.Errorf("field %d: %w", p.currentIndex, err)
		return 0
	}

	int32Field, ok := field.(*types.Int32Field)
	if !ok {
		p.err = fmt.Errorf("field %d: expected Int32Field, got %T", p.currentIndex, field)
		return 0
	}

	p.currentIndex++
	return int32Field.Value
}

// ReadInt64 reads a 64-bit signed integer field at the current index and advances
func (p *Parser) ReadInt64() int64 {
	if p.err != nil {
		return 0
	}
	if p.currentIndex >= p.tuple.TupleDesc.NumFields() {
		p.err = fmt.Errorf("read beyond tuple bounds at field %d", p.currentIndex)
		return 0
	}

	field, err := p.tuple.GetField(p.currentIndex)
	if err != nil {
		p.err = fmt.Errorf("field %d: %w", p.currentIndex, err)
		return 0
	}

	// Try Int64Field first, fall back to IntField for backward compatibility
	if int64Field, ok := field.(*types.Int64Field); ok {
		p.currentIndex++
		return int64Field.Value
	}

	intField, ok := field.(*types.IntField)
	if !ok {
		p.err = fmt.Errorf("field %d: expected Int64Field or IntField, got %T", p.currentIndex, field)
		return 0
	}

	p.currentIndex++
	return intField.Value
}

// ReadUint32 reads a 32-bit unsigned integer field at the current index and advances
func (p *Parser) ReadUint32() uint32 {
	if p.err != nil {
		return 0
	}
	if p.currentIndex >= p.tuple.TupleDesc.NumFields() {
		p.err = fmt.Errorf("read beyond tuple bounds at field %d", p.currentIndex)
		return 0
	}

	field, err := p.tuple.GetField(p.currentIndex)
	if err != nil {
		p.err = fmt.Errorf("field %d: %w", p.currentIndex, err)
		return 0
	}

	uint32Field, ok := field.(*types.Uint32Field)
	if !ok {
		p.err = fmt.Errorf("field %d: expected Uint32Field, got %T", p.currentIndex, field)
		return 0
	}

	p.currentIndex++
	return uint32Field.Value
}

// ReadUint64 reads a 64-bit unsigned integer field at the current index and advances
func (p *Parser) ReadUint64() uint64 {
	if p.err != nil {
		return 0
	}
	if p.currentIndex >= p.tuple.TupleDesc.NumFields() {
		p.err = fmt.Errorf("read beyond tuple bounds at field %d", p.currentIndex)
		return 0
	}

	field, err := p.tuple.GetField(p.currentIndex)
	if err != nil {
		p.err = fmt.Errorf("field %d: %w", p.currentIndex, err)
		return 0
	}

	uint64Field, ok := field.(*types.Uint64Field)
	if !ok {
		p.err = fmt.Errorf("field %d: expected Uint64Field, got %T", p.currentIndex, field)
		return 0
	}

	p.currentIndex++
	return uint64Field.Value
}

// ReadString reads a string field at the current index and advances
func (p *Parser) ReadString() string {
	if p.err != nil {
		return ""
	}
	if p.currentIndex >= p.tuple.TupleDesc.NumFields() {
		p.err = fmt.Errorf("read beyond tuple bounds at field %d", p.currentIndex)
		return ""
	}

	field, err := p.tuple.GetField(p.currentIndex)
	if err != nil {
		p.err = fmt.Errorf("field %d: %w", p.currentIndex, err)
		return ""
	}

	strField, ok := field.(*types.StringField)
	if !ok {
		p.err = fmt.Errorf("field %d: expected StringField, got %T", p.currentIndex, field)
		return ""
	}

	p.currentIndex++
	return strField.Value
}

// ReadBool reads a boolean field at the current index and advances
func (p *Parser) ReadBool() bool {
	if p.err != nil {
		return false
	}
	if p.currentIndex >= p.tuple.TupleDesc.NumFields() {
		p.err = fmt.Errorf("read beyond tuple bounds at field %d", p.currentIndex)
		return false
	}

	field, err := p.tuple.GetField(p.currentIndex)
	if err != nil {
		p.err = fmt.Errorf("field %d: %w", p.currentIndex, err)
		return false
	}

	boolField, ok := field.(*types.BoolField)
	if !ok {
		p.err = fmt.Errorf("field %d: expected BoolField, got %T", p.currentIndex, field)
		return false
	}

	p.currentIndex++
	return boolField.Value
}

// ReadFloat reads a float field at the current index and advances
func (p *Parser) ReadFloat() float64 {
	if p.err != nil {
		return 0.0
	}
	if p.currentIndex >= p.tuple.TupleDesc.NumFields() {
		p.err = fmt.Errorf("read beyond tuple bounds at field %d", p.currentIndex)
		return 0.0
	}

	field, err := p.tuple.GetField(p.currentIndex)
	if err != nil {
		p.err = fmt.Errorf("field %d: %w", p.currentIndex, err)
		return 0.0
	}

	floatField, ok := field.(*types.Float64Field)
	if !ok {
		p.err = fmt.Errorf("field %d: expected Float64Field, got %T", p.currentIndex, field)
		return 0.0
	}

	p.currentIndex++
	return floatField.Value
}

// ReadTimestamp reads a Unix timestamp field and returns it as time.Time
func (p *Parser) ReadTimestamp() time.Time {
	unixTime := p.ReadInt64()
	if p.err != nil {
		return time.Time{}
	}
	return time.Unix(unixTime, 0)
}

// ReadScaledFloat reads an integer field and converts it back to float using the given scale
// This is the inverse of Builder.AddFloatAsScaledInt
func (p *Parser) ReadScaledFloat(scale int64) float64 {
	scaledInt := p.ReadInt64()
	if p.err != nil {
		return 0.0
	}
	return float64(scaledInt) / float64(scale)
}

// ReadField reads a generic field at the current index and advances
func (p *Parser) ReadField() types.Field {
	if p.err != nil {
		return nil
	}
	if p.currentIndex >= p.tuple.TupleDesc.NumFields() {
		p.err = fmt.Errorf("read beyond tuple bounds at field %d", p.currentIndex)
		return nil
	}

	field, err := p.tuple.GetField(p.currentIndex)
	if err != nil {
		p.err = fmt.Errorf("field %d: %w", p.currentIndex, err)
		return nil
	}

	p.currentIndex++
	return field
}

// Error returns any accumulated parsing error
func (p *Parser) Error() error {
	return p.err
}

// Done checks that all fields have been read and returns any error
// Use this at the end of parsing to ensure the entire tuple was consumed
func (p *Parser) Done() error {
	if p.err != nil {
		return p.err
	}
	if p.currentIndex != p.tuple.TupleDesc.NumFields() {
		return fmt.Errorf("incomplete parsing: read %d of %d fields",
			p.currentIndex, p.tuple.TupleDesc.NumFields())
	}
	return nil
}

// MustDone panics if there's an error (use only when errors are impossible)
func (p *Parser) MustDone() {
	if err := p.Done(); err != nil {
		panic(fmt.Sprintf("tuple parser error: %v", err))
	}
}
