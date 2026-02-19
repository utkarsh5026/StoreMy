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
	if p.tuple.TupleDesc.NumFields() != primitives.ColumnID(count) { // #nosec G115
		p.err = fmt.Errorf("invalid tuple: expected %d fields, got %d",
			count, p.tuple.TupleDesc.NumFields())
	}
	return p
}

// readTyped is a generic helper that reads a field of the expected type at the current index and advances.
// F is the concrete field type (e.g. *types.IntField), V is the value type (e.g. int64).
func readTyped[F types.Field, V any](p *Parser, extract func(F) V) V {
	var zero V
	if p.err != nil {
		return zero
	}
	if p.currentIndex >= p.tuple.TupleDesc.NumFields() {
		p.err = fmt.Errorf("read beyond tuple bounds at field %d", p.currentIndex)
		return zero
	}

	field, err := p.tuple.GetField(p.currentIndex)
	if err != nil {
		p.err = fmt.Errorf("field %d: %w", p.currentIndex, err)
		return zero
	}

	typed, ok := field.(F)
	if !ok {
		var expected F
		p.err = fmt.Errorf("field %d: expected %T, got %T", p.currentIndex, expected, field)
		return zero
	}

	p.currentIndex++
	return extract(typed)
}

// ReadInt reads an integer field at the current index and advances (backward compatible)
func (p *Parser) ReadInt() int {
	return readTyped(p, func(f *types.IntField) int { return int(f.Value) })
}

// ReadInt32 reads a 32-bit signed integer field at the current index and advances
func (p *Parser) ReadInt32() int32 {
	return readTyped(p, func(f *types.Int32Field) int32 { return f.Value })
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
	return readTyped(p, func(f *types.Uint32Field) uint32 { return f.Value })
}

// ReadUint64 reads a 64-bit unsigned integer field at the current index and advances
func (p *Parser) ReadUint64() uint64 {
	return readTyped(p, func(f *types.Uint64Field) uint64 { return f.Value })
}

// ReadString reads a string field at the current index and advances
func (p *Parser) ReadString() string {
	return readTyped(p, func(f *types.StringField) string { return f.Value })
}

// ReadBool reads a boolean field at the current index and advances
func (p *Parser) ReadBool() bool {
	return readTyped(p, func(f *types.BoolField) bool { return f.Value })
}

// ReadFloat reads a float field at the current index and advances
func (p *Parser) ReadFloat() float64 {
	return readTyped(p, func(f *types.Float64Field) float64 { return f.Value })
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

// Builder provides a fluent interface for constructing tuples
type Builder struct {
	tuple        *Tuple
	currentIndex primitives.ColumnID
	err          error
}

// NewBuilder creates a new tuple builder with the given schema
func NewBuilder(td *TupleDescription) *Builder {
	return &Builder{
		tuple:        NewTuple(td),
		currentIndex: 0,
		err:          nil,
	}
}

// AddInt adds an integer field at the current index (backward compatible, uses int64)
func (b *Builder) AddInt(value int64) *Builder {
	return b.AddField(types.NewIntField(value))
}

// AddInt32 adds a 32-bit signed integer field at the current index
func (b *Builder) AddInt32(value int32) *Builder {
	return b.AddField(types.NewInt32Field(value))
}

// AddInt64 adds a 64-bit signed integer field at the current index
func (b *Builder) AddInt64(value int64) *Builder {
	return b.AddField(types.NewInt64Field(value))
}

// AddUint32 adds a 32-bit unsigned integer field at the current index
func (b *Builder) AddUint32(value uint32) *Builder {
	return b.AddField(types.NewUint32Field(value))
}

// AddUint64 adds a 64-bit unsigned integer field at the current index
func (b *Builder) AddUint64(value uint64) *Builder {
	return b.AddField(types.NewUint64Field(value))
}

// AddString adds a string field at the current index
func (b *Builder) AddString(value string) *Builder {
	return b.AddField(types.NewStringField(value, types.StringMaxSize))
}

// AddFloat adds a float field at the current index
func (b *Builder) AddFloat(value float64) *Builder {
	return b.AddField(types.NewFloat64Field(value))
}

// AddBool adds a boolean field at the current index
func (b *Builder) AddBool(value bool) *Builder {
	return b.AddField(types.NewBoolField(value))
}

// AddTimestamp adds a Unix timestamp field at the current index
func (b *Builder) AddTimestamp(value time.Time) *Builder {
	return b.AddInt(value.Unix())
}

// AddFloatAsScaledInt adds a float as a scaled integer (e.g., for clustering factor)
// scale determines precision: 1000000 stores 6 decimal places
func (b *Builder) AddFloatAsScaledInt(value float64, scale int64) *Builder {
	return b.AddInt(int64(value * float64(scale)))
}

// AddField adds a generic field at the current index
func (b *Builder) AddField(field types.Field) *Builder {
	if b.err != nil {
		return b
	}
	if err := b.tuple.SetField(b.currentIndex, field); err != nil {
		b.err = fmt.Errorf("field %d: %w", b.currentIndex, err)
		return b
	}
	b.currentIndex++
	return b
}

// Build returns the constructed tuple or an error if any operation failed
func (b *Builder) Build() (*Tuple, error) {
	if b.err != nil {
		return nil, b.err
	}

	if b.currentIndex != b.tuple.TupleDesc.NumFields() {
		return nil, fmt.Errorf("incomplete tuple: expected %d fields, got %d",
			b.tuple.TupleDesc.NumFields(), b.currentIndex)
	}

	return b.tuple, nil
}

// MustBuild returns the tuple or panics on error (use only when errors are impossible)
func (b *Builder) MustBuild() *Tuple {
	t, err := b.Build()
	if err != nil {
		panic(fmt.Sprintf("tuple builder error: %v", err))
	}
	return t
}

type Iterator struct {
	tuples    []*Tuple
	tupleDesc *TupleDescription
	index     int
	opened    bool
}

// NewIterator creates a new iterator for the given slice of tuples
func NewIterator(tuples []*Tuple) *Iterator {
	return &Iterator{
		tuples: tuples,
		index:  -1,
		opened: false,
	}
}

// NewIteratorWithDesc creates a new iterator with an associated TupleDescription.
func NewIteratorWithDesc(tuples []*Tuple, desc *TupleDescription) *Iterator {
	return &Iterator{
		tuples:    tuples,
		tupleDesc: desc,
		index:     -1,
		opened:    false,
	}
}

// Open initializes the iterator for use. Must be called before calling HasNext or Next.
func (it *Iterator) Open() error {
	it.opened = true
	it.index = -1
	return nil
}

// Close releases any resources held by the iterator. After calling Close, the iterator should not be used.
func (it *Iterator) Close() error {
	it.opened = false
	return nil
}

// HasNext checks if there are more tuples to iterate over. Returns an error if the iterator is not opened or not initialized.
func (it *Iterator) HasNext() (bool, error) {
	if !it.opened {
		return false, fmt.Errorf("iterator not opened")
	}
	if it.tuples == nil {
		return false, fmt.Errorf("iterator not initialized with tuples")
	}
	return it.index+1 < len(it.tuples), nil
}

// Next retrieves the next tuple from the iterator. Returns an error if the iterator is not opened or if there are no more tuples.
func (it *Iterator) Next() (*Tuple, error) {
	hasNext, err := it.HasNext()
	if err != nil {
		return nil, err
	}
	if !hasNext {
		return nil, nil
	}

	it.index++
	return it.tuples[it.index], nil
}

// Rewind resets the iterator to the beginning of the tuple slice. Returns an error if the iterator is not opened.
func (it *Iterator) Rewind() error {
	if !it.opened {
		return fmt.Errorf("iterator not opened")
	}
	it.index = -1
	return nil
}

// GetTupleDesc returns the TupleDescription associated with this iterator, if any.
func (it *Iterator) GetTupleDesc() *TupleDescription {
	return it.tupleDesc
}
