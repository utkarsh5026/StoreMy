package tuple

import (
	"fmt"
	"storemy/pkg/types"
	"time"
)

// Builder provides a fluent interface for constructing tuples
type Builder struct {
	tuple        *Tuple
	currentIndex int
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

// AddInt adds an integer field at the current index
func (b *Builder) AddInt(value int64) *Builder {
	if b.err != nil {
		return b
	}
	if err := b.tuple.SetField(b.currentIndex, types.NewIntField(value)); err != nil {
		b.err = fmt.Errorf("field %d: %w", b.currentIndex, err)
		return b
	}
	b.currentIndex++
	return b
}

// AddString adds a string field at the current index
func (b *Builder) AddString(value string) *Builder {
	if b.err != nil {
		return b
	}
	if err := b.tuple.SetField(b.currentIndex, types.NewStringField(value, types.StringMaxSize)); err != nil {
		b.err = fmt.Errorf("field %d: %w", b.currentIndex, err)
		return b
	}
	b.currentIndex++
	return b
}

// AddFloat adds a float field at the current index
func (b *Builder) AddFloat(value float64) *Builder {
	if b.err != nil {
		return b
	}
	if err := b.tuple.SetField(b.currentIndex, types.NewFloat64Field(value)); err != nil {
		b.err = fmt.Errorf("field %d: %w", b.currentIndex, err)
		return b
	}
	b.currentIndex++
	return b
}

// AddBool adds a boolean field at the current index
func (b *Builder) AddBool(value bool) *Builder {
	if b.err != nil {
		return b
	}
	if err := b.tuple.SetField(b.currentIndex, types.NewBoolField(value)); err != nil {
		b.err = fmt.Errorf("field %d: %w", b.currentIndex, err)
		return b
	}
	b.currentIndex++
	return b
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

	// Verify all fields were set
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
