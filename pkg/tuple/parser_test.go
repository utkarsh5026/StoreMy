package tuple

import (
	"storemy/pkg/types"
	"testing"
	"time"
)

func TestParser_BasicTypes(t *testing.T) {
	// Create a tuple with various field types
	td, err := NewTupleDesc(
		[]types.Type{types.IntType, types.StringType, types.BoolType, types.FloatType},
		[]string{"id", "name", "active", "score"},
	)
	if err != nil {
		t.Fatalf("failed to create tuple desc: %v", err)
	}

	tuple := NewBuilder(td).
		AddInt(42).
		AddString("test").
		AddBool(true).
		AddFloat(3.14).
		MustBuild()

	// Parse it back
	p := NewParser(tuple).ExpectFields(4)

	id := p.ReadInt()
	name := p.ReadString()
	active := p.ReadBool()
	score := p.ReadFloat()

	if err := p.Error(); err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}

	// Verify values
	if id != 42 {
		t.Errorf("expected id=42, got %d", id)
	}
	if name != "test" {
		t.Errorf("expected name='test', got '%s'", name)
	}
	if !active {
		t.Errorf("expected active=true, got %v", active)
	}
	if score != 3.14 {
		t.Errorf("expected score=3.14, got %f", score)
	}
}

func TestParser_FieldCountValidation(t *testing.T) {
	td, _ := NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	tuple := NewBuilder(td).
		AddInt(1).
		AddString("foo").
		MustBuild()

	// Expect wrong number of fields
	p := NewParser(tuple).ExpectFields(3)

	if err := p.Error(); err == nil {
		t.Error("expected field count error, got nil")
	}
}

func TestParser_TypeMismatch(t *testing.T) {
	td, _ := NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	tuple := NewBuilder(td).
		AddInt(1).
		AddString("foo").
		MustBuild()

	// Try to read string as int
	p := NewParser(tuple).ExpectFields(2)
	_ = p.ReadInt()  // OK
	_ = p.ReadInt()  // Wrong - should be string

	if err := p.Error(); err == nil {
		t.Error("expected type mismatch error, got nil")
	}
}

func TestParser_Timestamp(t *testing.T) {
	td, _ := NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"timestamp"},
	)

	now := time.Now().Truncate(time.Second)
	tuple := NewBuilder(td).
		AddTimestamp(now).
		MustBuild()

	p := NewParser(tuple).ExpectFields(1)
	parsed := p.ReadTimestamp()

	if err := p.Error(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !parsed.Equal(now) {
		t.Errorf("expected %v, got %v", now, parsed)
	}
}

func TestParser_ScaledFloat(t *testing.T) {
	td, _ := NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"clustering_factor"},
	)

	originalValue := 0.123456
	tuple := NewBuilder(td).
		AddFloatAsScaledInt(originalValue, 1000000).
		MustBuild()

	p := NewParser(tuple).ExpectFields(1)
	parsed := p.ReadScaledFloat(1000000)

	if err := p.Error(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Allow small floating point error
	diff := parsed - originalValue
	if diff < -0.000001 || diff > 0.000001 {
		t.Errorf("expected %f, got %f (diff: %f)", originalValue, parsed, diff)
	}
}

func TestParser_ReadBeyondBounds(t *testing.T) {
	td, _ := NewTupleDesc(
		[]types.Type{types.IntType},
		[]string{"id"},
	)

	tuple := NewBuilder(td).
		AddInt(42).
		MustBuild()

	p := NewParser(tuple).ExpectFields(1)
	_ = p.ReadInt()  // OK
	_ = p.ReadInt()  // Beyond bounds

	if err := p.Error(); err == nil {
		t.Error("expected read beyond bounds error, got nil")
	}
}

func TestParser_Done(t *testing.T) {
	td, _ := NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)

	tuple := NewBuilder(td).
		AddInt(1).
		AddString("foo").
		MustBuild()

	// Test incomplete parsing
	p := NewParser(tuple).ExpectFields(2)
	_ = p.ReadInt()  // Only read 1 field

	if err := p.Done(); err == nil {
		t.Error("expected incomplete parsing error, got nil")
	}

	// Test complete parsing
	p2 := NewParser(tuple).ExpectFields(2)
	_ = p2.ReadInt()
	_ = p2.ReadString()

	if err := p2.Done(); err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}
