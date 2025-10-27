package primitives

import (
	"testing"
)

func TestFileID_IsValid(t *testing.T) {
	tests := []struct {
		name     string
		fileID   FileID
		expected bool
	}{
		{"Zero FileID is invalid", FileID(0), false},
		{"Non-zero FileID is valid", FileID(12345), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.fileID.IsValid()
			if result != tt.expected {
				t.Errorf("expected IsValid=%v, got %v", tt.expected, result)
			}
		})
	}
}

func TestFileID_AsUint64(t *testing.T) {
	fileID := FileID(9876543210)
	result := fileID.AsUint64()
	if result != 9876543210 {
		t.Errorf("expected 9876543210, got %d", result)
	}
}

func TestFileID_String(t *testing.T) {
	fileID := FileID(12345)
	result := fileID.String()
	expected := "FileID(12345)"
	if result != expected {
		t.Errorf("expected '%s', got '%s'", expected, result)
	}
}

func TestTableID_ToFileID(t *testing.T) {
	tableID := TableID(12345)
	fileID := tableID.ToFileID()
	if FileID(tableID) != fileID {
		t.Errorf("ToFileID conversion failed")
	}
}

func TestTableID_IsValid(t *testing.T) {
	tests := []struct {
		name     string
		tableID  TableID
		expected bool
	}{
		{"Zero TableID is invalid", TableID(0), false},
		{"Non-zero TableID is valid", TableID(12345), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.tableID.IsValid()
			if result != tt.expected {
				t.Errorf("expected IsValid=%v, got %v", tt.expected, result)
			}
		})
	}
}

func TestTableID_AsUint64(t *testing.T) {
	tableID := TableID(9876543210)
	result := tableID.AsUint64()
	if result != 9876543210 {
		t.Errorf("expected 9876543210, got %d", result)
	}
}

func TestTableID_String(t *testing.T) {
	tableID := TableID(12345)
	result := tableID.String()
	expected := "TableID(12345)"
	if result != expected {
		t.Errorf("expected '%s', got '%s'", expected, result)
	}
}

func TestTableID_AsIndexID(t *testing.T) {
	tableID := TableID(12345)
	indexID := tableID.AsIndexID()

	// Verify they have the same underlying value
	if tableID.AsUint64() != indexID.AsUint64() {
		t.Errorf("AsIndexID conversion should preserve value")
	}
}

func TestIndexID_ToFileID(t *testing.T) {
	indexID := IndexID(12345)
	fileID := indexID.ToFileID()
	if FileID(indexID) != fileID {
		t.Errorf("ToFileID conversion failed")
	}
}

func TestIndexID_IsValid(t *testing.T) {
	tests := []struct {
		name     string
		indexID  IndexID
		expected bool
	}{
		{"Zero IndexID is invalid", IndexID(0), false},
		{"Non-zero IndexID is valid", IndexID(12345), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.indexID.IsValid()
			if result != tt.expected {
				t.Errorf("expected IsValid=%v, got %v", tt.expected, result)
			}
		})
	}
}

func TestIndexID_AsUint64(t *testing.T) {
	indexID := IndexID(9876543210)
	result := indexID.AsUint64()
	if result != 9876543210 {
		t.Errorf("expected 9876543210, got %d", result)
	}
}

func TestIndexID_String(t *testing.T) {
	indexID := IndexID(12345)
	result := indexID.String()
	expected := "IndexID(12345)"
	if result != expected {
		t.Errorf("expected '%s', got '%s'", expected, result)
	}
}

func TestIndexID_AsTableID(t *testing.T) {
	indexID := IndexID(12345)
	tableID := indexID.AsTableID()

	// Verify they have the same underlying value
	if indexID.AsUint64() != tableID.AsUint64() {
		t.Errorf("AsTableID conversion should preserve value")
	}
}

func TestNewFileIDFromUint64(t *testing.T) {
	value := uint64(9876543210)
	fileID := NewFileIDFromUint64(value)
	if fileID.AsUint64() != value {
		t.Errorf("expected %d, got %d", value, fileID.AsUint64())
	}
}

func TestNewTableIDFromUint64(t *testing.T) {
	value := uint64(9876543210)
	tableID := NewTableIDFromUint64(value)
	if tableID.AsUint64() != value {
		t.Errorf("expected %d, got %d", value, tableID.AsUint64())
	}
}

func TestNewIndexIDFromUint64(t *testing.T) {
	value := uint64(9876543210)
	indexID := NewIndexIDFromUint64(value)
	if indexID.AsUint64() != value {
		t.Errorf("expected %d, got %d", value, indexID.AsUint64())
	}
}

func TestNewTableIDFromFileID(t *testing.T) {
	fileID := FileID(12345)
	tableID := NewTableIDFromFileID(fileID)
	if tableID.ToFileID() != fileID {
		t.Errorf("conversion failed")
	}
}

func TestNewIndexIDFromFileID(t *testing.T) {
	fileID := FileID(12345)
	indexID := NewIndexIDFromFileID(fileID)
	if indexID.ToFileID() != fileID {
		t.Errorf("conversion failed")
	}
}

// Test the relationship between FileID, TableID, and IndexID
func TestFileID_TableID_IndexID_Relationship(t *testing.T) {
	// Create a FileID
	fileID := FileID(123456789)

	// Convert to TableID and IndexID
	tableID := NewTableIDFromFileID(fileID)
	indexID := NewIndexIDFromFileID(fileID)

	// They should all have the same underlying value
	if fileID.AsUint64() != tableID.AsUint64() {
		t.Errorf("FileID and TableID should have same value")
	}
	if fileID.AsUint64() != indexID.AsUint64() {
		t.Errorf("FileID and IndexID should have same value")
	}
	if tableID.AsUint64() != indexID.AsUint64() {
		t.Errorf("TableID and IndexID should have same value")
	}

	// Convert back to FileID
	if tableID.ToFileID() != fileID {
		t.Errorf("TableID -> FileID conversion failed")
	}
	if indexID.ToFileID() != fileID {
		t.Errorf("IndexID -> FileID conversion failed")
	}

	// Cross-convert between TableID and IndexID
	convertedIndexID := tableID.AsIndexID()
	if convertedIndexID.AsUint64() != tableID.AsUint64() {
		t.Errorf("TableID -> IndexID conversion failed")
	}

	convertedTableID := indexID.AsTableID()
	if convertedTableID.AsUint64() != indexID.AsUint64() {
		t.Errorf("IndexID -> TableID conversion failed")
	}
}

// Test Filepath integration with FileID types
func TestFilepath_FileID_Integration(t *testing.T) {
	tablePath := Filepath("/data/users.dat")
	indexPath := Filepath("/data/indexes/users_id.idx")

	// Hash as FileID
	tableFileID := tablePath.Hash()
	indexFileID := indexPath.Hash()

	// They should be different (different paths)
	if tableFileID == indexFileID {
		t.Errorf("different paths should have different FileIDs")
	}

	// Hash directly as TableID and IndexID
	tableID := tablePath.HashAsTableID()
	indexID := indexPath.HashAsIndexID()

	// Verify they match the FileID conversions
	if tableID.ToFileID() != tableFileID {
		t.Errorf("HashAsTableID should match Hash conversion")
	}
	if indexID.ToFileID() != indexFileID {
		t.Errorf("HashAsIndexID should match Hash conversion")
	}

	// Same path should always produce same ID
	tableID2 := tablePath.HashAsTableID()
	if tableID != tableID2 {
		t.Errorf("same path should produce same TableID")
	}
}
