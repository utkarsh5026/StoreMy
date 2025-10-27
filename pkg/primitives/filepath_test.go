package primitives

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFilepath_String(t *testing.T) {
	path := Filepath("/data/users.dat")
	if path.String() != "/data/users.dat" {
		t.Errorf("expected '/data/users.dat', got '%s'", path.String())
	}
}

func TestFilepath_Join(t *testing.T) {
	base := Filepath("/data")
	result := base.Join("tables", "users.dat")
	expected := filepath.Join("/data", "tables", "users.dat")
	if result.String() != expected {
		t.Errorf("expected '%s', got '%s'", expected, result.String())
	}
}

func TestFilepath_Base(t *testing.T) {
	path := Filepath("/data/indexes/users_id.idx")
	base := path.Base()
	if base != "users_id.idx" {
		t.Errorf("expected 'users_id.idx', got '%s'", base)
	}
}

func TestFilepath_Dir(t *testing.T) {
	path := Filepath("/data/indexes/users_id.idx")
	dir := path.Dir()
	expected := filepath.Dir("/data/indexes/users_id.idx")
	if dir != expected {
		t.Errorf("expected '%s', got '%s'", expected, dir)
	}
}

func TestFilepath_Ext(t *testing.T) {
	tests := []struct {
		path     Filepath
		expected string
	}{
		{Filepath("/data/users.dat"), ".dat"},
		{Filepath("/data/index.idx"), ".idx"},
		{Filepath("/data/file"), ""},
	}

	for _, tt := range tests {
		ext := tt.path.Ext()
		if ext != tt.expected {
			t.Errorf("for path '%s', expected ext '%s', got '%s'", tt.path, tt.expected, ext)
		}
	}
}

func TestFilepath_WithExt(t *testing.T) {
	tests := []struct {
		path     Filepath
		newExt   string
		expected string
	}{
		{Filepath("/data/users.dat"), ".bak", "/data/users.bak"},
		{Filepath("/data/users.dat"), "backup", "/data/users.backup"},
		{Filepath("/data/file"), ".log", "/data/file.log"},
	}

	for _, tt := range tests {
		result := tt.path.WithExt(tt.newExt)
		if result.String() != tt.expected {
			t.Errorf("for path '%s' with ext '%s', expected '%s', got '%s'",
				tt.path, tt.newExt, tt.expected, result.String())
		}
	}
}

func TestFilepath_IsEmpty(t *testing.T) {
	tests := []struct {
		path     Filepath
		expected bool
	}{
		{Filepath(""), true},
		{Filepath("/data/users.dat"), false},
	}

	for _, tt := range tests {
		result := tt.path.IsEmpty()
		if result != tt.expected {
			t.Errorf("for path '%s', expected IsEmpty=%v, got %v", tt.path, tt.expected, result)
		}
	}
}

func TestFilepath_IsAbs(t *testing.T) {
	// Create platform-specific absolute path
	tmpDir := t.TempDir() // This will be an absolute path
	absPath := Filepath(tmpDir)
	relPath := Filepath("data/users.dat")

	// Test absolute path
	if !absPath.IsAbs() {
		t.Errorf("path '%s' should be absolute", absPath)
	}

	// Test relative path
	if relPath.IsAbs() {
		t.Errorf("path '%s' should be relative", relPath)
	}
}

func TestFilepath_Clean(t *testing.T) {
	tests := []struct {
		path     Filepath
		expected string
	}{
		{Filepath("/data/../data/./users.dat"), filepath.Clean("/data/../data/./users.dat")},
		{Filepath("/data//tables///users.dat"), filepath.Clean("/data//tables///users.dat")},
	}

	for _, tt := range tests {
		result := tt.path.Clean()
		if result.String() != tt.expected {
			t.Errorf("for path '%s', expected Clean='%s', got '%s'", tt.path, tt.expected, result.String())
		}
	}
}

func TestFilepath_Hash(t *testing.T) {
	path1 := Filepath("/data/users.dat")
	path2 := Filepath("/data/users.dat")
	path3 := Filepath("/data/products.dat")

	hash1 := path1.Hash()
	hash2 := path2.Hash()
	hash3 := path3.Hash()

	// Same paths should have same hash
	if hash1 != hash2 {
		t.Errorf("expected same hash for identical paths")
	}

	// Different paths should (likely) have different hashes
	if hash1 == hash3 {
		t.Errorf("expected different hashes for different paths")
	}
}

func TestFilepath_ExistsAndRemove(t *testing.T) {
	// Create a temporary file
	tmpDir := t.TempDir()
	testPath := Filepath(filepath.Join(tmpDir, "test_file.dat"))

	// File should not exist initially
	if testPath.Exists() {
		t.Errorf("file should not exist initially")
	}

	// Create the file
	f, err := os.Create(testPath.String())
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	f.Close()

	// File should now exist
	if !testPath.Exists() {
		t.Errorf("file should exist after creation")
	}

	// Remove the file
	err = testPath.Remove()
	if err != nil {
		t.Errorf("Remove() failed: %v", err)
	}

	// File should not exist after removal
	if testPath.Exists() {
		t.Errorf("file should not exist after removal")
	}

	// Remove again should be idempotent (no error)
	err = testPath.Remove()
	if err != nil {
		t.Errorf("second Remove() should not error: %v", err)
	}
}

func TestFilepath_MkdirAll(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := Filepath(filepath.Join(tmpDir, "nested", "directories", "file.dat"))

	// Create parent directories
	err := testPath.MkdirAll(0755)
	if err != nil {
		t.Fatalf("MkdirAll() failed: %v", err)
	}

	// Verify directory was created
	dirPath := testPath.Dir()
	info, err := os.Stat(dirPath)
	if err != nil {
		t.Errorf("directory should exist: %v", err)
	}
	if !info.IsDir() {
		t.Errorf("path should be a directory")
	}
}

func TestFilepath_Stat(t *testing.T) {
	// Create a temporary file
	tmpDir := t.TempDir()
	testPath := Filepath(filepath.Join(tmpDir, "test_file.dat"))

	// Stat on non-existent file should error
	_, err := testPath.Stat()
	if err == nil {
		t.Errorf("Stat() should error on non-existent file")
	}

	// Create the file with some content
	content := []byte("test content")
	err = os.WriteFile(testPath.String(), content, 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Stat should now work
	info, err := testPath.Stat()
	if err != nil {
		t.Errorf("Stat() failed: %v", err)
	}

	if info.Size() != int64(len(content)) {
		t.Errorf("expected size %d, got %d", len(content), info.Size())
	}
}
