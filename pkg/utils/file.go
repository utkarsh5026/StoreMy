package utils

import (
	"fmt"
	"os"
)

// openFile opens a file with appropriate flags
func OpenFile(filename string) (*os.File, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %v", filename, err)
	}
	return file, nil
}
