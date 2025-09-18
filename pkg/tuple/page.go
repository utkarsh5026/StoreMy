package tuple

// PageID interface represents a unique identifier for a page
// This is a placeholder interface that will be implemented by specific page types
type PageID interface {
	// GetTableID returns the table this page belongs to
	GetTableID() int

	// PageNo returns the page number within the table
	PageNo() int

	// Serialize returns a representation of this page ID as integers
	Serialize() []int

	// Equals checks if two page IDs are equal
	Equals(other PageID) bool

	// String returns a string representation
	String() string

	// HashCode returns a hash code for this page ID
	HashCode() int
}
