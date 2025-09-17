package types

import "io"

type Field interface {
	Serialize(w io.Writer) error

	Compare(op Predicate, other Field) (bool, error)

	GetType() Type

	String() string

	Equals(other Field) bool

	Hash() (uint32, error)
}
