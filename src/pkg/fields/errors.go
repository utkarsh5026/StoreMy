package fields

import "errors"

var UnSupportedPredicate = errors.New("unsupported predicate")

var TypeMismatch = errors.New("type mismatch")

var InvalidDataType = errors.New("invalid data type")
