package index

import (
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/types"
)

func OpenFile(indexType IndexType, filePath primitives.Filepath, keyType types.Type) (page.DbFile, error) {
	switch indexType {
	case HashIndex:
		return NewHashFile(filePath, keyType, DefaultBuckets)

	case BTreeIndex:
		return NewBTreeFile(filePath, keyType)

	default:
		return nil, fmt.Errorf("unsupported index type: %s", indexType)
	}
}
