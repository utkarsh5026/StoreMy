package operations

import (
	"errors"
	"fmt"
	"storemy/pkg/catalog/catalogio"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/tuple"
)

type (
	TxContext = *transaction.TransactionContext
)

var (
	ErrSuccess = errors.New("success")
)

// BaseOperations provides common CRUD operations for catalog tables.
// It uses Go generics to provide type-safe operations across different catalog entity types.
//
// Type parameter T represents the parsed entity type (e.g., *systemtable.TableStatistics).
//
// This eliminates repetitive iterate-parse-process patterns across all operation structs.
type BaseOperations[T any] struct {
	reader  catalogio.CatalogReader
	writer  catalogio.CatalogWriter
	tableID int
	parser  func(*tuple.Tuple) (T, error)
	creator func(T) *tuple.Tuple
}

// NewBaseOperations creates a new BaseOperations instance.
//
// Parameters:
//   - reader: CatalogReader for reading catalog data
//   - writer: CatalogWriter for writing catalog data
//   - tableID: ID of the catalog table this base operates on
//   - parser: Function to parse a tuple into type T
//   - creator: Function to create a tuple from type T
func NewBaseOperations[T any](
	reader catalogio.CatalogReader,
	writer catalogio.CatalogWriter,
	tableID int,
	parser func(*tuple.Tuple) (T, error),
	creator func(T) *tuple.Tuple,
) *BaseOperations[T] {
	return &BaseOperations[T]{
		reader:  reader,
		writer:  writer,
		tableID: tableID,
		parser:  parser,
		creator: creator,
	}
}

// Iterate applies a processing function to each entity in the catalog table.
// The processFunc can return ErrSuccess to stop iteration early (used for "find first" patterns).
//
// Parameters:
//   - tx: Transaction context
//   - processFunc: Function to process each parsed entity. Return ErrSuccess for early termination.
//
// Returns error if iteration fails or if processFunc returns a non-ErrSuccess error.
func (bo *BaseOperations[T]) Iterate(tx TxContext, processFunc func(T) error) error {
	return bo.reader.IterateTable(bo.tableID, tx, func(tup *tuple.Tuple) error {
		entity, err := bo.parser(tup)
		if err != nil {
			return fmt.Errorf("failed to parse tuple: %w", err)
		}

		if err = processFunc(entity); err != nil {
			if errors.Is(err, ErrSuccess) {
				return err
			}
			return fmt.Errorf("failed to process entity: %w", err)
		}
		return nil
	})
}

// FindOne finds the first entity matching the predicate.
// Uses early termination for efficiency.
//
// Parameters:
//   - tx: Transaction context
//   - predicate: Function that returns true when desired entity is found
//
// Returns the matching entity or error if not found or iteration fails.
func (bo *BaseOperations[T]) FindOne(tx TxContext, predicate func(T) bool) (T, error) {
	var result T
	var found bool

	err := bo.Iterate(tx, func(entity T) error {
		if predicate(entity) {
			result = entity
			found = true
			return ErrSuccess
		}
		return nil
	})

	if errors.Is(err, ErrSuccess) {
		return result, nil
	}

	if err != nil {
		return result, err
	}

	if !found {
		return result, fmt.Errorf("entity not found")
	}

	return result, nil
}

// FindAll finds all entities matching the predicate.
//
// Parameters:
//   - tx: Transaction context
//   - predicate: Function that returns true for entities to include in results
//
// Returns slice of matching entities or error if iteration fails.
func (bo *BaseOperations[T]) FindAll(tx TxContext, predicate func(T) bool) ([]T, error) {
	var results []T

	err := bo.Iterate(tx, func(entity T) error {
		if predicate(entity) {
			results = append(results, entity)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return results, nil
}

// DeleteBy deletes all entities matching the predicate.
// Uses a two-phase approach: collect matching tuples, then delete them.
//
// Parameters:
//   - tx: Transaction context
//   - predicate: Function that returns true for entities to delete
//
// Returns error if iteration or deletion fails.
func (bo *BaseOperations[T]) DeleteBy(tx TxContext, predicate func(T) bool) error {
	var tuplesToDelete []*tuple.Tuple

	err := bo.Iterate(tx, func(entity T) error {
		if predicate(entity) {
			tuplesToDelete = append(tuplesToDelete, bo.creator(entity))
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to collect tuples for deletion: %w", err)
	}

	for _, tup := range tuplesToDelete {
		if err := bo.writer.DeleteRow(bo.tableID, tx, tup); err != nil {
			return fmt.Errorf("failed to delete tuple: %w", err)
		}
	}

	return nil
}

// Insert inserts a new entity into the catalog table.
//
// Parameters:
//   - tx: Transaction context
//   - entity: Entity to insert
//
// Returns error if insertion fails.
func (bo *BaseOperations[T]) Insert(tx TxContext, entity T) error {
	tup := bo.creator(entity)
	if err := bo.writer.InsertRow(bo.tableID, tx, tup); err != nil {
		return fmt.Errorf("failed to insert entity: %w", err)
	}
	return nil
}

// Upsert updates an entity if it exists (matching predicate), otherwise inserts it.
// Uses delete-then-insert pattern for MVCC compatibility.
//
// Parameters:
//   - tx: Transaction context
//   - predicate: Function to find existing entity to update
//   - entity: Entity to insert or update
//
// Returns error if operation fails.
func (bo *BaseOperations[T]) Upsert(tx TxContext, predicate func(T) bool, entity T) error {
	_, err := bo.FindOne(tx, predicate)
	if err == nil {
		if err := bo.DeleteBy(tx, predicate); err != nil {
			return fmt.Errorf("failed to delete old entity: %w", err)
		}
	}
	return bo.Insert(tx, entity)
}

// TableID returns the catalog table ID this BaseOperations instance operates on.
func (bo *BaseOperations[T]) TableID() int {
	return bo.tableID
}
