package execution

import (
	"fmt"
	"storemy/pkg/tuple"
)

type Filter struct {
	base      *BaseIterator
	predicate *Predicate
	child     DbIterator
}

func NewFilter(predicate *Predicate, child DbIterator) (*Filter, error) {
	if predicate == nil {
		return nil, fmt.Errorf("predicate cannot be nil")
	}
	if child == nil {
		return nil, fmt.Errorf("child operator cannot be nil")
	}

	f := &Filter{
		predicate: predicate,
		child:     child,
	}

	f.base = NewBaseIterator(f.readNext)
	return f, nil
}

func (f *Filter) Open() error {
	// Open our child first - data flows from child to us
	if err := f.child.Open(); err != nil {
		return fmt.Errorf("failed to open child operator: %v", err)
	}

	f.base.MarkOpened()
	return nil
}

func (f *Filter) Close() error {
	if f.child != nil {
		f.child.Close()
	}
	return f.base.Close()
}

// GetTupleDesc returns the schema - filtering doesn't change the schema
func (f *Filter) GetTupleDesc() *tuple.TupleDescription {
	return f.child.GetTupleDesc()
}

func (f *Filter) HasNext() (bool, error)      { return f.base.HasNext() }
func (f *Filter) Next() (*tuple.Tuple, error) { return f.base.Next() }

func (f *Filter) readNext() (*tuple.Tuple, error) {
	for {
		hasNext, err := f.child.HasNext()
		if err != nil {
			return nil, fmt.Errorf("error checking if child has next: %v", err)
		}

		if !hasNext {
			return nil, nil // This signals "no more tuples"
		}

		t, err := f.child.Next()
		if err != nil {
			return nil, fmt.Errorf("error getting next tuple from child: %v", err)
		}

		if t == nil {
			return nil, nil
		}

		passes, err := f.predicate.Filter(t)
		if err != nil {
			return nil, fmt.Errorf("predicate evaluation failed: %v", err)
		}

		if passes {
			return t, nil
		}
	}
}

func (f *Filter) Rewind() error {
	// Rewind our child
	if err := f.child.Rewind(); err != nil {
		return err
	}

	// Clear our cache so we start fresh
	f.base.ClearCache()
	return nil
}
