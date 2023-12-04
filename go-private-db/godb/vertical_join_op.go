package godb

type VerticalJoin[T comparable] struct {
	tables []Operator //operators for the inputs tables of the join

	// The maximum number of records of intermediate state that the join should use
	// (only required for optional exercise)
	maxBufferSize int
}

// Return a TupleDescriptor for this join. The descriptor should be identical for all tables if unmodified since instansiation. Else, return nil.
func (hj *VerticalJoin[T]) Descriptor() *TupleDesc {
	if len(hj.tables) <= 0 {
		return nil
	}

	// verify all tables have the same descriptor
	desc := (hj.tables[0]).Descriptor()
	for i := 0; i < len(hj.tables); i++ {
		if desc != (hj.tables[i]).Descriptor() {
			return nil
		}
	}

	return (hj.tables[0]).Descriptor()
}

// Constructor for a  join of integer expressions
func NewVerticalJoin(tables []Operator, maxBufferSize int) (*VerticalJoin[int64], error) {
	if len(tables) <= 0 {
		return nil, GoDBError{GoDBErrorCode(IllegalOperationError), "Must have at least one table for a vertical join."}
	}

	return &VerticalJoin[int64]{tables, maxBufferSize}, nil
}

// Join operator implementation.  This function should iterate over the results
// of the join. The join should be the result of joining joinOp.left and
// joinOp.right, applying the joinOp.leftField and joinOp.rightField expressions
// to the tuples of the left and right iterators respectively, and joining them
// using an equality predicate.
func (joinOp *VerticalJoin[T]) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	i := 0
	iter, _ := (joinOp.tables[i]).Iterator(tid)

	return func() (*Tuple, error) {
		// try to return from current table
		t1, err := iter()
		if t1 != nil {
			return t1, nil
		} else if err != nil {
			return nil, err
		}

		// current table exhausted, try other tables in list
		var t2 *Tuple = nil
		for i < len(joinOp.tables)-1 && t2 == nil {
			// update iter to reference next table
			i++
			iter, _ = (joinOp.tables[i]).Iterator(tid)

			// try to return tuple from it
			t2, err = iter()
			if t2 != nil {
				return t2, nil
			} else if err != nil {
				return nil, err
			}
		}

		// no more tuples left
		return nil, nil
	}, nil
}
