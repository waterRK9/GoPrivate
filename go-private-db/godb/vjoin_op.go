package godb

type VerticalJoin[T comparable] struct {
	top, bottom *Operator //operators for the two inputs of the join

	// The maximum number of records of intermediate state that the join should use
	// (only required for optional exercise)
	maxBufferSize int
}

// Return a TupleDescriptor for this join. The descriptor should be identical to the description of both
// the right and left fields. Else, returns nil
func (hj *VerticalJoin[T]) Descriptor() *TupleDesc {
	leftDesc := (*hj.top).Descriptor()
	rightDesc := (*hj.bottom).Descriptor()
	if rightDesc != leftDesc {
		return nil
	}
	return leftDesc
}

// Constructor for a  join of integer expressions
func NewVJoin(top Operator, bottom Operator, maxBufferSize int) (*VerticalJoin[int64], error) {
	return &VerticalJoin[int64]{&top, &bottom, maxBufferSize}, nil
}

// Join operator implementation.  This function should iterate over the results
// of the join. The join should be the result of joining joinOp.left and
// joinOp.right, applying the joinOp.leftField and joinOp.rightField expressions
// to the tuples of the left and right iterators respectively, and joining them
// using an equality predicate.
func (joinOp *VerticalJoin[T]) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	iter1, _ := (*joinOp.top).Iterator(tid)
	iter2, _ := (*joinOp.bottom).Iterator(tid)

	return func() (*Tuple, error) {
		// try to return from table 1
		t1, err := iter1()
		if t1 != nil {
			return t1, nil
		} else if err != nil {
			return nil, err
		}

		// try to return form table 2
		t2, err := iter2()
		if t2 != nil {
			return t2, nil
		} else if err != nil {
			return nil, err
		}

		// no more tuple left
		return nil, nil
	}, nil
}
