package godb

type LimitOp struct {
	child     Operator //required fields for parser
	limitTups Expr
	//add additional fields here, if needed
}

// Limit constructor -- should save how many tuples to return and the child op.
// lim is how many tuples to return and child is the child op.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	return &LimitOp{child: child, limitTups: lim}
}

// Return a TupleDescriptor for this limit
func (l *LimitOp) Descriptor() *TupleDesc {
	return l.child.Descriptor()

}

// Limit operator implementation. This function should iterate over the
// results of the child iterator, and limit the result set to the first
// [lim] tuples it sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	iter, err := l.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	var count int64 = 0
	var max int64 = -1

	return func() (*Tuple, error) {
		t, _ := iter()
		if t == nil {
			return nil, nil
		}
		if max == -1 {
			v, _ := l.limitTups.EvalExpr(t)
			max = v.(IntField).Value
		}
		if count >= max {
			return nil, nil
		}
		count++
		return t, nil
	}, nil
}
