package godb

import "sort"

// TODO: some code goes here
type OrderBy struct {
	orderBy   []Expr // OrderBy should include these two fields (used by parser)
	child     Operator
	ascending []bool
	//add additional fields here
}

// Order by constructor -- should save the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extacted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields
// list should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	return &OrderBy{orderBy: orderByFields, child: child, ascending: ascending}, nil
}

func (o *OrderBy) Descriptor() *TupleDesc {
	return o.child.Descriptor()
}

// BELOW CODE IS TAKEN DIRECTLY FROM SortKeys EXAMPLE ON https://pkg.go.dev/sort
type By func(t1, t2 *Tuple) bool

func (by By) Sort(tuples []*Tuple) {
	ts := &tupleSorter{
		tuples: tuples,
		by:     by, // The Sort method's receiver is the function (closure) that defines the sort order.
	}
	sort.Sort(ts)
}

type tupleSorter struct {
	tuples []*Tuple
	by     func(t1, t2 *Tuple) bool // Closure used in the Less method.
}

func (s *tupleSorter) Len() int {
	return len(s.tuples)
}

func (s *tupleSorter) Swap(i, j int) {
	s.tuples[i], s.tuples[j] = s.tuples[j], s.tuples[i]
}

func (s *tupleSorter) Less(i, j int) bool {
	return s.by(s.tuples[i], s.tuples[j])
}

// Return a function that iterators through the results of the child iterator in
// ascending/descending order, as specified in the construtor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort pacakge and the [sort.Sort] method for this purpose.  To
// use this you will need to implement three methods:  Len, Swap, and Less that
// the sort algorithm will invoke to preduce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at: https://pkg.go.dev/sort
func (o *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	iter, err := o.child.Iterator(tid)
	if err != nil {
		return nil, err
	}

	orderByFields := func(t1, t2 *Tuple) bool {
		for i := 0; i < len(o.orderBy); i++ {
			v1, _ := o.orderBy[i].EvalExpr(t1)
			v2, _ := o.orderBy[i].EvalExpr(t2)
			_type := o.orderBy[i].GetExprType().Ftype
			switch _type {
			case StringType:
				stringv1 := v1.(StringField).Value
				stringv2 := v2.(StringField).Value
				if stringv1 < stringv2 {
					return o.ascending[i]
				} else if stringv1 > stringv2 {
					return !o.ascending[i]
				}
			default:
				intv1 := v1.(IntField).Value
				intv2 := v2.(IntField).Value
				if intv1 < intv2 {
					return o.ascending[i]
				} else if intv1 > intv2 {
					return !o.ascending[i]
				}
			}
		}
		return false
	}

	var tuples []*Tuple
	tupleIndex := 0
	return func() (*Tuple, error) {
		if tuples == nil {
			for {
				t, _ := iter()
				if t == nil {
					By(orderByFields).Sort(tuples)
					break
				}
				tuples = append(tuples, t)
			}
		}

		if tupleIndex >= len(tuples) {
			return nil, nil
		}
		t := tuples[tupleIndex]
		tupleIndex++
		return t, nil
	}, nil
}
