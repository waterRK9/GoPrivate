package godb

type Project struct {
	selectFields []Expr // required fields for parser
	outputNames  []string
	child        Operator
	distinct     bool
	//add additional fields here
	// TODO: some code goes here
}

// Project constructor -- should save the list of selected field, child, and the child op.
// Here, selectFields is a list of expressions that represents the fields to be selected,
// outputNames are names by which the selected fields are named (should be same length as
// selectFields; throws error if not), distinct is for noting whether the projection reports
// only distinct results, and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) {
	return &Project{selectFields: selectFields, outputNames: outputNames, child: child, distinct: distinct}, nil
}

// Return a TupleDescriptor for this projection. The returned descriptor should contain
// fields for each field in the constructor selectFields list with outputNames
// as specified in the constructor.
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() *TupleDesc {
	fts := make([]FieldType, len(p.outputNames))
	for i := 0; i < len(p.outputNames); i++ {
		t := p.selectFields[i].GetExprType().Ftype
		name := p.outputNames[i]
		ft := FieldType{name, "", t}
		fts[i] = ft
	}
	td := TupleDesc{}
	td.Fields = fts
	return &td

}

// Project operator implementation.  This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed.
// To implement this you will need to record in some data structure with the
// distinct tuples seen so far.  Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	iter, _ := p.child.Iterator(tid)
	seen := make(map[any]bool)

	return func() (*Tuple, error) {
		for {
			tup, _ := iter()
			if tup == nil {
				return nil, nil
			}
			fs := make([]DBValue, len(p.selectFields))
			for i := 0; i < len(p.selectFields); i++ {
				f, err := p.selectFields[i].EvalExpr(tup)
				if err != nil {
					return nil, err
				}
				fs[i] = f
			}
			newTup := &Tuple{*p.Descriptor(), fs, nil}
			key := newTup.tupleKey()
			if !p.distinct || seen[key] != true {
				seen[key] = true
				return &Tuple{*p.Descriptor(), fs, nil}, nil
			}
		}
	}, nil
}
