package godb

import (
	"github.com/getamis/alice/crypto/homo"
	"golang.org/x/exp/constraints"
)

type Number interface {
	constraints.Integer | constraints.Float
}

// interface for an aggregation state
type AggState interface {

	// Initializes an aggregation state. Is supplied with an alias,
	// an expr to evaluate an input tuple into a DBValue, and a getter
	// to extract from the DBValue its int or string field's value.
	Init(alias string, expr Expr, getter func(DBValue) any) error

	// Makes an copy of the aggregation state.
	Copy() AggState

	// Adds an tuple to the aggregation state.
	AddTuple(*Tuple)

	// Returns the final result of the aggregation as a tuple.
	Finalize() *Tuple

	// Gets the tuple description of the tuple that Finalize() returns.
	GetTupleDesc() *TupleDesc
}

type EncryptedAggState interface {

	// Initializes an aggregation state. Is supplied with an alias,
	// an expr to evaluate an input tuple into a DBValue, and a getter
	// to extract from the DBValue its int or string field's value.
	Init(alias string, expr Expr, getter func(DBValue) any, publicKey homo.Pubkey) error

	// Makes an copy of the aggregation state.
	Copy() EncryptedAggState

	// Adds an tuple to the aggregation state.
	AddTuple(*Tuple)

	// Returns the final result of the aggregation as a tuple.
	Finalize() *Tuple

	// Gets the tuple description of the tuple that Finalize() returns.
	GetTupleDesc() *TupleDesc
}

// Implements the aggregation state for COUNT
type CountAggState struct {
	alias string
	expr  Expr
	count int
}

type EncryptedCountAggState struct {
	alias     string
	expr      Expr
	count     int64
	publicKey homo.Pubkey
}

func (a *CountAggState) Copy() AggState {
	return &CountAggState{a.alias, a.expr, a.count}
}

func (a *EncryptedCountAggState) Copy() EncryptedAggState {
	return &EncryptedCountAggState{a.alias, a.expr, a.count, a.publicKey}
}

func (a *CountAggState) Init(alias string, expr Expr, getter func(DBValue) any) error {
	a.count = 0
	a.expr = expr
	a.alias = alias
	return nil
}

func (a *EncryptedCountAggState) Init(alias string, expr Expr, getter func(DBValue) any, publicKey homo.Pubkey) error {
	a.count = 0
	a.expr = expr
	a.alias = alias
	a.publicKey = publicKey
	return nil
}

func (a *CountAggState) AddTuple(t *Tuple) {
	a.count++
}

func (a *EncryptedCountAggState) AddTuple(t *Tuple) {
	a.count++
}

func (a *CountAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{int64(a.count)}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

func (a *EncryptedCountAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{int64(a.count)}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

func (a *CountAggState) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *EncryptedCountAggState) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

// Implements the aggregation state for SUM
type SumAggState[T Number] struct {
	alias  string
	expr   Expr
	getter func(DBValue) any
	sum    T
}

type EncryptedSumAggState[T string] struct {
	alias     string
	expr      Expr
	getter    func(DBValue) any
	sum       string
	publicKey homo.Pubkey
}

func (a *SumAggState[T]) Copy() AggState {
	return &SumAggState[T]{a.alias, a.expr, a.getter, a.sum}
}

func (a *EncryptedSumAggState[T]) Copy() EncryptedAggState {
	return &EncryptedSumAggState[T]{a.alias, a.expr, a.getter, a.sum, a.publicKey}
}

func intAggGetter(v DBValue) any {
	intV := v.(IntField)
	return intV.Value
}

func stringAggGetter(v DBValue) any {
	stringV := v.(StringField)
	return stringV.Value
}

func (a *SumAggState[T]) Init(alias string, expr Expr, getter func(DBValue) any) error {
	a.sum = 0
	a.expr = expr
	a.alias = alias
	a.getter = getter
	return nil
}

func (a *EncryptedSumAggState[T]) Init(alias string, expr Expr, getter func(DBValue) any, publicKey homo.Pubkey) error {
	z, _ := publicKey.Encrypt(make([]byte, 0))
	a.sum = string(z)
	a.expr = expr
	a.alias = alias
	a.getter = getter
	a.publicKey = publicKey
	return nil
}

func (a *SumAggState[T]) AddTuple(t *Tuple) {
	v, _ := a.expr.EvalExpr(t)
	a.sum += a.getter(v).(T)
}

func (a *EncryptedSumAggState[T]) AddTuple(t *Tuple) {
	v, _ := a.expr.EvalExpr(t)
	b1 := []byte(a.sum)
	b2 := []byte(a.getter(v).(string))
	result, _ := a.publicKey.Add(b1, b2)
	a.sum = string(result)
}

func (a *SumAggState[T]) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *EncryptedSumAggState[T]) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", StringType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *SumAggState[T]) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{int64(a.sum)}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

func (a *EncryptedSumAggState[T]) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := StringField{a.sum}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

// Implements the aggregation state for AVG
// Note that we always AddTuple() at least once before Finalize()
// so no worries for divide-by-zero
type AvgAggState[T Number] struct {
	alias  string
	expr   Expr
	getter func(DBValue) any
	count  T
	sum    T
}

type EncryptedAvgAggState[T string] struct {
	alias     string
	expr      Expr
	getter    func(DBValue) any
	count     int64
	sum       string
	publicKey homo.Pubkey
}

func (a *AvgAggState[T]) Copy() AggState {
	return &AvgAggState[T]{a.alias, a.expr, a.getter, a.count, a.sum}
}

func (a *EncryptedAvgAggState[T]) Copy() EncryptedAggState {
	return &EncryptedAvgAggState[T]{a.alias, a.expr, a.getter, a.count, a.sum, a.publicKey}
}

func (a *AvgAggState[T]) Init(alias string, expr Expr, getter func(DBValue) any) error {
	a.alias = alias
	a.expr = expr
	a.getter = getter
	a.count = 0
	a.sum = 0
	return nil
}

func (a *EncryptedAvgAggState[T]) Init(alias string, expr Expr, getter func(DBValue) any, publicKey homo.Pubkey) error {
	a.alias = alias
	a.expr = expr
	a.getter = getter
	a.count = 0

	z, _ := publicKey.Encrypt(make([]byte, 0))
	a.sum = string(z)
	a.publicKey = publicKey
	return nil
}

func (a *AvgAggState[T]) AddTuple(t *Tuple) {
	v, _ := a.expr.EvalExpr(t)
	a.sum += a.getter(v).(T)
	a.count++
}

func (a *EncryptedAvgAggState[T]) AddTuple(t *Tuple) {
	v, _ := a.expr.EvalExpr(t)
	b1 := []byte(a.sum)
	b2 := []byte(a.getter(v).(string))
	result, _ := a.publicKey.Add(b1, b2)
	a.sum = string(result)
	a.count++
}

func (a *AvgAggState[T]) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *EncryptedAvgAggState[T]) GetTupleDesc() *TupleDesc {
	ft1 := FieldType{"sum", "", StringType}
	ft2 := FieldType{"count", "", IntType}
	fts := []FieldType{ft1, ft2}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *AvgAggState[T]) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{int64(a.sum / a.count)}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

func (a *EncryptedAvgAggState[T]) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f1 := StringField{a.sum}
	f2 := IntField{a.count}
	fs := []DBValue{f1, f2}
	t := Tuple{*td, fs, nil}
	return &t
}

// Implements the aggregation state for MAX
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN max
type MaxAggState[T constraints.Ordered] struct {
	alias  string
	expr   Expr
	max    T
	null   bool // whether the agg state have not seen any tuple inputted yet
	getter func(DBValue) any
}

func (a *MaxAggState[T]) Copy() AggState {
	return &MaxAggState[T]{a.alias, a.expr, a.max, true, a.getter}
}

func (a *MaxAggState[T]) Init(alias string, expr Expr, getter func(DBValue) any) error {
	a.expr = expr
	a.getter = getter
	a.alias = alias
	return nil
}

func (a *MaxAggState[T]) AddTuple(t *Tuple) {
	v, err := a.expr.EvalExpr(t)
	if err != nil {
		return
	}
	val := a.getter(v).(T)
	if a.null {
		a.max = val
		a.null = false
	} else if val > a.max {
		a.max = val
	}
}

func (a *MaxAggState[T]) GetTupleDesc() *TupleDesc {
	var ft FieldType
	switch any(a.max).(type) {
	case string:
		ft = FieldType{a.alias, "", StringType}
	default:
		ft = FieldType{a.alias, "", IntType}
	}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *MaxAggState[T]) Finalize() *Tuple {
	td := a.GetTupleDesc()
	var f any
	switch any(a.max).(type) {
	case string:
		f = StringField{any(a.max).(string)}
	default:
		f = IntField{any(a.max).(int64)}
	}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

// Implements the aggregation state for MIN
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN min
type MinAggState[T constraints.Ordered] struct {
	alias  string
	expr   Expr
	min    T
	null   bool
	getter func(DBValue) any
}

func (a *MinAggState[T]) Copy() AggState {
	return &MinAggState[T]{a.alias, a.expr, a.min, true, a.getter}
}

func (a *MinAggState[T]) Init(alias string, expr Expr, getter func(DBValue) any) error {
	a.expr = expr
	a.getter = getter
	a.alias = alias
	return nil
}

func (a *MinAggState[T]) AddTuple(t *Tuple) {
	v, err := a.expr.EvalExpr(t)
	if err != nil {
		return
	}
	val := a.getter(v).(T)
	if a.null {
		a.min = val
		a.null = false
	} else if val < a.min {
		a.min = val
	}
}

func (a *MinAggState[T]) GetTupleDesc() *TupleDesc {
	var ft FieldType
	switch any(a.min).(type) {
	case string:
		ft = FieldType{a.alias, "", StringType}
	default:
		ft = FieldType{a.alias, "", IntType}
	}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *MinAggState[T]) Finalize() *Tuple {
	td := a.GetTupleDesc()
	var f any
	switch any(a.min).(type) {
	case string:
		f = StringField{any(a.min).(string)}
	default:
		f = IntField{any(a.min).(int64)}
	}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}
