package godb

import (
	"fmt"
	"testing"
)

// func TestEncryptedAvgAgg(t *testing.T) {
// 	sql := "select avg(age) from t"
// 	bp := NewBufferPool(10)
// 	hf, err := MakeTestPatientDatabase(bp)
// 	if err != nil {
// 		t.Errorf("%s", err.Error())
// 		return
// 	}

// 	err, e := translateQuery(sql)
// 	if err != nil {
// 		t.Errorf("%s", err.Error())
// 		return
// 	}

// 	tid := NewTID()
// 	bp.BeginTransaction(tid)
// 	encryptedHf, err := e.encryptOrDecrypt(hf, "encrypted_patients.dat", true, tid)
// 	if err != nil {
// 		t.Errorf("%s", err.Error())
// 		return
// 	}

// 	aa := EncryptedAvgAggState[string]{}
// 	expr := FieldExpr{FieldType{Fname: "age", TableQualifier: "t"}}
// 	aa.Init("avg", &expr, stringAggGetter, *e.PublicKeys["age"])
// 	agg := NewEncryptedAggregator([]EncryptedAggState{&aa}, encryptedHf)

// 	iter, err := agg.Iterator(tid)
// 	if err != nil {
// 		t.Fatalf(err.Error())
// 	}
// 	if iter == nil {
// 		t.Fatalf("Iterator was nil")
// 	}
// 	tup, err := iter()
// 	if err != nil {
// 		t.Fatalf(err.Error())
// 	}
// 	if tup == nil {
// 		t.Fatalf("Expected non-null tuple")
// 	}

// 	result, err := e.encryptOrDecryptTuple(tup, false)
// 	if err != nil {
// 		t.Fatalf(err.Error())
// 	}

// 	sum := result.Fields[0].(IntField).Value
// 	count := result.Fields[1].(IntField).Value
// 	if sum != 395 || count != 10 {
// 		t.Errorf("unexpected sum or count")
// 	}
// }

func TestEncryptedAvgAgg(t *testing.T) {
	sql := "select avg(age) from t"

	var td = TupleDesc{Fields: []FieldType{
		{Fname: "id", Ftype: StringType},
		{Fname: "ssn", Ftype: StringType},
		{Fname: "first_name", Ftype: StringType},
		{Fname: "last_name", Ftype: StringType},
		{Fname: "phone_number", Ftype: StringType},
		{Fname: "gender", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
		{Fname: "diagnosis_code", Ftype: StringType},
	}}

	inputFileName := "encryptedresults/small_mock_patitent_data.csv"
	resultFileName := "encryptedresults/small_encrypted_mock_patitent_data.csv"

	encryptedHf, e := CSVToEncryptedDat(td, inputFileName, resultFileName, sql)

	aa := EncryptedAvgAggState[string]{}
	expr := FieldExpr{FieldType{Fname: "age", TableQualifier: "t"}}
	aa.Init("avg", &expr, stringAggGetter, *e.PublicKeys["age"])
	agg := NewEncryptedAggregator([]EncryptedAggState{&aa}, encryptedHf)

	iter, err := agg.Iterator(nil)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}
	tup, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if tup == nil {
		t.Fatalf("Expected non-null tuple")
	}

	result, err := e.encryptOrDecryptTuple(tup, false)
	if err != nil {
		t.Fatalf(err.Error())
	}

	sum := result.Fields[0].(IntField).Value
	count := result.Fields[1].(IntField).Value

	fmt.Println(sum, count)
	if sum != 1702 || count != 27 {
		t.Errorf("unexpected sum or count")
	}
}

func TestEncryptedCountAgg(t *testing.T) {
	sql := "select count(ssn) from t"

	var td = TupleDesc{Fields: []FieldType{
		{Fname: "id", Ftype: StringType},
		{Fname: "ssn", Ftype: StringType},
		{Fname: "first_name", Ftype: StringType},
		{Fname: "last_name", Ftype: StringType},
		{Fname: "phone_number", Ftype: StringType},
		{Fname: "gender", Ftype: StringType},
		{Fname: "age", Ftype: StringType},
		{Fname: "diagnosis_code", Ftype: StringType},
	}}

	inputFileName := "encryptedresults/298_mock_patient_data.csv"
	resultFileName := "encryptedresults/298_encrypted_mock_patient_data.dat"

	encryptedHf, e := CSVToEncryptedDat(td, inputFileName, resultFileName, sql)

	aa := EncryptedCountAggState{}
	expr := FieldExpr{FieldType{Fname: "ssn", TableQualifier: "t", Ftype: StringType}}
	aa.Init("count", &expr, stringAggGetter, *e.PublicKeys["ssn"])
	agg := NewEncryptedAggregator([]EncryptedAggState{&aa}, encryptedHf)

	iter, err := agg.Iterator(nil)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}
	tup, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if tup == nil {
		t.Fatalf("Expected non-null tuple")
	}

	result, err := e.encryptOrDecryptTuple(tup, false)
	if err != nil {
		t.Fatalf(err.Error())
	}

	print("yes")
	//sum := result.Fields[0].(IntField).Value
	count := result.Fields[0].(IntField).Value
	if count != 298 {
		t.Errorf("unexpected sum or count")
	}
}

func TestEncryptedAvgAggVertiJoin(t *testing.T) {
	sql := "select avg(age) from t"

	var td = TupleDesc{Fields: []FieldType{
		{Fname: "id", Ftype: StringType},
		{Fname: "ssn", Ftype: StringType},
		{Fname: "first_name", Ftype: StringType},
		{Fname: "last_name", Ftype: StringType},
		{Fname: "phone_number", Ftype: StringType},
		{Fname: "gender", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
		{Fname: "diagnosis_code", Ftype: StringType},
	}}

	inputFileName1 := "encryptedresults/small_mock_patitent_data.csv"
	resultFileName1 := "encryptedresults/small_encrypted_mock_patitent_data.csv"
	inputFileName2 := "encryptedresults/other_small_mock_patitent_data.csv"
	resultFileName2 := "encryptedresults/other_small_encrypted_mock_patitent_data.csv"

	encryptedHf1, e := CSVToEncryptedDat(td, inputFileName1, resultFileName1, sql)
	//encryptedHf2, e := CSVToEncryptedDat(td, inputFileName2, resultFileName2, sql)
	encryptedHf2 := CSVToEncryptedDatGivenE(td, inputFileName2, resultFileName2, e)

	join, err := NewVerticalJoin([]Operator{encryptedHf1, encryptedHf2}, 100)
	if err != nil {
		t.Fatalf(err.Error())
	}

	aa := EncryptedAvgAggState[string]{}
	expr := FieldExpr{FieldType{Fname: "age", TableQualifier: "t"}}
	aa.Init("avg", &expr, stringAggGetter, *e.PublicKeys["age"])
	agg := NewEncryptedAggregator([]EncryptedAggState{&aa}, join)

	iter, err := agg.Iterator(nil)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}
	tup, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if tup == nil {
		t.Fatalf("Expected non-null tuple")
	}

	result, err := e.encryptOrDecryptTuple(tup, false)
	if err != nil {
		t.Fatalf(err.Error())
	}

	sum := result.Fields[0].(IntField).Value
	count := result.Fields[1].(IntField).Value

	fmt.Println(sum, count)
	if sum != 2412 || count != 35 {
		t.Errorf("unexpected sum or count")
	}
}
