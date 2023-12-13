package godb

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// / Encryped AVG Test ///
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

	resultFileName := "encryptedresults/100_encrypted_mock_patient_data.csv"

	// Uncomment Only Below for First Run to Generate Encrypted File
	inputFileName := "encryptedresults/100_mock_patient_data.csv"
	encryptedHf, e := CSVToEncryptedDat(td, inputFileName, resultFileName, sql)

	// Uncomment Only Below for Subsequent Runs to Avoid Generating Encrypted File
	// err, e := translateQuery(sql)
	// if err != nil {
	// 	panic(err.Error())
	// }

	// bp := NewBufferPool(10)
	// encryptedHf := HeapFile{
	// 	bufPool: bp,
	// 	desc:    &td,
	// 	file:    resultFileName,
	// }
	//

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
}

// / Unencrypted AVG Test///
func TestAvgAgg(t *testing.T) {
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

	resultFileName := "encryptedresults/100_unencrypted_mock_patient_data.dat"
	os.Remove(resultFileName)

	bp := NewBufferPool(10)
	hf, err := NewHeapFile(resultFileName, &td, bp)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// Uncomment the below on first run to generate dat file
	inputFileName := "encryptedresults/100_mock_patient_data.csv"
	f, err := os.Open(inputFileName)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = hf.LoadFromCSV(f, true, ",", false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	////

	sa := AvgAggState[int64]{}
	expr := FieldExpr{FieldType{Fname: "age", TableQualifier: "t"}}
	sa.Init("avg", &expr, intAggGetter)
	agg := NewAggregator([]AggState{&sa}, hf)

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
	fmt.Println(tup)
}

// / Encryped COUNT Test ///
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

	resultFileName := "encryptedresults/1000_encrypted_mock_patient_data.csv"

	// Uncomment Only Below for First Run to Generate Encrypted File
	//inputFileName := "encryptedresults/1000_mock_patient_data.csv"
	//encryptedHf, e := CSVToEncryptedDat(td, inputFileName, resultFileName, sql)

	// Uncomment Only Below for Subsequent Runs to Avoid Generating Encrypted File
	err, e := translateQuery(sql)
	if err != nil {
		panic(err.Error())
	}

	bp := NewBufferPool(10)
	encryptedHf := &HeapFile{
		bufPool: bp,
		desc:    &td,
		file:    resultFileName,
	}

	start := time.Now()
	aa := EncryptedCountAggState{}
	expr := FieldExpr{FieldType{Fname: "ssn", TableQualifier: "t"}}
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
	time_now := time.Now()
	elapsed := time_now.Sub(start)
	fmt.Println(elapsed)

	//t.Fatalf("Expected non-null tuple")
}

// / Unencryped COUNT Test ///
func TestCountAgg(t *testing.T) {
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

	resultFileName := "encryptedresults/1000_unencrypted_mock_patient_data.dat"
	os.Remove(resultFileName)

	bp := NewBufferPool(10)
	hf, err := NewHeapFile(resultFileName, &td, bp)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// Uncomment the below on first run to generate dat file
	inputFileName := "encryptedresults/1000_mock_patient_data.csv"
	f, err := os.Open(inputFileName)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = hf.LoadFromCSV(f, true, ",", false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	////

	start := time.Now()
	aa := CountAggState{}
	expr := FieldExpr{FieldType{Fname: "ssn", TableQualifier: "t"}}
	aa.Init("count", &expr, stringAggGetter)
	agg := NewAggregator([]AggState{&aa}, hf)

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
	time_now := time.Now()
	elapsed := time_now.Sub(start)
	fmt.Println(elapsed)

	//t.Fatalf("Expected non-null tuple")

}

// / Encryped SUM Test ///
func TestEncryptedSumAgg(t *testing.T) {
	sql := "select sum(age) from t"

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
	resultFileName := "encryptedresults/1000_encrypted_mock_patient_data.csv"

	// Uncomment Only Below for First Run to Generate Encrypted File
	// inputFileName := "encryptedresults/1000_mock_patient_data.csv"
	// encryptedHf, e := CSVToEncryptedDat(td, inputFileName, resultFileName, sql)

	// Uncomment Only Below for Subsequent Runs to Avoid Generating Encrypted File
	err, e := translateQuery(sql)
	if err != nil {
		panic(err.Error())
	}

	bp := NewBufferPool(10)
	encryptedHf, err := NewHeapFile(resultFileName, &td, bp)
	if err != nil {
		t.Fatalf(err.Error())
	}
	// ///

	aa := EncryptedSumAggState[string]{}
	expr := FieldExpr{FieldType{Fname: "age", TableQualifier: "t"}}
	aa.Init("sum", &expr, stringAggGetter, *e.PublicKeys["age"])

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
	fmt.Println(tup)
}

// / Unencryped SUM Test ///
func TestSumAgg(t *testing.T) {
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

	resultFileName := "encryptedresults/1000_unencrypted_mock_patient_data.dat"
	os.Remove(resultFileName)

	bp := NewBufferPool(10)
	hf, err := NewHeapFile(resultFileName, &td, bp)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// Uncomment the below on first run to generate dat file
	inputFileName := "encryptedresults/1000_mock_patient_data.csv"
	f, err := os.Open(inputFileName)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = hf.LoadFromCSV(f, true, ",", false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	////

	sa := SumAggState[int64]{}
	expr := FieldExpr{FieldType{Fname: "age", TableQualifier: "t"}}
	sa.Init("sum", &expr, intAggGetter)
	agg := NewAggregator([]AggState{&sa}, hf)

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
	fmt.Println(tup)
}

// / Encryped AVG Test with Filter ///
func TestEncryptedAvgAggWhere(t *testing.T) {
	//the query we are trying to execute
	//"select avg(age) from t where diagnosis_code = "S61519A" "

	//the query translator is quite simplistic at this moment, so we
	//pass in a simpler query that has the same encryption scheme instead
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

	var f FieldType = FieldType{"diagnosis_code", "", StringType}
	encrypted_val, err := e.encryptVal("S61519A", "diagnosis_code")
	if err != nil {
		t.Fatalf(err.Error())
	}
	filt, err := NewStringFilter(&ConstExpr{StringField{encrypted_val}, StringType}, OpEq, &FieldExpr{f}, encryptedHf)
	if err != nil {
		t.Errorf(err.Error())
	}

	iter1, err := filt.Iterator(nil)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter1 == nil {
		t.Fatalf("Iterator was nil")
	}

	cnt := 0
	for {
		tup, _ := iter1()
		if tup == nil {
			break
		}
		fmt.Printf("filter passed tup %d: %v\n", cnt, tup)
		cnt++
	}

	aa := EncryptedAvgAggState[string]{}
	expr := FieldExpr{FieldType{Fname: "age", TableQualifier: "t"}}
	aa.Init("avg", &expr, stringAggGetter, *e.PublicKeys["age"])
	agg := NewEncryptedAggregator([]EncryptedAggState{&aa}, filt)

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
	if sum != 184 || count != 2 {
		t.Errorf("unexpected sum or count")
	}
}

// / Encryped COUNT Test with Filter ///
func TestEncryptedCountAggWhere(t *testing.T) {
	//the query we are trying to execute
	//"select count(ssn) from t where diagnosis_code = "S61519A" "

	//the query translator is quite simplistic at this moment, so we
	//pass in a simpler query that has the same encryption scheme instead
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

	inputFileName := "encryptedresults/small_mock_patitent_data.csv"
	resultFileName := "encryptedresults/small_encrypted_mock_patitent_data.csv"

	encryptedHf, e := CSVToEncryptedDat(td, inputFileName, resultFileName, sql)

	var f FieldType = FieldType{"diagnosis_code", "", StringType}
	encrypted_val, err := e.encryptVal("S61519A", "diagnosis_code")
	if err != nil {
		t.Fatalf(err.Error())
	}
	filt, err := NewStringFilter(&ConstExpr{StringField{encrypted_val}, StringType}, OpEq, &FieldExpr{f}, encryptedHf)
	if err != nil {
		t.Errorf(err.Error())
	}

	iter1, err := filt.Iterator(nil)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter1 == nil {
		t.Fatalf("Iterator was nil")
	}

	cnt := 0
	for {
		tup, _ := iter1()
		if tup == nil {
			break
		}
		fmt.Printf("filter passed tup %d: %v\n", cnt, tup)
		cnt++
	}

	aa := EncryptedCountAggState{}
	expr := FieldExpr{FieldType{Fname: "ssn", TableQualifier: "t"}}
	aa.Init("count", &expr, stringAggGetter, *e.PublicKeys["ssn"])
	agg := NewEncryptedAggregator([]EncryptedAggState{&aa}, filt)

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

	count := result.Fields[0].(IntField).Value

	fmt.Println(count)
	if count != 2 {
		t.Errorf("unexpected sum or count")
	}
}

// / Encryped AVG Test with Vertical Join ///
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
	encryptedHf2 := CSVToEncryptedDatGivenE(td, inputFileName2, resultFileName2, e)

	join, err := NewVerticalJoin([]Operator{encryptedHf1, encryptedHf2})
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
	if sum != 1186 || count != 16 {
		t.Errorf("unexpected sum or count")
	}
}

// / Encryped COUNT Test with Vertical Join ///
func TestEncryptedCountAggVertiJoin(t *testing.T) {
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

	inputFileName1 := "encryptedresults/small_mock_patitent_data.csv"
	resultFileName1 := "encryptedresults/small_encrypted_mock_patitent_data.csv"
	inputFileName2 := "encryptedresults/other_small_mock_patitent_data.csv"
	resultFileName2 := "encryptedresults/other_small_encrypted_mock_patitent_data.csv"

	encryptedHf1, e := CSVToEncryptedDat(td, inputFileName1, resultFileName1, sql)
	encryptedHf2 := CSVToEncryptedDatGivenE(td, inputFileName2, resultFileName2, e)

	join, err := NewVerticalJoin([]Operator{encryptedHf1, encryptedHf2})
	if err != nil {
		t.Fatalf(err.Error())
	}

	aa := EncryptedCountAggState{}
	expr := FieldExpr{FieldType{Fname: "ssn", TableQualifier: "t"}}
	aa.Init("count", &expr, stringAggGetter, *e.PublicKeys["ssn"])
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

	count := result.Fields[0].(IntField).Value

	fmt.Println(count)
	if count != 16 {
		t.Errorf("unexpected sum or count")
	}
}

// / Encryped COUNT Test with DISTINCT and Vertical Join ///
func TestEncryptedCountAggVertiJoinDistinct(t *testing.T) {
	//the query we are trying to execute
	//"select count(ssn) from (select distinct ssn from t)"

	//the query translator is quite simplistic at this moment, so we
	//pass in a simpler query that has the same encryption scheme instead
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

	inputFileName1 := "encryptedresults/small_mock_patitent_data.csv"
	resultFileName1 := "encryptedresults/small_encrypted_mock_patitent_data.csv"
	inputFileName2 := "encryptedresults/other_small_mock_patitent_data.csv"
	resultFileName2 := "encryptedresults/other_small_encrypted_mock_patitent_data.csv"

	encryptedHf1, e := CSVToEncryptedDat(td, inputFileName1, resultFileName1, sql)
	encryptedHf2 := CSVToEncryptedDatGivenE(td, inputFileName2, resultFileName2, e)

	join, err := NewVerticalJoin([]Operator{encryptedHf1, encryptedHf2})
	if err != nil {
		t.Fatalf(err.Error())
	}

	var outNames []string = make([]string, 1)
	outNames[0] = "ssn"
	fieldExpr1 := FieldExpr{td.Fields[1]}
	proj, _ := NewProjectOp([]Expr{&fieldExpr1}, outNames, true, join)
	if proj == nil {
		t.Fatalf("project was nil")
	}

	aa := EncryptedCountAggState{}
	expr := FieldExpr{FieldType{Fname: "ssn", TableQualifier: "t"}}
	aa.Init("count", &expr, stringAggGetter, *e.PublicKeys["ssn"])
	agg := NewEncryptedAggregator([]EncryptedAggState{&aa}, proj)

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

	count := result.Fields[0].(IntField).Value

	fmt.Println(count)
	if count != 15 {
		t.Errorf("unexpected sum or count")
	}
}
