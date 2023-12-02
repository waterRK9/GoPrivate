package godb

import (
	"testing"
)

func TestEncryptedAvgAgg(t *testing.T) {
	sql := "select avg(age) from t"
	bp := NewBufferPool(10)
	hf, err := MakeTestPatientDatabase(bp)
	if err != nil {
		t.Errorf("%s", err.Error())
		return
	}

	err, e := translateQuery(sql)
	if err != nil {
		t.Errorf("%s", err.Error())
		return
	}

	tid := NewTID()
	bp.BeginTransaction(tid)
	encryptedHf, err := e.encryptOrDecrypt(hf, "encrypted_patients.dat", true, tid)
	if err != nil {
		t.Errorf("%s", err.Error())
		return
	}

	aa := EncryptedAvgAggState[string]{}
	expr := FieldExpr{FieldType{Fname: "age", TableQualifier: "t"}}
	aa.Init("avg", &expr, stringAggGetter, *e.PublicKeys["age"])
	agg := NewEncryptedAggregator([]EncryptedAggState{&aa}, encryptedHf)
	iter, err := agg.Iterator(tid)
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
	// sum := tup.Fields[0].(IntField).Value
	// if sum != 1024 {
	// 	t.Errorf("unexpected sum")
	// }
}
