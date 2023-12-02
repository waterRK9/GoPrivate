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
	_, err = e.encryptOrDecrypt(hf, "encrypted_patients.dat", true, tid)
	if err != nil {
		t.Errorf("%s", err.Error())
		return
	}

	// aa := EncryptedAvgAggState[[]byte]{}
	// expr := FieldExpr{FieldType{Fname: "age", TableQualifier: "t"}}
	_, exists := e.PublicKeys["age"]
	if !exists {
		println("COULD NOT FIND")
	}
	// aa.Init("avg", &expr, intAggGetter, *e.PublicKeys["age"])
}
