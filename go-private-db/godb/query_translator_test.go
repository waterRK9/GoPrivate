package godb

import (
	"os"
	"testing"
)

func MakeTestPatientDatabase(bp *BufferPool) (*HeapFile, error) {
	var td = TupleDesc{Fields: []FieldType{
		{Fname: "id", Ftype: StringType},
		{Fname: "ssn", Ftype: StringType},
		{Fname: "age", Ftype: StringType},
		{Fname: "first_name", Ftype: StringType},
		{Fname: "diagnosis_code", Ftype: StringType},
	}}

	os.Remove("patients.dat")
	hf, err := NewHeapFile("patients.dat", &td, bp)
	if err != nil {
		return nil, err
	}

	f, err := os.Open("patientdb.txt")
	if err != nil {
		return nil, err
	}
	err = hf.LoadFromCSV(f, true, ",", false)
	if err != nil {
		return nil, err
	}
	return hf, nil
}

func TestTranslation(t *testing.T) {
	var queries []string = []string{
		"select avg(age) from t",
	}

	bp := NewBufferPool(10)
	hf, err := MakeTestPatientDatabase(bp)
	if err != nil {
		t.Errorf("%s", err.Error())
		return
	}
	for _, sql := range queries {
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
		_, err = e.encryptOrDecrypt(encryptedHf, "decrypted_patients.dat", false, tid)
		if err != nil {
			t.Errorf("%s", err.Error())
			return
		}
	}
}
