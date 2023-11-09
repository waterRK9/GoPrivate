package godb

import "testing"

func getDummyEncryptionScheme() EncryptionScheme {
	applyInt := func(v int64) (int64, error) {
		return (v + 1), nil
	}

	applyString := func(v string) (string, error) {
		return (v + "abc"), nil
	}

	return EncryptionScheme{ApplyInt: applyInt, ApplyString: applyString}
}

func TestTupleEncryption(t *testing.T) {
	_, t1, _, _, _, _ := makeTestVars()

	e := getDummyEncryptionScheme()
	encryptedT1, err := e.encryptTuple(&t1)

	if err != nil {
		t.Fatalf(err.Error())
	}

	if encryptedT1.Fields[0].(StringField).Value != t1.Fields[0].(StringField).Value+"abc" {
		t.Errorf("Encrypted string is incorrect")
	}
	if encryptedT1.Fields[1].(IntField).Value != t1.Fields[1].(IntField).Value+1 {
		t.Errorf("Encrypted int is incorrect")
	}
}
