package godb

import (
	"errors"
	"testing"
)

func getDummyEncryptionScheme() EncryptionScheme {
	applyInt := func(v any) (any, error) {
		_v, translated := v.(int64)
		if !translated {
			return nil, errors.New("Input is wrong type; should be int64")
		}
		return (_v + 1), nil
	}

	applyString := func(v any) (any, error) {
		_v, translated := v.(string)
		if !translated {
			return nil, errors.New("Input is wrong type; should be string")
		}
		return (_v + "abc"), nil
	}

	encryptMethods := make(map[string]func(v any) (any, error))
	encryptMethods["age"] = applyInt
	encryptMethods["name"] = applyString

	defaultEncrypt := func(v any) (any, error) {
		return v, nil
	}

	return EncryptionScheme{EncryptMethods: encryptMethods, DefaultEncrypt: defaultEncrypt}
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

func TestHeapFileEncryption(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars()
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)

	e := getDummyEncryptionScheme()
	encryptedHf, err := e.encrypt(hf, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}

	iter, _ := encryptedHf.Iterator(tid)
	i := 0
	for {
		t, _ := iter()
		if t == nil {
			break
		}
		i = i + 1
	}
	if i != 2 {
		t.Errorf("Encrypted HeapFile iterator expected 2 tuples, got %d", i)
	}
}
