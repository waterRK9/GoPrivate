package godb

import (
	"errors"
	"testing"
)

func getDummyEncryptionScheme() EncryptionScheme {
	encryptInt := func(v any) (any, error) {
		_v, translated := v.(int64)
		if !translated {
			return nil, errors.New("Input is wrong type; should be int64")
		}
		return (_v + 1), nil
	}

	decryptInt := func(v any) (any, error) {
		_v, translated := v.(int64)
		if !translated {
			return nil, errors.New("Input is wrong type; should be int64")
		}
		return (_v - 1), nil
	}

	encryptString := func(v any) (any, error) {
		_v, translated := v.(string)
		if !translated {
			return nil, errors.New("Input is wrong type; should be string")
		}
		return (_v + "abc"), nil
	}

	decryptString := func(v any) (any, error) {
		_v, translated := v.(string)
		if !translated {
			return nil, errors.New("Input is wrong type; should be string")
		}
		return _v[:len(_v)-3], nil
	}

	encryptMethods := make(map[string]func(v any) (any, error))
	encryptMethods["age"] = encryptInt
	encryptMethods["name"] = encryptString

	decryptMethods := make(map[string]func(v any) (any, error))
	decryptMethods["age"] = decryptInt
	decryptMethods["name"] = decryptString

	defaultEncrypt := func(v any) (any, error) {
		return v, nil
	}

	return EncryptionScheme{
		EncryptMethods: encryptMethods,
		DefaultEncrypt: defaultEncrypt,
		DecryptMethods: decryptMethods,
		DefaultDecrypt: defaultEncrypt,
	}
}

func TestTupleEncryption(t *testing.T) {
	_, t1, _, _, _, _ := makeTestVars()

	e := getDummyEncryptionScheme()
	encryptedT1, err := e.encryptOrDecryptTuple(&t1, true)

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
	encryptedHf, err := e.encryptOrDecrypt(hf, "encrypted_test.dat", true, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}

	et1, err := e.encryptOrDecryptTuple(&t1, true)
	if err != nil {
		t.Fatalf(err.Error())
	}
	et2, err := e.encryptOrDecryptTuple(&t2, true)
	if err != nil {
		t.Fatalf(err.Error())
	}

	iter, _ := encryptedHf.Iterator(tid)
	i := 0
	for {
		tp, _ := iter()
		if tp == nil {
			break
		}
		if !tp.equals(et1) && !tp.equals(et2) {
			t.Errorf("Tuple encrypted incorrectly")
		}
		i = i + 1
	}
	if i != 2 {
		t.Errorf("Encrypted HeapFile iterator expected 2 tuples, got %d", i)
	}
}

func TestHeapFileDecryption(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars()
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)

	e := getDummyEncryptionScheme()
	encryptedHf, err := e.encryptOrDecrypt(hf, "encrypted_test.dat", true, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}

	decryptedHf, err := e.encryptOrDecrypt(encryptedHf, "decrypted_test.dat", false, tid)
	iter, _ := decryptedHf.Iterator(tid)
	i := 0
	for {
		tp, _ := iter()
		if tp == nil {
			break
		}
		if !tp.equals(&t1) && !tp.equals(&t2) {
			t.Errorf("Tuple decrypted incorrectly")
		}
		i = i + 1
	}
	if i != 2 {
		t.Errorf("Encrypted HeapFile iterator expected 2 tuples, got %d", i)
	}
}
