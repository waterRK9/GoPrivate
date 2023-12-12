package godb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/getamis/alice/crypto/homo/paillier"
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

	paillierMap := make(map[string](*(paillier.Paillier)))

	return EncryptionScheme{
		EncryptMethods: encryptMethods,
		DefaultEncrypt: defaultEncrypt,
		DecryptMethods: decryptMethods,
		DefaultDecrypt: defaultEncrypt,
		PaillierMap:    paillierMap,
	}
}

func CSVToEncryptedDat(desc TupleDesc, inputFilename string, resultFileName string, sql string) (*HeapFile, EncryptionScheme) {
	// assumes there are no .dat files with the below name
	tempFileName := inputFilename + ".dat"
	os.Remove(tempFileName)
	os.Remove(resultFileName)
	defer os.Remove(tempFileName)

	// Make heapfile for csv file
	bp := NewBufferPool(10)
	hf, err := NewHeapFile(tempFileName, &desc, bp)
	if err != nil {
		print("ERROR MAKING TEST VARS, BLARGH")
		panic(err.Error())
	}

	//bp.BeginTransaction(nil)

	// Reading from CSV
	f, err := os.Open(inputFilename)
	if err != nil {
		panic("GenerateCSVToEncryptedDat: couldn't open csv file")
	}
	err = hf.LoadFromCSV(f, true, ",", false)
	if err != nil {
		panic(err.Error())
	}

	// Generating encrypted file
	os.Remove(resultFileName)
	err, e := translateQuery(sql)
	if err != nil {
		panic(err.Error())
	}

	encryptedHf, err := e.encryptOrDecrypt(hf, resultFileName, true, nil)
	if err != nil {
		panic(err.Error())
	}

	// Emptying bufferpool
	encryptedHf.bufPool.FlushAllPages()
	return encryptedHf, e
}

// to encrypt a csv file: modify the file name variables, then run this test
func TestRunCSVToEncryptedDat(t *testing.T) {
	// File Name Variables
	csvFileName := "encryptedresults/298_mock_patient_data.csv"
	encryptedDatFileName := "encryptedresults/298_encrypted_mock_patient_data.dat"

	// Making Test Variables
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

	// Creating Encrypted .dat file
	encryptedHf, e := CSVToEncryptedDat(td, csvFileName, encryptedDatFileName, "select avg(age) from t")

	// UNDER TESTING: attempt to iterate through the encrypted tuples, and decrypt and print out each of the tuples
	iter, err := encryptedHf.Iterator(nil)
	if err != nil {
		panic(err)
	}

	for {
		tuple, err := iter()
		if err != nil {
			panic(err)
		} else if tuple != nil {
			decryptedTuple, err := e.encryptOrDecryptTuple(tuple, false)
			if err != nil {
				panic(err)
			}
			fmt.Println(decryptedTuple)
		}
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

func TestDetEncryptionInt64(t *testing.T) {
	key := []byte("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	encryptFunc := newDetEncryptionFunc(key)

	var v1 int64
	var v2 int64

	v1 = 125
	v2 = 125
	e1, _ := encryptFunc(v1)
	e2, _ := encryptFunc(v2)
	if e1 != e2 {
		t.Errorf("Expected equal values! got %v != %v", e1, e2)
	}

	v1 = 0
	v2 = 0
	e1, _ = encryptFunc(v1)
	e2, _ = encryptFunc(v2)
	if e1 != e2 {
		t.Errorf("Expected equal values! got %v != %v", e1, e2)
	}

	v1 = 1234567890
	v2 = 1234567890
	e1, _ = encryptFunc(v1)
	e2, _ = encryptFunc(v2)
	if e1 != e2 {
		t.Errorf("Expected equal values! got %v != %v", e1, e2)
	}
}

func TestDetDecryptionInt64(t *testing.T) {
	key := []byte("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	encryptFunc := newDetEncryptionFunc(key)
	decryptFunc := newDetDecryptionFunc(key)

	var v1 int64
	var v2 int64

	v1 = 125
	v2 = 125
	e1, _ := encryptFunc(v1)
	e2, _ := encryptFunc(v2)
	d1, _ := decryptFunc(e1.(string))
	d2, _ := decryptFunc(e2.(string))
	if fmt.Sprint(v1) != d1 {
		t.Errorf("Expected equal values! got %v != %v", d1, d2)
	}
	if fmt.Sprint(v2) != d2 {
		t.Errorf("Expected equal values! got %v != %v", d1, d2)
	}

	v1 = 0
	v2 = 0
	e1, _ = encryptFunc(v1)
	e2, _ = encryptFunc(v2)
	d1, _ = decryptFunc(e1.(string))
	d2, _ = decryptFunc(e2.(string))
	if fmt.Sprint(v1) != d1 {
		t.Errorf("Expected equal values! got %v != %v", d1, d2)
	}
	if fmt.Sprint(v2) != d2 {
		t.Errorf("Expected equal values! got %v != %v", d1, d2)
	}

	v1 = 1234567890
	v2 = 1234567890
	e1, _ = encryptFunc(v1)
	e2, _ = encryptFunc(v2)
	d1, _ = decryptFunc(e1.(string))
	d2, _ = decryptFunc(e2.(string))
	if fmt.Sprint(v1) != d1 {
		t.Errorf("Expected equal values! got %v != %v", d1, d2)
	}
	if fmt.Sprint(v2) != d2 {
		t.Errorf("Expected equal values! got %v != %v", d1, d2)
	}
}

func TestDetEncryptionString(t *testing.T) {
	key := []byte("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	encryptFunc := newDetEncryptionFunc(key)

	var v1 string
	var v2 string

	v1 = ""
	v2 = ""
	e1, _ := encryptFunc(v1)
	e2, _ := encryptFunc(v2)
	if e1 != e2 {
		t.Errorf("Expected equal values! got %v != %v", e1, e2)
	}

	v1 = "aBc"
	v2 = "aBc"
	e1, _ = encryptFunc(v1)
	e2, _ = encryptFunc(v2)
	if e1 != e2 {
		t.Errorf("Expected equal values! got %v != %v", e1, e2)
	}

	v1 = "aBc123"
	v2 = "aBc123"
	e1, _ = encryptFunc(v1)
	e2, _ = encryptFunc(v2)
	if e1 != e2 {
		t.Errorf("Expected equal values! got %v != %v", e1, e2)
	}
}

func TestDetDecryptionString(t *testing.T) {
	key := []byte("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	encryptFunc := newDetEncryptionFunc(key)
	decryptFunc := newDetDecryptionFunc(key)

	var v1 string
	var v2 string

	v1 = ""
	v2 = ""
	e1, _ := encryptFunc(v1)
	e2, _ := encryptFunc(v2)
	d1, _ := decryptFunc(e1.(string))
	d2, _ := decryptFunc(e2.(string))
	if fmt.Sprint(v1) != d1 {
		t.Errorf("Expected equal values! got %v != %v", d1, d2)
	}
	if fmt.Sprint(v2) != d2 {
		t.Errorf("Expected equal values! got %v != %v", d1, d2)
	}

	v1 = "aBc"
	v2 = "aBc"
	e1, _ = encryptFunc(v1)
	e2, _ = encryptFunc(v2)
	d1, _ = decryptFunc(e1.(string))
	d2, _ = decryptFunc(e2.(string))
	if fmt.Sprint(v1) != d1 {
		t.Errorf("Expected equal values! got %v != %v", d1, d2)
	}
	if fmt.Sprint(v2) != d2 {
		t.Errorf("Expected equal values! got %v != %v", d1, d2)
	}

	v1 = "aBc123"
	v2 = "aBc123"
	e1, _ = encryptFunc(v1)
	e2, _ = encryptFunc(v2)
	d1, _ = decryptFunc(e1.(string))
	d2, _ = decryptFunc(e2.(string))
	if fmt.Sprint(v1) != d1 {
		t.Errorf("Expected equal values! got %v != %v", d1, d2)
	}
	if fmt.Sprint(v2) != d2 {
		t.Errorf("Expected equal values! got %v != %v", d1, d2)
	}
}

func TestHomDecryptionInt64(t *testing.T) {
	e := getDummyEncryptionScheme()
	keySize := 2048
	encryptFunc := e.newHomEncryptionFunc(keySize)
	decryptFunc := e.newHomDecryptionFunc()

	var v1 int64
	var v2 int64

	v1 = 125
	v2 = 125
	e1, _ := encryptFunc(v1)
	e2, _ := encryptFunc(v2)
	d1, _ := decryptFunc(e1)
	d2, _ := decryptFunc(e2)

	//convert to int
	d1Byte := make([]byte, 8)
	copy(d1Byte[8-len(d1.([]byte)):], d1.([]byte))
	d1Int := binary.BigEndian.Uint64(d1Byte)

	d2Byte := make([]byte, 8)
	copy(d2Byte[8-len(d2.([]byte)):], d2.([]byte))
	d2Int := binary.BigEndian.Uint64(d2Byte)

	if v1 != int64(d1Int) {
		t.Errorf("Expected equal values! got %v != %v", d1, d2)
	}
	if v2 != int64(d2Int) {
		t.Errorf("Expected equal values! got %v != %v", d1, d2)
	}

	v1 = 0
	v2 = 0
	e1, _ = encryptFunc(v1)
	e2, _ = encryptFunc(v2)
	d1, _ = decryptFunc(e1)
	d2, _ = decryptFunc(e2)
	d1Byte = make([]byte, 8)
	copy(d1Byte[8-len(d1.([]byte)):], d1.([]byte))
	d1Int = binary.BigEndian.Uint64(d1Byte)

	d2Byte = make([]byte, 8)
	copy(d2Byte[8-len(d2.([]byte)):], d2.([]byte))
	d2Int = binary.BigEndian.Uint64(d2Byte)

	if v1 != int64(d1Int) {
		t.Errorf("Expected equal values! got %v != %v", d1, d2)
	}
	if v2 != int64(d2Int) {
		t.Errorf("Expected equal values! got %v != %v", d1, d2)
	}

	v1 = 1234567890
	v2 = 1234567890
	e1, _ = encryptFunc(v1)
	e2, _ = encryptFunc(v2)
	d1, _ = decryptFunc(e1)
	d2, _ = decryptFunc(e2)

	//convert to int
	d1Byte = make([]byte, 8)
	copy(d1Byte[8-len(d1.([]byte)):], d1.([]byte))
	d1Int = binary.BigEndian.Uint64(d1Byte)

	d2Byte = make([]byte, 8)
	copy(d2Byte[8-len(d2.([]byte)):], d2.([]byte))
	d2Int = binary.BigEndian.Uint64(d2Byte)

	if v1 != int64(d1Int) {
		t.Errorf("Expected equal values! got %v != %v", d1, d2)
	}
	if v2 != int64(d2Int) {
		t.Errorf("Expected equal values! got %v != %v", d1, d2)
	}
}

func TestHomSumInt64(t *testing.T) {
	e := getDummyEncryptionScheme()
	keySize := 2048
	encryptFunc := e.newHomEncryptionFunc(keySize)
	decryptFunc := e.newHomDecryptionFunc()

	var v1 int64
	var v2 int64

	v1 = 125
	v2 = 125
	e1, _ := encryptFunc(v1)
	e2, _ := encryptFunc(v2)
	sum, err := e.homAdd(e1.([]byte), e2.([]byte))
	if err != nil {
		t.Errorf("Could not add numbers!!")
	}

	d1, _ := decryptFunc(sum)

	//convert to int
	d1Byte := make([]byte, 8)
	copy(d1Byte[8-len(d1.([]byte)):], d1.([]byte))
	d1Int := binary.BigEndian.Uint64(d1Byte)
	if v1+v2 != int64(d1Int) {
		t.Errorf("Expected a different sum! got %v != %v", int64(d1Int), v1+v2)
	}

	v1 = 0
	v2 = 0
	e1, _ = encryptFunc(v1)
	e2, _ = encryptFunc(v2)
	sum, err = e.homAdd(e1.([]byte), e2.([]byte))

	if err != nil {
		t.Errorf("Could not add numbers!!")
	}
	d1, _ = decryptFunc(sum)

	//convert to int
	d1Byte = make([]byte, 8)
	copy(d1Byte[8-len(d1.([]byte)):], d1.([]byte))
	d1Int = binary.BigEndian.Uint64(d1Byte)

	if v1+v2 != int64(d1Int) {
		t.Errorf("Expected a different sum! got %v != %v", int64(d1Int), v1+v2)
	}

	v1 = 1234567890
	v2 = 1234567890
	e1, _ = encryptFunc(v1)
	e2, _ = encryptFunc(v2)
	sum, err = e.homAdd(e1.([]byte), e2.([]byte))

	if err != nil {
		t.Errorf("Could not add numbers!!")
	}
	d1, _ = decryptFunc(sum)

	//convert to int
	d1Byte = make([]byte, 8)
	copy(d1Byte[8-len(d1.([]byte)):], d1.([]byte))
	d1Int = binary.BigEndian.Uint64(d1Byte)

	if v1+v2 != int64(d1Int) {
		t.Errorf("Expected a different sum! got %v != %v", int64(d1Int), v1+v2)
	}
}
