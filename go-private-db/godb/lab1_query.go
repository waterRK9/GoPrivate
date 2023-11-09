package godb

import (
	"errors"
	"os"
)

// This function should load the csv file in fileName into a heap file (see
// [HeapFile.LoadFromCSV]) and then compute the sum of the integer field in
// string and return its value as an int The supplied csv file is comma
// delimited and has a header If the file doesn't exist or can't be opened, or
// the field doesn't exist, or the field is not and integer, should return an
// err. Note that when you create a HeapFile, you will need to supply a file
// name;  you can supply a non-existant file, in which case it will be created.
// However, subsequent invocations of this method will result in tuples being
// reinserted into this file unless you delete (e.g., with [os.Remove] it before
// calling NewHeapFile.
func computeFieldSum(fileName string, td TupleDesc, sumField string) (int, error) {
	fieldIndex := 0
	for i := 0; i < len(td.Fields); i++ {
		if td.Fields[i].Fname == sumField {
			if td.Fields[i].Ftype != IntType {
				return 0, errors.New("Field type is not integer")
			}
			fieldIndex = i
			break
		}
	}

	fromFile := "lab1.dat"
	bp := NewBufferPool(3)
	hf, err := NewHeapFile(fromFile, &td, bp)
	if err != nil {
		os.Remove(fromFile)
		return 0, err
	}
	file, err := os.Open(fileName)
	if err != nil {
		os.Remove(fromFile)
		return 0, err
	}
	err = hf.LoadFromCSV(file, true, ",", false)
	if err != nil {
		os.Remove(fromFile)
		return 0, err
	}
	tid := NewTID()
	iter, err := hf.Iterator(tid)
	sum := 0
	for {
		t, _ := iter()
		if t == nil {
			break
		}
		field, isInt := t.Fields[fieldIndex].(IntField)
		if !isInt {
			os.Remove(fromFile)
			return 0, errors.New("Field is not type int")
		}
		sum += int(field.Value)
	}

	err = os.Remove(fromFile)
	if err != nil {
		return 0, err
	}
	return sum, nil
}
