package godb

type EncryptionScheme struct {
	EncryptMethods map[string](func(v any) (any, error))
	DefaultEncrypt func(v any) (any, error)
	DecryptMethods map[string](func(v any) (any, error))
	DefaultDecrypt func(v any) (any, error)
}

func (e *EncryptionScheme) getMethod(fname string, encrypt bool) func(v any) (any, error) {
	if encrypt {
		method, exists := e.EncryptMethods[fname]
		if exists {
			return method
		} else {
			return e.DefaultEncrypt
		}
	} else {
		method, exists := e.DecryptMethods[fname]
		if exists {
			return method
		} else {
			return e.DefaultDecrypt
		}
	}
}

func (e *EncryptionScheme) encryptOrDecryptTuple(t *Tuple, encrypt bool) (*Tuple, error) {
	fields := make([]DBValue, len(t.Fields))
	for i := 0; i < len(t.Desc.Fields); i++ {
		method := e.getMethod(t.Desc.Fields[i].Fname, encrypt)

		if t.Desc.Fields[i].Ftype == StringType {
			encryptedField, err := method(t.Fields[i].(StringField).Value)
			if err != nil {
				return nil, err
			}
			fields[i] = StringField{Value: encryptedField.(string)}
		} else if t.Desc.Fields[i].Ftype == IntType {
			encryptedField, err := method(t.Fields[i].(IntField).Value)
			if err != nil {
				return nil, err
			}
			fields[i] = IntField{Value: encryptedField.(int64)}
		}
	}
	return &Tuple{Desc: t.Desc, Fields: fields}, nil
}

func (e *EncryptionScheme) encrypt(hf *HeapFile, tid TransactionID) (*HeapFile, error) {
	fileName := "encrypted_" + hf.file
	bp := NewBufferPool(3)
	encryptedHf, err := NewHeapFile(fileName, hf.desc, bp)
	if err != nil {
		return nil, err
	}

	iter, _ := hf.Iterator(tid)
	for {
		t, _ := iter()
		if t == nil {
			break
		}
		encryptedTuple, err := e.encryptOrDecryptTuple(t, true)
		if err != nil {
			return nil, err
		}
		encryptedHf.insertTuple(encryptedTuple, tid)
	}

	return encryptedHf, nil
}
