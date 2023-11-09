package godb

type EncryptionScheme struct {
	EncryptMethods map[string](func(v any) (any, error))
	DefaultEncrypt func(v any) (any, error)
}

func (e *EncryptionScheme) encryptTuple(t *Tuple) (*Tuple, error) {
	fields := make([]DBValue, len(t.Fields))
	for i := 0; i < len(t.Desc.Fields); i++ {
		method, exists := e.EncryptMethods[t.Desc.Fields[i].Fname]
		var encrypt func(v any) (any, error)
		if exists {
			encrypt = method
		} else {
			encrypt = e.DefaultEncrypt
		}

		if t.Desc.Fields[i].Ftype == StringType {
			encryptedField, err := encrypt(t.Fields[i].(StringField).Value)
			if err != nil {
				return nil, err
			}
			fields[i] = StringField{Value: encryptedField.(string)}
		} else if t.Desc.Fields[i].Ftype == IntType {
			encryptedField, err := encrypt(t.Fields[i].(IntField).Value)
			if err != nil {
				return nil, err
			}
			fields[i] = IntField{Value: encryptedField.(int64)}
		}
	}
	return &Tuple{Desc: t.Desc, Fields: fields}, nil
}

// func (e *EncryptionScheme) encrypt(hf *HeapFile) (*HeapFile, error) {

// }
