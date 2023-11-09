package godb

type EncryptionScheme struct {
	ApplyInt    func(v int64) (int64, error)
	ApplyString func(v string) (string, error)
}

func (e *EncryptionScheme) encryptTuple(t *Tuple) (*Tuple, error) {
	fields := make([]DBValue, len(t.Fields))
	for i := 0; i < len(t.Desc.Fields); i++ {
		if t.Desc.Fields[i].Ftype == StringType {
			encryptedField, err := e.ApplyString(t.Fields[i].(StringField).Value)
			if err != nil {
				return nil, err
			}
			fields[i] = StringField{Value: encryptedField}
		} else if t.Desc.Fields[i].Ftype == IntType {
			encryptedField, err := e.ApplyInt(t.Fields[i].(IntField).Value)
			if err != nil {
				return nil, err
			}
			fields[i] = IntField{Value: encryptedField}
		}
	}
	return &Tuple{Desc: t.Desc, Fields: fields}, nil
}

// func (e *EncryptionScheme) encrypt(hf *HeapFile) (*HeapFile, error) {

// }
