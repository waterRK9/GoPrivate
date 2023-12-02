package godb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/getamis/alice/crypto/homo/paillier"
	"github.com/tink-crypto/tink-go/daead/subtle"
)

type EncryptionScheme struct {
	EncryptMethods map[string](func(v any) (any, error))
	DefaultEncrypt func(v any) (any, error)
	DecryptMethods map[string](func(v any) (any, error))
	DefaultDecrypt func(v any) (any, error)

	PaillierMap map[string](*(paillier.Paillier))
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

func (e *EncryptionScheme) encryptOrDecrypt(hf *HeapFile, toFile string, encrypt bool, tid TransactionID) (*HeapFile, error) {
	bp := NewBufferPool(3)
	_hf, err := NewHeapFile(toFile, hf.desc, bp)
	if err != nil {
		return nil, err
	}

	iter, _ := hf.Iterator(tid)
	for {
		t, _ := iter()
		if t == nil {
			break
		}
		encryptedTuple, err := e.encryptOrDecryptTuple(t, encrypt)
		if err != nil {
			return nil, err
		}
		_hf.insertTuple(encryptedTuple, tid)
	}

	return _hf, nil
}

func newDetEncryptionFunc(key []byte) func(v any) (any, error) {
	aessiv, err := subtle.NewAESSIV(key)
	if err != nil {
		panic(err)
	}
	aad := []byte("")

	return func(v any) (any, error) {
		if intValue, ok := v.(int64); ok {
			buf := []byte(strconv.Itoa(int(intValue)))
			result, err := aessiv.EncryptDeterministically(buf, aad)

			// TODO: int64 after encryption becomes array with more than 8 bytes, meaning it cannot become an int64 again
			if err != nil {
				return nil, err
			} else {
				return string(result), nil
			}
		} else if stringValue, ok := v.(string); ok {
			buf := []byte(stringValue)
			result, err := aessiv.EncryptDeterministically(buf, aad)

			// convert back to string
			if err != nil {
				return nil, err
			} else {
				return string(result), nil
			}
		} else {
			panic("cannot encrypt unsupported type!")
		}
	}
}

func newDetDecryptionFunc(key []byte) func(v any) (any, error) {
	aessiv, err := subtle.NewAESSIV(key)
	if err != nil {
		panic("issue with generating decryption function")
	}
	aad := []byte("")

	return func(v any) (any, error) {
		if stringValue, ok := v.(string); ok {
			buf := []byte(stringValue)
			result, err := aessiv.DecryptDeterministically(buf, aad)

			// convert back to string
			if err != nil {
				return nil, err
			} else {
				return string(result), nil
			}
		} else {
			panic("cannot decrypt unsupported type!")
		}
	}
}

func (e *EncryptionScheme) newHomEncryptionFunc(keysize int) func(v any) (any, error) {
	pall, err := paillier.NewPaillier(keysize)
	if err != nil {
		panic(err)
	}

	return func(v any) (any, error) {
		if intValue, ok := v.(int64); ok {

			buf := new(bytes.Buffer)
			err := binary.Write(buf, binary.BigEndian, intValue)
			if err != nil {
				fmt.Println("binary.Write failed:", err)
			}
			byts := buf.Bytes()

			result, err := pall.Encrypt(byts)
			e.PaillierMap[string(result)] = pall

			if err != nil {
				return nil, err
			} else {
				return result, nil
			}
		} else {
			panic("cannot encrypt unsupported type!")
		}
	}
}

func (e *EncryptionScheme) newHomDecryptionFunc() func(v any) (any, error) {

	return func(v any) (any, error) {
		pall, exists := e.PaillierMap[string(v.([]byte))]

		if !exists {
			panic("cannot decrypt unencrypted type!")
		}

		result, err := pall.Decrypt(v.([]byte))

		if err != nil {
			return nil, err
		} else {
			return result, nil
		}
	}
}

func (e *EncryptionScheme) homAdd(v1 []byte, v2 []byte) ([]byte, error) {
	pall1, exists := e.PaillierMap[string(v1)]
	if !exists {
		panic("cannot add unencrypted type!")
	}

	pall2, exists := e.PaillierMap[string(v2)]
	if !exists {
		panic("cannot add unencrypted type!")
	}

	if pall1 != pall2 {
		panic("cannot add encrypted types with different schemes!")
	}

	sum, _ := pall1.Add(v1, v2)
	e.PaillierMap[string(sum)] = pall1

	return sum, nil
}
