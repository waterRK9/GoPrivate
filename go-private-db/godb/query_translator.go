package godb

import (
	"github.com/getamis/alice/crypto/homo"
	"github.com/getamis/alice/crypto/homo/paillier"
	"github.com/xwb1989/sqlparser"
)

func translateQuery(sql string) (error, EncryptionScheme) {
	encryptMethods := make(map[string]func(v any) (any, error))
	decryptMethods := make(map[string]func(v any) (any, error))
	paillierMap := make(map[string](*(paillier.Paillier)))
	publicKeys := make(map[string](*homo.Pubkey))
	intFieldEncryptedAsStringField := make(map[string]bool)

	defaultEncrypt := func(v any) (any, error) {
		return v, nil
	}

	key := []byte("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	detEncryptFunc := newDetEncryptionFunc(key)
	detDecryptFunc := newDetDecryptionFunc(key)

	keySize := 2048
	homEncryptFunc, homDecryptFunc, publicKey := newHomEncryptionFunc(keySize)

	e := EncryptionScheme{
		EncryptMethods:                 encryptMethods,
		DefaultEncrypt:                 detEncryptFunc,
		DecryptMethods:                 decryptMethods,
		DefaultDecrypt:                 detDecryptFunc,
		PaillierMap:                    paillierMap,
		PublicKeys:                     publicKeys,
		IntFieldEncryptedAsStringField: intFieldEncryptedAsStringField,
	}

	bp := NewBufferPool(10)
	c, err := NewCatalogFromFile("patients_catalog.txt", bp, "./")

	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return err, e
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		plan, _ := parseStatement(c, stmt)
		aggs := plan.aggs
		for _, agg := range aggs {
			switch aggType := *(agg.funcOp); aggType {
			case "avg":
				e.EncryptMethods[agg.field] = homEncryptFunc
				e.DecryptMethods[agg.field] = homDecryptFunc
				e.PublicKeys[agg.field] = &publicKey
				e.EncryptMethods["count"] = defaultEncrypt
				e.DecryptMethods["sum"] = homDecryptFunc
				e.DecryptMethods["count"] = defaultEncrypt
				e.PublicKeys["sum"] = &publicKey
				e.IntFieldEncryptedAsStringField["sum"] = true
				e.IntFieldEncryptedAsStringField[agg.field] = true

			case "sum":
				e.EncryptMethods[agg.field] = homEncryptFunc
				e.DecryptMethods[agg.field] = homDecryptFunc
				e.PublicKeys[agg.field] = &publicKey
				e.DecryptMethods["sum"] = homDecryptFunc
				e.PublicKeys["sum"] = &publicKey
				e.IntFieldEncryptedAsStringField["sum"] = true
				e.IntFieldEncryptedAsStringField[agg.field] = true

			case "count":
				e.EncryptMethods[agg.field] = detEncryptFunc
				e.DecryptMethods[agg.field] = detDecryptFunc
				e.PublicKeys[agg.field] = &publicKey
				e.EncryptMethods["count"] = defaultEncrypt
				e.DecryptMethods["count"] = defaultEncrypt
				e.PublicKeys["count"] = &publicKey
			}
		}
	}
	return nil, e
}
