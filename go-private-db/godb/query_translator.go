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

	key := []byte("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	detEncryptFunc := newDetEncryptionFunc(key)
	detDecryptFunc := newDetDecryptionFunc(key)

	keySize := 2048
	homEncryptFunc, homDecryptFunc, publicKey := newHomEncryptionFunc(keySize)

	e := EncryptionScheme{
		EncryptMethods: encryptMethods,
		DefaultEncrypt: detEncryptFunc,
		DecryptMethods: decryptMethods,
		DefaultDecrypt: detDecryptFunc,
		PaillierMap:    paillierMap,
		PublicKeys:     publicKeys,
	}

	bp := NewBufferPool(10)
	c, err := NewCatalogFromFile("catalog.txt", bp, "./")

	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return err, e
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		plan, _ := parseStatement(c, stmt)
		aggs := plan.aggs
		for _, agg := range aggs {
			e.EncryptMethods[agg.field] = homEncryptFunc
			e.DecryptMethods[agg.field] = homDecryptFunc
			e.PublicKeys[agg.field] = &publicKey
		}
	}
	return nil, e
}
