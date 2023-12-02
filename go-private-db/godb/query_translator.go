package godb

import (
	"github.com/getamis/alice/crypto/homo/paillier"
	"github.com/xwb1989/sqlparser"
)

func translateQuery(sql string) (error, EncryptionScheme) {
	encryptMethods := make(map[string]func(v any) (any, error))
	decryptMethods := make(map[string]func(v any) (any, error))
	defaultEncrypt := func(v any) (any, error) {
		return v, nil
	}
	paillierMap := make(map[string](*(paillier.Paillier)))
	e := EncryptionScheme{
		EncryptMethods: encryptMethods,
		DefaultEncrypt: defaultEncrypt,
		DecryptMethods: decryptMethods,
		DefaultDecrypt: defaultEncrypt,
		PaillierMap:    paillierMap,
	}

	keySize := 2048
	homEncryptFunc := e.newHomEncryptionFunc(keySize)
	homDecryptFunc := e.newHomDecryptionFunc()

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
		}
	}
	return nil, e
}
