package godb

import "testing"

func TestTranslation(t *testing.T) {
	translateQuery("select sum(age + 10) , sum(age) from t")
}
