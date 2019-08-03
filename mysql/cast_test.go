package mysql

import (
	"testing"
)

func TestCAST_AS_DATE(t *testing.T) {
	assertClauseSerialize(t, CAST(Int(22)).AS_DATE(), `CAST(? AS DATE)`, int64(22))
}
