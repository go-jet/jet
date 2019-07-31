package mysql

import (
	"github.com/go-jet/jet"
	"testing"
)

func TestCAST_AS_DATE(t *testing.T) {

	jet.AssertMySQLClauseSerialize(t, CAST(Int(22)).AS_DATE(), `CAST(? AS DATE)`, int64(22))
}
