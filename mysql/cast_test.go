package mysql

import (
	"testing"
)

func TestCAST(t *testing.T) {
	assertClauseSerialize(t, CAST(Float(11.22)).AS("bigint"), `CAST(? AS bigint)`)
	assertClauseSerialize(t, CAST(Int(22)).AS_CHAR(), `CAST(? AS CHAR)`)
	assertClauseSerialize(t, CAST(Int(22)).AS_CHAR(10), `CAST(? AS CHAR(10))`)
	assertClauseSerialize(t, CAST(Int(22)).AS_DATE(), `CAST(? AS DATE)`)
	assertClauseSerialize(t, CAST(Int(22)).AS_DECIMAL(), `CAST(? AS DECIMAL)`)
	assertClauseSerialize(t, CAST(Int(22)).AS_TIME(), `CAST(? AS TIME)`)
	assertClauseSerialize(t, CAST(Int(22)).AS_DATETIME(), `CAST(? AS DATETIME)`)
	assertClauseSerialize(t, CAST(Int(22)).AS_SIGNED(), `CAST(? AS SIGNED)`)
	assertClauseSerialize(t, CAST(Int(22)).AS_UNSIGNED(), `CAST(? AS UNSIGNED)`)
	assertClauseSerialize(t, CAST(Int(22)).AS_BINARY(), `CAST(? AS BINARY)`)
}
