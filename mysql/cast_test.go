package mysql

import (
	"testing"
)

func TestCAST(t *testing.T) {
	assertSerialize(t, CAST(Float(11.22)).AS("bigint"), `CAST(? AS bigint)`)
	assertSerialize(t, CAST(Int(22)).AS_CHAR(), `CAST(? AS CHAR)`)
	assertSerialize(t, CAST(Int(22)).AS_CHAR(10), `CAST(? AS CHAR(10))`)
	assertSerialize(t, CAST(Int(22)).AS_DATE(), `CAST(? AS DATE)`)
	assertSerialize(t, CAST(Int(22)).AS_DECIMAL(), `CAST(? AS DECIMAL)`)
	assertSerialize(t, CAST(Int(22)).AS_TIME(), `CAST(? AS TIME)`)
	assertSerialize(t, CAST(Int(22)).AS_DATETIME(), `CAST(? AS DATETIME)`)
	assertSerialize(t, CAST(Int(22)).AS_SIGNED(), `CAST(? AS SIGNED)`)
	assertSerialize(t, CAST(Int(22)).AS_UNSIGNED(), `CAST(? AS UNSIGNED)`)
	assertSerialize(t, CAST(Int(22)).AS_BINARY(), `CAST(? AS BINARY)`)
}
