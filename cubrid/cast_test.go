package cubrid

import (
	"testing"
)

func TestCAST(t *testing.T) {
	assertSerialize(t, CAST(Float(11.22)).AS("bigint"), `CAST(? AS bigint)`)
	assertSerialize(t, CAST(Int(22)).AS_CHAR(), `CAST(? AS CHAR)`)
	assertSerialize(t, CAST(Int(22)).AS_CHAR(10), `CAST(? AS CHAR(10))`)
	assertSerialize(t, CAST(Int(22)).AS_VARCHAR(), `CAST(? AS VARCHAR)`)
	assertSerialize(t, CAST(Int(22)).AS_VARCHAR(255), `CAST(? AS VARCHAR(255))`)
	assertSerialize(t, CAST(Int(22)).AS_DATE(), `CAST(? AS DATE)`)
	assertSerialize(t, CAST(Int(22)).AS_NUMERIC(), `CAST(? AS NUMERIC)`)
	assertSerialize(t, CAST(Int(22)).AS_TIME(), `CAST(? AS TIME)`)
	assertSerialize(t, CAST(Int(22)).AS_DATETIME(), `CAST(? AS DATETIME)`)
	assertSerialize(t, CAST(Int(22)).AS_TIMESTAMP(), `CAST(? AS TIMESTAMP)`)
	assertSerialize(t, CAST(Int(22)).AS_INTEGER(), `CAST(? AS INTEGER)`)
	assertSerialize(t, CAST(Int(22)).AS_BIGINT(), `CAST(? AS BIGINT)`)
	assertSerialize(t, CAST(Int(22)).AS_SMALLINT(), `CAST(? AS SMALLINT)`)
	assertSerialize(t, CAST(Int(22)).AS_FLOAT(), `CAST(? AS FLOAT)`)
	assertSerialize(t, CAST(Int(22)).AS_DOUBLE(), `CAST(? AS DOUBLE)`)
}
