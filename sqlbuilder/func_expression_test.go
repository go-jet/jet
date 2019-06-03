package sqlbuilder

import (
	"gotest.tools/assert"
	"testing"
)

func TestFuncABS(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		assertExpressionSerialize(t, ABSf(table1ColFloat), "ABS(table1.colFloat)")
		assertExpressionSerialize(t, ABSf(Float(11.2222)), "ABS($1)", float64(11.2222))
	})

	t.Run("integer", func(t *testing.T) {
		assertExpressionSerialize(t, ABSi(table1ColInt), "ABS(table1.colInt)")
		assertExpressionSerialize(t, ABSi(Int(11)), "ABS($1)", int64(11))
	})
}

func TestFuncSQRT(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		assertExpressionSerialize(t, SQRTf(table1ColFloat), "SQRT(table1.colFloat)")
		assertExpressionSerialize(t, SQRTf(Float(11.2222)), "SQRT($1)", float64(11.2222))
	})

	t.Run("integer", func(t *testing.T) {
		assertExpressionSerialize(t, SQRTi(table1ColInt), "SQRT(table1.colInt)")
		assertExpressionSerialize(t, SQRTi(Int(11)), "SQRT($1)", int64(11))
	})
}

func TestFuncCBRT(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		assertExpressionSerialize(t, CBRTf(table1ColFloat), "CBRT(table1.colFloat)")
		assertExpressionSerialize(t, CBRTf(Float(11.2222)), "CBRT($1)", float64(11.2222))
	})

	t.Run("integer", func(t *testing.T) {
		assertExpressionSerialize(t, CBRTi(table1ColInt), "CBRT(table1.colInt)")
		assertExpressionSerialize(t, CBRTi(Int(11)), "CBRT($1)", int64(11))
	})
}

func TestFuncMAX(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		assertExpressionSerialize(t, MAXf(table1ColFloat), "MAX(table1.colFloat)")
		assertExpressionSerialize(t, MAXf(Float(11.2222)), "MAX($1)", float64(11.2222))
	})

	t.Run("integer", func(t *testing.T) {
		assertExpressionSerialize(t, MAXi(table1ColInt), "MAX(table1.colInt)")
		assertExpressionSerialize(t, MAXi(Int(11)), "MAX($1)", int64(11))
	})
}

func TestFuncSUM(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		assertExpressionSerialize(t, SUMf(table1ColFloat), "SUM(table1.colFloat)")
		assertExpressionSerialize(t, SUMf(Float(11.2222)), "SUM($1)", float64(11.2222))
	})

	t.Run("integer", func(t *testing.T) {
		assertExpressionSerialize(t, SUMi(table1ColInt), "SUM(table1.colInt)")
		assertExpressionSerialize(t, SUMi(Int(11)), "SUM($1)", int64(11))
	})
}

func TestFuncCOUNT(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		assertExpressionSerialize(t, COUNTf(table1ColFloat), "COUNT(table1.colFloat)")
		assertExpressionSerialize(t, COUNTf(Float(11.2222)), "COUNT($1)", float64(11.2222))
	})

	t.Run("integer", func(t *testing.T) {
		assertExpressionSerialize(t, COUNTi(table1ColInt), "COUNT(table1.colInt)")
		assertExpressionSerialize(t, COUNTi(Int(11)), "COUNT($1)", int64(11))
	})
}

func TestFuncCEIL(t *testing.T) {
	assertExpressionSerialize(t, CEIL(table1ColFloat), "CEIL(table1.colFloat)")
	assertExpressionSerialize(t, CEIL(Float(11.2222)), "CEIL($1)", float64(11.2222))
}

func TestFuncFLOOR(t *testing.T) {
	assertExpressionSerialize(t, FLOOR(table1ColFloat), "FLOOR(table1.colFloat)")
	assertExpressionSerialize(t, FLOOR(Float(11.2222)), "FLOOR($1)", float64(11.2222))
}

func TestFuncROUND(t *testing.T) {
	assertExpressionSerialize(t, ROUND(table1ColFloat), "ROUND(table1.colFloat)")
	assertExpressionSerialize(t, ROUND(Float(11.2222)), "ROUND($1)", float64(11.2222))

	assertExpressionSerialize(t, ROUND(table1ColFloat, Int(2)), "ROUND(table1.colFloat, $1)", int64(2))
	assertExpressionSerialize(t, ROUND(Float(11.2222), Int(1)), "ROUND($1, $2)", float64(11.2222), int64(1))
}

func TestFuncSIGN(t *testing.T) {
	assertExpressionSerialize(t, SIGN(table1ColFloat), "SIGN(table1.colFloat)")
	assertExpressionSerialize(t, SIGN(Float(11.2222)), "SIGN($1)", float64(11.2222))
}

func TestFuncTRUNC(t *testing.T) {
	assertExpressionSerialize(t, TRUNC(table1ColFloat), "TRUNC(table1.colFloat)")
	assertExpressionSerialize(t, TRUNC(Float(11.2222)), "TRUNC($1)", float64(11.2222))

	assertExpressionSerialize(t, TRUNC(table1ColFloat, Int(2)), "TRUNC(table1.colFloat, $1)", int64(2))
	assertExpressionSerialize(t, TRUNC(Float(11.2222), Int(1)), "TRUNC($1, $2)", float64(11.2222), int64(1))
}

func TestFuncLN(t *testing.T) {
	assertExpressionSerialize(t, LN(table1ColFloat), "LN(table1.colFloat)")
	assertExpressionSerialize(t, LN(Float(11.2222)), "LN($1)", float64(11.2222))
}

func TestFuncLOG(t *testing.T) {
	assertExpressionSerialize(t, LOG(table1ColFloat), "LOG(table1.colFloat)")
	assertExpressionSerialize(t, LOG(Float(11.2222)), "LOG($1)", float64(11.2222))
}

func TestFuncCOALESCE(t *testing.T) {
	assertExpressionSerialize(t, COALESCE(table1ColFloat), "COALESCE(table1.colFloat)")
	assertExpressionSerialize(t, COALESCE(Float(11.2222), NULL, String("str")), "COALESCE($1, NULL, $2)", float64(11.2222), "str")
}

func TestFuncNULLIF(t *testing.T) {
	assertExpressionSerialize(t, NULLIF(table1ColFloat, table2ColInt), "NULLIF(table1.colFloat, table2.colInt)")
	assertExpressionSerialize(t, NULLIF(Float(11.2222), NULL), "NULLIF($1, NULL)", float64(11.2222))
}

func TestFuncGREATEST(t *testing.T) {
	assertExpressionSerialize(t, GREATEST(table1ColFloat), "GREATEST(table1.colFloat)")
	assertExpressionSerialize(t, GREATEST(Float(11.2222), NULL, String("str")), "GREATEST($1, NULL, $2)", float64(11.2222), "str")
}

func TestFuncLEAST(t *testing.T) {
	assertExpressionSerialize(t, LEAST(table1ColFloat), "LEAST(table1.colFloat)")
	assertExpressionSerialize(t, LEAST(Float(11.2222), NULL, String("str")), "LEAST($1, NULL, $2)", float64(11.2222), "str")
}

func TestInterval(t *testing.T) {
	query := INTERVAL(`6 years 5 months 4 days 3 hours 2 minutes 1 second`)

	queryData := &queryData{}

	err := query.serialize(select_statement, queryData)

	assert.NilError(t, err)
	assert.Equal(t, queryData.buff.String(), `INTERVAL $1`)
}
