package sqlbuilder

import (
	"gotest.tools/assert"
	"testing"
)

func TestFuncABS(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		assert.Equal(t, getTestSerialize(t, ABSf(table1ColFloat)), "ABS(table1.colFloat)")
		assert.Equal(t, getTestSerialize(t, ABSf(Float(11.2222))), "ABS($1)")
	})

	t.Run("integer", func(t *testing.T) {
		assert.Equal(t, getTestSerialize(t, ABSi(table1ColInt)), "ABS(table1.colInt)")
		assert.Equal(t, getTestSerialize(t, ABSi(Int(11))), "ABS($1)")
	})
}

func TestFuncSQRT(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		assert.Equal(t, getTestSerialize(t, SQRTf(table1ColFloat)), "SQRT(table1.colFloat)")
		assert.Equal(t, getTestSerialize(t, SQRTf(Float(11.2222))), "SQRT($1)")
	})

	t.Run("integer", func(t *testing.T) {
		assert.Equal(t, getTestSerialize(t, SQRTi(table1ColInt)), "SQRT(table1.colInt)")
		assert.Equal(t, getTestSerialize(t, SQRTi(Int(11))), "SQRT($1)")
	})
}

func TestFuncCBRT(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		assert.Equal(t, getTestSerialize(t, CBRTf(table1ColFloat)), "CBRT(table1.colFloat)")
		assert.Equal(t, getTestSerialize(t, CBRTf(Float(11.2222))), "CBRT($1)")
	})

	t.Run("integer", func(t *testing.T) {
		assert.Equal(t, getTestSerialize(t, CBRTi(table1ColInt)), "CBRT(table1.colInt)")
		assert.Equal(t, getTestSerialize(t, CBRTi(Int(11))), "CBRT($1)")
	})
}

func TestFuncMAX(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		assert.Equal(t, getTestSerialize(t, MAXf(table1ColFloat)), "MAX(table1.colFloat)")
		assert.Equal(t, getTestSerialize(t, MAXf(Float(11.2222))), "MAX($1)")
	})

	t.Run("integer", func(t *testing.T) {
		assert.Equal(t, getTestSerialize(t, MAXi(table1ColInt)), "MAX(table1.colInt)")
		assert.Equal(t, getTestSerialize(t, MAXi(Int(11))), "MAX($1)")
	})
}

func TestFuncSUM(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		assert.Equal(t, getTestSerialize(t, SUMf(table1ColFloat)), "SUM(table1.colFloat)")
		assert.Equal(t, getTestSerialize(t, SUMf(Float(11.2222))), "SUM($1)")
	})

	t.Run("integer", func(t *testing.T) {
		assert.Equal(t, getTestSerialize(t, SUMi(table1ColInt)), "SUM(table1.colInt)")
		assert.Equal(t, getTestSerialize(t, SUMi(Int(11))), "SUM($1)")
	})
}

func TestFuncCOUNT(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		assert.Equal(t, getTestSerialize(t, COUNTf(table1ColFloat)), "COUNT(table1.colFloat)")
		assert.Equal(t, getTestSerialize(t, COUNTf(Float(11.2222))), "COUNT($1)")
	})

	t.Run("integer", func(t *testing.T) {
		assert.Equal(t, getTestSerialize(t, COUNTi(table1ColInt)), "COUNT(table1.colInt)")
		assert.Equal(t, getTestSerialize(t, COUNTi(Int(11))), "COUNT($1)")
	})
}

func TestFuncCEIL(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, CEIL(table1ColFloat)), "CEIL(table1.colFloat)")
	assert.Equal(t, getTestSerialize(t, CEIL(Float(11.2222))), "CEIL($1)")
}

func TestFuncFLOOR(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, FLOOR(table1ColFloat)), "FLOOR(table1.colFloat)")
	assert.Equal(t, getTestSerialize(t, FLOOR(Float(11.2222))), "FLOOR($1)")
}

func TestFuncROUND(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, ROUND(table1ColFloat)), "ROUND(table1.colFloat)")
	assert.Equal(t, getTestSerialize(t, ROUND(Float(11.2222))), "ROUND($1)")

	assert.Equal(t, getTestSerialize(t, ROUND(table1ColFloat, Int(2))), "ROUND(table1.colFloat, $1)")
	assert.Equal(t, getTestSerialize(t, ROUND(Float(11.2222), Int(1))), "ROUND($1, $2)")
}

func TestFuncSIGN(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, SIGN(table1ColFloat)), "SIGN(table1.colFloat)")
	assert.Equal(t, getTestSerialize(t, SIGN(Float(11.2222))), "SIGN($1)")
}

func TestFuncTRUNC(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, TRUNC(table1ColFloat)), "TRUNC(table1.colFloat)")
	assert.Equal(t, getTestSerialize(t, TRUNC(Float(11.2222))), "TRUNC($1)")

	assert.Equal(t, getTestSerialize(t, TRUNC(table1ColFloat, Int(2))), "TRUNC(table1.colFloat, $1)")
	assert.Equal(t, getTestSerialize(t, TRUNC(Float(11.2222), Int(1))), "TRUNC($1, $2)")
}

func TestFuncLN(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, LN(table1ColFloat)), "LN(table1.colFloat)")
	assert.Equal(t, getTestSerialize(t, LN(Float(11.2222))), "LN($1)")
}

func TestFuncLOG(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, LOG(table1ColFloat)), "LOG(table1.colFloat)")
	assert.Equal(t, getTestSerialize(t, LOG(Float(11.2222))), "LOG($1)")
}

func TestCase1(t *testing.T) {
	query := CASE().
		WHEN(table3Col1.EQ(Int(1))).THEN(table3Col1.ADD(Int(1))).
		WHEN(table3Col1.EQ(Int(2))).THEN(table3Col1.ADD(Int(2)))

	queryData := &queryData{}

	err := query.serialize(select_statement, queryData)

	assert.NilError(t, err)
	assert.Equal(t, queryData.buff.String(), `(CASE WHEN table3.col1 = $1 THEN table3.col1 + $2 WHEN table3.col1 = $3 THEN table3.col1 + $4 END)`)
}

func TestCase2(t *testing.T) {
	query := CASE(table3Col1).
		WHEN(Int(1)).THEN(table3Col1.ADD(Int(1))).
		WHEN(Int(2)).THEN(table3Col1.ADD(Int(2))).
		ELSE(Int(0))

	queryData := &queryData{}

	err := query.serialize(select_statement, queryData)

	assert.NilError(t, err)
	assert.Equal(t, queryData.buff.String(), `(CASE table3.col1 WHEN $1 THEN table3.col1 + $2 WHEN $3 THEN table3.col1 + $4 ELSE $5 END)`)
}

func TestInterval(t *testing.T) {
	query := INTERVAL(`6 years 5 months 4 days 3 hours 2 minutes 1 second`)

	queryData := &queryData{}

	err := query.serialize(select_statement, queryData)

	assert.NilError(t, err)
	assert.Equal(t, queryData.buff.String(), `INTERVAL $1`)
}
