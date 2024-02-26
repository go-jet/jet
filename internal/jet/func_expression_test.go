package jet

import (
	"testing"
)

func TestAND(t *testing.T) {
	assertClauseSerializeErr(t, AND(), "jet: syntax error, expression list empty")
	assertClauseSerialize(t, AND(table1ColInt.IS_NULL()), `table1.col_int IS NULL`) // IS NULL doesn't add parenthesis
	assertClauseSerialize(t, AND(table1ColInt.LT(Int(11))), `(table1.col_int < $1)`, int64(11))
	assertClauseSerialize(t, AND(table1ColInt.GT(Int(11)), table1ColFloat.EQ(Float(0))),
		`(
    (table1.col_int > $1)
        AND (table1.col_float = $2)
)`, int64(11), 0.0)
}

func TestOR(t *testing.T) {
	assertClauseSerializeErr(t, OR(), "jet: syntax error, expression list empty")
	assertClauseSerialize(t, OR(table1ColInt.IS_NULL()), `table1.col_int IS NULL`) // IS NULL doesn't add parenthesis
	assertClauseSerialize(t, OR(table1ColInt.LT(Int(11))), `(table1.col_int < $1)`, int64(11))
	assertClauseSerialize(t, OR(table1ColInt.GT(Int(11)), table1ColFloat.EQ(Float(0))),
		`(
    (table1.col_int > $1)
        OR (table1.col_float = $2)
)`, int64(11), 0.0)
}

func TestFuncAVG(t *testing.T) {
	assertClauseSerialize(t, AVG(table1ColFloat), "AVG(table1.col_float)")
	assertClauseSerialize(t, AVG(table1ColInt), "AVG(table1.col_int)")
}

func TestFuncBIT_AND(t *testing.T) {
	assertClauseSerialize(t, BIT_AND(table1ColInt), "BIT_AND(table1.col_int)")
}

func TestFuncBIT_OR(t *testing.T) {
	assertClauseSerialize(t, BIT_OR(table1ColInt), "BIT_OR(table1.col_int)")
}

func TestFuncBOOL_AND(t *testing.T) {
	assertClauseSerialize(t, BOOL_AND(table1ColBool), "BOOL_AND(table1.col_bool)")
}

func TestFuncBOOL_OR(t *testing.T) {
	assertClauseSerialize(t, BOOL_OR(table1ColBool), "BOOL_OR(table1.col_bool)")
}

func TestFuncEVERY(t *testing.T) {
	assertClauseSerialize(t, EVERY(table1ColBool), "EVERY(table1.col_bool)")
}

func TestFuncMIN(t *testing.T) {
	t.Run("expression", func(t *testing.T) {
		assertClauseSerialize(t, MIN(table1ColDate), "MIN(table1.col_date)")
		assertClauseSerialize(t, MIN(Date(2001, 1, 1)), "MIN($1)", "2001-01-01")
		assertClauseSerialize(t, MIN(Time(12, 10, 10)), "MIN($1)", "12:10:10")
		assertClauseSerialize(t, MIN(Timestamp(2001, 1, 1, 12, 10, 10)), "MIN($1)", "2001-01-01 12:10:10")
		assertClauseSerialize(t, MIN(Timestampz(2001, 1, 1, 12, 10, 10, 1, "UTC")), "MIN($1)", "2001-01-01 12:10:10.000000001 UTC")
	})

	t.Run("float", func(t *testing.T) {
		assertClauseSerialize(t, MINf(table1ColFloat), "MIN(table1.col_float)")
	})

	t.Run("integer", func(t *testing.T) {
		assertClauseSerialize(t, MINi(table1ColInt), "MIN(table1.col_int)")
	})
}

func TestFuncMAX(t *testing.T) {
	t.Run("expression", func(t *testing.T) {
		assertClauseSerialize(t, MAX(table1ColDate), "MAX(table1.col_date)")
		assertClauseSerialize(t, MAX(Date(2001, 1, 1)), "MAX($1)", "2001-01-01")
		assertClauseSerialize(t, MAX(Time(12, 10, 10)), "MAX($1)", "12:10:10")
		assertClauseSerialize(t, MAX(Timestamp(2001, 1, 1, 12, 10, 10)), "MAX($1)", "2001-01-01 12:10:10")
		assertClauseSerialize(t, MAX(Timestampz(2001, 1, 1, 12, 10, 10, 1, "UTC")), "MAX($1)", "2001-01-01 12:10:10.000000001 UTC")
	})

	t.Run("float", func(t *testing.T) {
		assertClauseSerialize(t, MAXf(table1ColFloat), "MAX(table1.col_float)")
		assertClauseSerialize(t, MAXf(Float(11.2222)), "MAX($1)", float64(11.2222))
	})

	t.Run("integer", func(t *testing.T) {
		assertClauseSerialize(t, MAXi(table1ColInt), "MAX(table1.col_int)")
		assertClauseSerialize(t, MAXi(Int(11)), "MAX($1)", int64(11))
	})
}

func TestFuncSUM(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		assertClauseSerialize(t, SUMf(table1ColFloat), "SUM(table1.col_float)")
		assertClauseSerialize(t, SUMf(Float(11.2222)), "SUM($1)", float64(11.2222))
	})

	t.Run("integer", func(t *testing.T) {
		assertClauseSerialize(t, SUMi(table1ColInt), "SUM(table1.col_int)")
		assertClauseSerialize(t, SUMi(Int(11)), "SUM($1)", int64(11))
	})
}

func TestFuncCOUNT(t *testing.T) {
	assertClauseSerialize(t, COUNT(STAR), "COUNT(*)")
	assertClauseSerialize(t, COUNT(table1ColFloat), "COUNT(table1.col_float)")
	assertClauseSerialize(t, COUNT(Float(11.2222)), "COUNT($1)", float64(11.2222))
}

func TestFuncABS(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		assertClauseSerialize(t, ABSf(table1ColFloat), "ABS(table1.col_float)")
		assertClauseSerialize(t, ABSf(Float(11.2222)), "ABS($1)", float64(11.2222))
	})

	t.Run("integer", func(t *testing.T) {
		assertClauseSerialize(t, ABSi(table1ColInt), "ABS(table1.col_int)")
		assertClauseSerialize(t, ABSi(Int(11)), "ABS($1)", int64(11))
	})
}

func TestFuncSQRT(t *testing.T) {
	assertClauseSerialize(t, SQRT(table1ColFloat), "SQRT(table1.col_float)")
	assertClauseSerialize(t, SQRT(Float(11.2222)), "SQRT($1)", float64(11.2222))
	assertClauseSerialize(t, SQRT(table1ColInt), "SQRT(table1.col_int)")
	assertClauseSerialize(t, SQRT(Int(11)), "SQRT($1)", int64(11))
}

func TestFuncCBRT(t *testing.T) {
	assertClauseSerialize(t, CBRT(table1ColFloat), "CBRT(table1.col_float)")
	assertClauseSerialize(t, CBRT(Float(11.2222)), "CBRT($1)", float64(11.2222))
	assertClauseSerialize(t, CBRT(table1ColInt), "CBRT(table1.col_int)")
	assertClauseSerialize(t, CBRT(Int(11)), "CBRT($1)", int64(11))
}

func TestFuncCEIL(t *testing.T) {
	assertClauseSerialize(t, CEIL(table1ColFloat), "CEIL(table1.col_float)")
	assertClauseSerialize(t, CEIL(Float(11.2222)), "CEIL($1)", float64(11.2222))
}

func TestFuncFLOOR(t *testing.T) {
	assertClauseSerialize(t, FLOOR(table1ColFloat), "FLOOR(table1.col_float)")
	assertClauseSerialize(t, FLOOR(Float(11.2222)), "FLOOR($1)", float64(11.2222))
}

func TestFuncROUND(t *testing.T) {
	assertClauseSerialize(t, ROUND(table1ColFloat), "ROUND(table1.col_float)")
	assertClauseSerialize(t, ROUND(Float(11.2222)), "ROUND($1)", float64(11.2222))

	assertClauseSerialize(t, ROUND(table1ColFloat, Int(2)), "ROUND(table1.col_float, $1)", int64(2))
	assertClauseSerialize(t, ROUND(Float(11.2222), Int(1)), "ROUND($1, $2)", float64(11.2222), int64(1))
}

func TestFuncSIGN(t *testing.T) {
	assertClauseSerialize(t, SIGN(table1ColFloat), "SIGN(table1.col_float)")
	assertClauseSerialize(t, SIGN(Float(11.2222)), "SIGN($1)", float64(11.2222))
}

func TestFuncTRUNC(t *testing.T) {
	assertClauseSerialize(t, TRUNC(table1ColFloat), "TRUNC(table1.col_float)")
	assertClauseSerialize(t, TRUNC(Float(11.2222)), "TRUNC($1)", float64(11.2222))

	assertClauseSerialize(t, TRUNC(table1ColFloat, Int(2)), "TRUNC(table1.col_float, $1)", int64(2))
	assertClauseSerialize(t, TRUNC(Float(11.2222), Int(1)), "TRUNC($1, $2)", float64(11.2222), int64(1))
}

func TestFuncLN(t *testing.T) {
	assertClauseSerialize(t, LN(table1ColFloat), "LN(table1.col_float)")
	assertClauseSerialize(t, LN(Float(11.2222)), "LN($1)", float64(11.2222))
}

func TestFuncLOG(t *testing.T) {
	assertClauseSerialize(t, LOG(table1ColFloat), "LOG(table1.col_float)")
	assertClauseSerialize(t, LOG(Float(11.2222)), "LOG($1)", float64(11.2222))
}

func TestFuncCOALESCE(t *testing.T) {
	assertClauseSerialize(t, COALESCE(table1ColFloat), "COALESCE(table1.col_float)")
	assertClauseSerialize(t, COALESCE(Float(11.2222), NULL, String("str")), "COALESCE($1, NULL, $2)", float64(11.2222), "str")
}

func TestFuncNULLIF(t *testing.T) {
	assertClauseSerialize(t, NULLIF(table1ColFloat, table2ColInt), "NULLIF(table1.col_float, table2.col_int)")
	assertClauseSerialize(t, NULLIF(Float(11.2222), NULL), "NULLIF($1, NULL)", float64(11.2222))
}

func TestFuncGREATEST(t *testing.T) {
	assertClauseSerialize(t, GREATEST(table1ColFloat), "GREATEST(table1.col_float)")
	assertClauseSerialize(t, GREATEST(Float(11.2222), NULL, String("str")), "GREATEST($1, NULL, $2)", float64(11.2222), "str")
}

func TestFuncLEAST(t *testing.T) {
	assertClauseSerialize(t, LEAST(table1ColFloat), "LEAST(table1.col_float)")
	assertClauseSerialize(t, LEAST(Float(11.2222), NULL, String("str")), "LEAST($1, NULL, $2)", float64(11.2222), "str")
}

func TestTO_ASCII(t *testing.T) {
	assertClauseSerialize(t, TO_ASCII(String("Karel")), `TO_ASCII($1)`, "Karel")
	assertClauseSerialize(t, TO_ASCII(String("Karel")), `TO_ASCII($1)`, "Karel")
}

func TestFunc(t *testing.T) {
	assertClauseSerialize(t, Func("FOO", String("test"), NULL, MAX(Int(1))), "FOO($1, NULL, MAX($2))", "test", int64(1))
}

func Test_rangePointCaster(t *testing.T) {
	mainRange := Int8Range(Int8(10), Int8(12))
	exp := NewFunc("UPPER", []Expression{mainRange}, nil)

	got := rangeTypeCaster(mainRange, exp)
	_, ok := got.(IntegerExpression)
	if !ok {
		t.Errorf("expecting to get IntegerExpression but got %v", got)
	}
}
