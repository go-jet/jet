package jet

import (
	"testing"
)

func TestFuncAVG(t *testing.T) {
	assertPostgreClauseSerialize(t, AVG(table1ColFloat), "AVG(table1.col_float)")
	assertPostgreClauseSerialize(t, AVG(table1ColInt), "AVG(table1.col_int)")
}

func TestFuncBIT_AND(t *testing.T) {
	assertPostgreClauseSerialize(t, BIT_AND(table1ColInt), "BIT_AND(table1.col_int)")
}

func TestFuncBIT_OR(t *testing.T) {
	assertPostgreClauseSerialize(t, BIT_OR(table1ColInt), "BIT_OR(table1.col_int)")
}

func TestFuncBOOL_AND(t *testing.T) {
	assertPostgreClauseSerialize(t, BOOL_AND(table1ColBool), "BOOL_AND(table1.col_bool)")
}

func TestFuncBOOL_OR(t *testing.T) {
	assertPostgreClauseSerialize(t, BOOL_OR(table1ColBool), "BOOL_OR(table1.col_bool)")
}

func TestFuncEVERY(t *testing.T) {
	assertPostgreClauseSerialize(t, EVERY(table1ColBool), "EVERY(table1.col_bool)")
}

func TestFuncMIN(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		assertPostgreClauseSerialize(t, MINf(table1ColFloat), "MIN(table1.col_float)")
	})

	t.Run("integer", func(t *testing.T) {
		assertPostgreClauseSerialize(t, MINi(table1ColInt), "MIN(table1.col_int)")
	})
}

func TestFuncMAX(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		assertPostgreClauseSerialize(t, MAXf(table1ColFloat), "MAX(table1.col_float)")
		assertPostgreClauseSerialize(t, MAXf(Float(11.2222)), "MAX($1)", float64(11.2222))
	})

	t.Run("integer", func(t *testing.T) {
		assertPostgreClauseSerialize(t, MAXi(table1ColInt), "MAX(table1.col_int)")
		assertPostgreClauseSerialize(t, MAXi(Int(11)), "MAX($1)", int64(11))
	})
}

func TestFuncSUM(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		assertPostgreClauseSerialize(t, SUMf(table1ColFloat), "SUM(table1.col_float)")
		assertPostgreClauseSerialize(t, SUMf(Float(11.2222)), "SUM($1)", float64(11.2222))
	})

	t.Run("integer", func(t *testing.T) {
		assertPostgreClauseSerialize(t, SUMi(table1ColInt), "SUM(table1.col_int)")
		assertPostgreClauseSerialize(t, SUMi(Int(11)), "SUM($1)", int64(11))
	})
}

func TestFuncCOUNT(t *testing.T) {
	assertPostgreClauseSerialize(t, COUNT(STAR), "COUNT(*)")
	assertPostgreClauseSerialize(t, COUNT(table1ColFloat), "COUNT(table1.col_float)")
	assertPostgreClauseSerialize(t, COUNT(Float(11.2222)), "COUNT($1)", float64(11.2222))
}

func TestFuncABS(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		assertPostgreClauseSerialize(t, ABSf(table1ColFloat), "ABS(table1.col_float)")
		assertPostgreClauseSerialize(t, ABSf(Float(11.2222)), "ABS($1)", float64(11.2222))
	})

	t.Run("integer", func(t *testing.T) {
		assertPostgreClauseSerialize(t, ABSi(table1ColInt), "ABS(table1.col_int)")
		assertPostgreClauseSerialize(t, ABSi(Int(11)), "ABS($1)", int64(11))
	})
}

func TestFuncSQRT(t *testing.T) {
	assertPostgreClauseSerialize(t, SQRT(table1ColFloat), "SQRT(table1.col_float)")
	assertPostgreClauseSerialize(t, SQRT(Float(11.2222)), "SQRT($1)", float64(11.2222))
	assertPostgreClauseSerialize(t, SQRT(table1ColInt), "SQRT(table1.col_int)")
	assertPostgreClauseSerialize(t, SQRT(Int(11)), "SQRT($1)", int64(11))
}

func TestFuncCBRT(t *testing.T) {
	assertPostgreClauseSerialize(t, CBRT(table1ColFloat), "CBRT(table1.col_float)")
	assertPostgreClauseSerialize(t, CBRT(Float(11.2222)), "CBRT($1)", float64(11.2222))
	assertPostgreClauseSerialize(t, CBRT(table1ColInt), "CBRT(table1.col_int)")
	assertPostgreClauseSerialize(t, CBRT(Int(11)), "CBRT($1)", int64(11))
}

func TestFuncCEIL(t *testing.T) {
	assertPostgreClauseSerialize(t, CEIL(table1ColFloat), "CEIL(table1.col_float)")
	assertPostgreClauseSerialize(t, CEIL(Float(11.2222)), "CEIL($1)", float64(11.2222))
}

func TestFuncFLOOR(t *testing.T) {
	assertPostgreClauseSerialize(t, FLOOR(table1ColFloat), "FLOOR(table1.col_float)")
	assertPostgreClauseSerialize(t, FLOOR(Float(11.2222)), "FLOOR($1)", float64(11.2222))
}

func TestFuncROUND(t *testing.T) {
	assertPostgreClauseSerialize(t, ROUND(table1ColFloat), "ROUND(table1.col_float)")
	assertPostgreClauseSerialize(t, ROUND(Float(11.2222)), "ROUND($1)", float64(11.2222))

	assertPostgreClauseSerialize(t, ROUND(table1ColFloat, Int(2)), "ROUND(table1.col_float, $1)", int64(2))
	assertPostgreClauseSerialize(t, ROUND(Float(11.2222), Int(1)), "ROUND($1, $2)", float64(11.2222), int64(1))
}

func TestFuncSIGN(t *testing.T) {
	assertPostgreClauseSerialize(t, SIGN(table1ColFloat), "SIGN(table1.col_float)")
	assertPostgreClauseSerialize(t, SIGN(Float(11.2222)), "SIGN($1)", float64(11.2222))
}

func TestFuncTRUNC(t *testing.T) {
	assertPostgreClauseSerialize(t, TRUNC(table1ColFloat), "TRUNC(table1.col_float)")
	assertPostgreClauseSerialize(t, TRUNC(Float(11.2222)), "TRUNC($1)", float64(11.2222))

	assertPostgreClauseSerialize(t, TRUNC(table1ColFloat, Int(2)), "TRUNC(table1.col_float, $1)", int64(2))
	assertPostgreClauseSerialize(t, TRUNC(Float(11.2222), Int(1)), "TRUNC($1, $2)", float64(11.2222), int64(1))
}

func TestFuncLN(t *testing.T) {
	assertPostgreClauseSerialize(t, LN(table1ColFloat), "LN(table1.col_float)")
	assertPostgreClauseSerialize(t, LN(Float(11.2222)), "LN($1)", float64(11.2222))
}

func TestFuncLOG(t *testing.T) {
	assertPostgreClauseSerialize(t, LOG(table1ColFloat), "LOG(table1.col_float)")
	assertPostgreClauseSerialize(t, LOG(Float(11.2222)), "LOG($1)", float64(11.2222))
}

func TestFuncCOALESCE(t *testing.T) {
	assertPostgreClauseSerialize(t, COALESCE(table1ColFloat), "COALESCE(table1.col_float)")
	assertPostgreClauseSerialize(t, COALESCE(Float(11.2222), NULL, String("str")), "COALESCE($1, NULL, $2)", float64(11.2222), "str")
}

func TestFuncNULLIF(t *testing.T) {
	assertPostgreClauseSerialize(t, NULLIF(table1ColFloat, table2ColInt), "NULLIF(table1.col_float, table2.col_int)")
	assertPostgreClauseSerialize(t, NULLIF(Float(11.2222), NULL), "NULLIF($1, NULL)", float64(11.2222))
}

func TestFuncGREATEST(t *testing.T) {
	assertPostgreClauseSerialize(t, GREATEST(table1ColFloat), "GREATEST(table1.col_float)")
	assertPostgreClauseSerialize(t, GREATEST(Float(11.2222), NULL, String("str")), "GREATEST($1, NULL, $2)", float64(11.2222), "str")
}

func TestFuncLEAST(t *testing.T) {
	assertPostgreClauseSerialize(t, LEAST(table1ColFloat), "LEAST(table1.col_float)")
	assertPostgreClauseSerialize(t, LEAST(Float(11.2222), NULL, String("str")), "LEAST($1, NULL, $2)", float64(11.2222), "str")
}

func TestTO_ASCII(t *testing.T) {
	assertPostgreClauseSerialize(t, TO_ASCII(String("Karel")), `TO_ASCII($1)`, "Karel")
	assertPostgreClauseSerialize(t, TO_ASCII(String("Karel")), `TO_ASCII($1)`, "Karel")
}
