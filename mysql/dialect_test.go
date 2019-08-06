package mysql

import (
	"testing"
)

func TestBoolExpressionIS_DISTINCT_FROM(t *testing.T) {
	assertClauseSerialize(t, table1ColBool.IS_DISTINCT_FROM(table2ColBool), "(NOT(table1.col_bool <=> table2.col_bool))")
	assertClauseSerialize(t, table1ColBool.IS_DISTINCT_FROM(Bool(false)), "(NOT(table1.col_bool <=> ?))", false)
}

func TestBoolExpressionIS_NOT_DISTINCT_FROM(t *testing.T) {
	assertClauseSerialize(t, table1ColBool.IS_NOT_DISTINCT_FROM(table2ColBool), "(table1.col_bool <=> table2.col_bool)")
	assertClauseSerialize(t, table1ColBool.IS_NOT_DISTINCT_FROM(Bool(false)), "(table1.col_bool <=> ?)", false)
}

func TestBoolLiteral(t *testing.T) {
	assertClauseSerialize(t, Bool(true), "?", true)
	assertClauseSerialize(t, Bool(false), "?", false)
}

func TestIntegerExpressionDIV(t *testing.T) {
	assertClauseSerialize(t, table1ColInt.DIV(table2ColInt), "(table1.col_int DIV table2.col_int)")
	assertClauseSerialize(t, table1ColInt.DIV(Int(11)), "(table1.col_int DIV ?)", int64(11))
}

func TestIntExpressionPOW(t *testing.T) {
	assertClauseSerialize(t, table1ColInt.POW(table2ColInt), "POW(table1.col_int, table2.col_int)")
	assertClauseSerialize(t, table1ColInt.POW(Int(11)), "POW(table1.col_int, ?)", int64(11))
}

func TestIntExpressionBIT_XOR(t *testing.T) {
	assertClauseSerialize(t, table1ColInt.BIT_XOR(table2ColInt), "(table1.col_int ^ table2.col_int)")
	assertClauseSerialize(t, table1ColInt.BIT_XOR(Int(11)), "(table1.col_int ^ ?)", int64(11))
}
