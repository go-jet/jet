package jet

import (
	"testing"
)

func TestFloatExpressionEQ(t *testing.T) {
	assertClauseSerialize(t, table1ColFloat.EQ(table2ColFloat), "(table1.col_float = table2.col_float)")
	assertClauseSerialize(t, table1ColFloat.EQ(Float(2.11)), "(table1.col_float = $1)", float64(2.11))
}

func TestFloatExpressionNOT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColFloat.NOT_EQ(table2ColFloat), "(table1.col_float != table2.col_float)")
	assertClauseSerialize(t, table1ColFloat.NOT_EQ(Float(2.11)), "(table1.col_float != $1)", float64(2.11))
}

func TestFloatExpressionIS_DISTINCT_FROM(t *testing.T) {
	assertClauseSerialize(t, table1ColFloat.IS_DISTINCT_FROM(table2ColFloat), "(table1.col_float IS DISTINCT FROM table2.col_float)")
	assertClauseSerialize(t, table1ColFloat.IS_DISTINCT_FROM(Float(2.11)), "(table1.col_float IS DISTINCT FROM $1)", float64(2.11))
}

func TestFloatExpressionIS_NOT_DISTINCT_FROM(t *testing.T) {
	assertClauseSerialize(t, table1ColFloat.IS_NOT_DISTINCT_FROM(table2ColFloat), "(table1.col_float IS NOT DISTINCT FROM table2.col_float)")
	assertClauseSerialize(t, table1ColFloat.IS_NOT_DISTINCT_FROM(Float(2.11)), "(table1.col_float IS NOT DISTINCT FROM $1)", float64(2.11))
}

func TestFloatExpressionGT(t *testing.T) {
	assertClauseSerialize(t, table1ColFloat.GT(table2ColFloat), "(table1.col_float > table2.col_float)")
	assertClauseSerialize(t, table1ColFloat.GT(Float(2.11)), "(table1.col_float > $1)", float64(2.11))
}

func TestFloatExpressionGT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColFloat.GT_EQ(table2ColFloat), "(table1.col_float >= table2.col_float)")
	assertClauseSerialize(t, table1ColFloat.GT_EQ(Float(2.11)), "(table1.col_float >= $1)", float64(2.11))
}

func TestFloatExpressionLT(t *testing.T) {
	assertClauseSerialize(t, table1ColFloat.LT(table2ColFloat), "(table1.col_float < table2.col_float)")
	assertClauseSerialize(t, table1ColFloat.LT(Float(2.11)), "(table1.col_float < $1)", float64(2.11))
}

func TestFloatExpressionLT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColFloat.LT_EQ(table2ColFloat), "(table1.col_float <= table2.col_float)")
	assertClauseSerialize(t, table1ColFloat.LT_EQ(Float(2.11)), "(table1.col_float <= $1)", float64(2.11))
}

func TestFloatExpressionADD(t *testing.T) {
	assertClauseSerialize(t, table1ColFloat.ADD(table2ColFloat), "(table1.col_float + table2.col_float)")
	assertClauseSerialize(t, table1ColFloat.ADD(Float(2.11)), "(table1.col_float + $1)", float64(2.11))
}

func TestFloatExpressionSUB(t *testing.T) {
	assertClauseSerialize(t, table1ColFloat.SUB(table2ColFloat), "(table1.col_float - table2.col_float)")
	assertClauseSerialize(t, table1ColFloat.SUB(Float(2.11)), "(table1.col_float - $1)", float64(2.11))
}

func TestFloatExpressionMUL(t *testing.T) {
	assertClauseSerialize(t, table1ColFloat.MUL(table2ColFloat), "(table1.col_float * table2.col_float)")
	assertClauseSerialize(t, table1ColFloat.MUL(Float(2.11)), "(table1.col_float * $1)", float64(2.11))
}

func TestFloatExpressionDIV(t *testing.T) {
	assertClauseSerialize(t, table1ColFloat.DIV(table2ColFloat), "(table1.col_float / table2.col_float)")
	assertClauseSerialize(t, table1ColFloat.DIV(Float(2.11)), "(table1.col_float / $1)", float64(2.11))
}

func TestFloatExpressionMOD(t *testing.T) {
	assertClauseSerialize(t, table1ColFloat.MOD(table2ColFloat), "(table1.col_float % table2.col_float)")
	assertClauseSerialize(t, table1ColFloat.MOD(Float(2.11)), "(table1.col_float % $1)", float64(2.11))
}

func TestFloatExpressionPOW(t *testing.T) {
	assertClauseSerialize(t, table1ColFloat.POW(table2ColFloat), "POW(table1.col_float, table2.col_float)")
	assertClauseSerialize(t, table1ColFloat.POW(Float(2.11)), "POW(table1.col_float, $1)", float64(2.11))
}

func TestFloatExp(t *testing.T) {
	assertClauseSerialize(t, FloatExp(table1ColInt), "table1.col_int")
	assertClauseSerialize(t, FloatExp(table1ColInt.ADD(table3ColInt)), "(table1.col_int + table3.col_int)")
	assertClauseSerialize(t, FloatExp(table1ColInt.ADD(table3ColInt)).ADD(Float(11.11)),
		"((table1.col_int + table3.col_int) + $1)", float64(11.11))
}
