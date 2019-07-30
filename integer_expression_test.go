package jet

import (
	"testing"
)

func TestIntegerExpressionEQ(t *testing.T) {
	assertPostgreClauseSerialize(t, table1ColInt.EQ(table2ColInt), "(table1.col_int = table2.col_int)")
	assertPostgreClauseSerialize(t, table1ColInt.EQ(Int(11)), "(table1.col_int = $1)", int64(11))
}

func TestIntegerExpressionNOT_EQ(t *testing.T) {
	assertPostgreClauseSerialize(t, table1ColInt.NOT_EQ(table2ColInt), "(table1.col_int != table2.col_int)")
	assertPostgreClauseSerialize(t, table1ColInt.NOT_EQ(Int(11)), "(table1.col_int != $1)", int64(11))
}

func TestIntegerExpressionGT(t *testing.T) {
	assertPostgreClauseSerialize(t, table1ColInt.GT(table2ColInt), "(table1.col_int > table2.col_int)")
	assertPostgreClauseSerialize(t, table1ColInt.GT(Int(11)), "(table1.col_int > $1)", int64(11))
}

func TestIntegerExpressionGT_EQ(t *testing.T) {
	assertPostgreClauseSerialize(t, table1ColInt.GT_EQ(table2ColInt), "(table1.col_int >= table2.col_int)")
	assertPostgreClauseSerialize(t, table1ColInt.GT_EQ(Int(11)), "(table1.col_int >= $1)", int64(11))
}

func TestIntegerExpressionLT(t *testing.T) {
	assertPostgreClauseSerialize(t, table1ColInt.LT(table2ColInt), "(table1.col_int < table2.col_int)")
	assertPostgreClauseSerialize(t, table1ColInt.LT(Int(11)), "(table1.col_int < $1)", int64(11))
}

func TestIntegerExpressionLT_EQ(t *testing.T) {
	assertPostgreClauseSerialize(t, table1ColInt.LT_EQ(table2ColInt), "(table1.col_int <= table2.col_int)")
	assertPostgreClauseSerialize(t, table1ColInt.LT_EQ(Int(11)), "(table1.col_int <= $1)", int64(11))
}

func TestIntegerExpressionADD(t *testing.T) {
	assertPostgreClauseSerialize(t, table1ColInt.ADD(table2ColInt), "(table1.col_int + table2.col_int)")
	assertPostgreClauseSerialize(t, table1ColInt.ADD(Int(11)), "(table1.col_int + $1)", int64(11))
}

func TestIntegerExpressionSUB(t *testing.T) {
	assertPostgreClauseSerialize(t, table1ColInt.SUB(table2ColInt), "(table1.col_int - table2.col_int)")
	assertPostgreClauseSerialize(t, table1ColInt.SUB(Int(11)), "(table1.col_int - $1)", int64(11))
}

func TestIntegerExpressionMUL(t *testing.T) {
	assertPostgreClauseSerialize(t, table1ColInt.MUL(table2ColInt), "(table1.col_int * table2.col_int)")
	assertPostgreClauseSerialize(t, table1ColInt.MUL(Int(11)), "(table1.col_int * $1)", int64(11))
}

func TestIntegerExpressionDIV(t *testing.T) {
	assertPostgreClauseSerialize(t, table1ColInt.DIV(table2ColInt), "(table1.col_int / table2.col_int)")
	assertPostgreClauseSerialize(t, table1ColInt.DIV(Int(11)), "(table1.col_int / $1)", int64(11))
}

func TestIntExpressionMOD(t *testing.T) {
	assertPostgreClauseSerialize(t, table1ColInt.MOD(table2ColInt), "(table1.col_int % table2.col_int)")
	assertPostgreClauseSerialize(t, table1ColInt.MOD(Int(11)), "(table1.col_int % $1)", int64(11))
}

func TestIntExpressionPOW(t *testing.T) {
	assertPostgreClauseSerialize(t, table1ColInt.POW(table2ColInt), "(table1.col_int ^ table2.col_int)")
	assertPostgreClauseSerialize(t, table1ColInt.POW(Int(11)), "(table1.col_int ^ $1)", int64(11))
}

func TestIntExpressionBIT_NOT(t *testing.T) {
	assertPostgreClauseSerialize(t, BIT_NOT(table2ColInt), "~ table2.col_int")
	assertPostgreClauseSerialize(t, BIT_NOT(Int(11)), "~ $1", int64(11))
}

func TestIntExpressionBIT_SHIFT_LEFT(t *testing.T) {
	assertPostgreClauseSerialize(t, table1ColInt.BIT_SHIFT_LEFT(table2ColInt), "(table1.col_int << table2.col_int)")
	assertPostgreClauseSerialize(t, table1ColInt.BIT_SHIFT_LEFT(Int(11)), "(table1.col_int << $1)", int64(11))
}

func TestIntExpressionBIT_SHIFT_RIGHT(t *testing.T) {
	assertPostgreClauseSerialize(t, table1ColInt.BIT_SHIFT_RIGHT(table2ColInt), "(table1.col_int >> table2.col_int)")
	assertPostgreClauseSerialize(t, table1ColInt.BIT_SHIFT_RIGHT(Int(11)), "(table1.col_int >> $1)", int64(11))
}

func TestIntExpressionIntExp(t *testing.T) {
	assertPostgreClauseSerialize(t, IntExp(table1ColFloat), "table1.col_float")
	assertPostgreClauseSerialize(t, IntExp(table1ColFloat.ADD(table2ColFloat)).ADD(Int(11)),
		"((table1.col_float + table2.col_float) + $1)", int64(11))
}
