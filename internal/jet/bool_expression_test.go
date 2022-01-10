package jet

import (
	"testing"
)

func TestBoolExpressionEQ(t *testing.T) {
	assertClauseSerialize(t, table1ColBool.EQ(table2ColBool), "(table1.col_bool = table2.col_bool)")
}

func TestBoolExpressionNOT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColBool.NOT_EQ(table2ColBool), "(table1.col_bool != table2.col_bool)")
	assertClauseSerialize(t, table1ColBool.NOT_EQ(Bool(true)), "(table1.col_bool != $1)", true)
}

func TestBoolExpressionIS_DISTINCT_FROM(t *testing.T) {
	assertClauseSerialize(t, table1ColBool.IS_DISTINCT_FROM(table2ColBool), "(table1.col_bool IS DISTINCT FROM table2.col_bool)")
	assertClauseSerialize(t, table1ColBool.IS_DISTINCT_FROM(Bool(false)), "(table1.col_bool IS DISTINCT FROM $1)", false)
}

func TestBoolExpressionIS_NOT_DISTINCT_FROM(t *testing.T) {
	assertClauseSerialize(t, table1ColBool.IS_NOT_DISTINCT_FROM(table2ColBool), "(table1.col_bool IS NOT DISTINCT FROM table2.col_bool)")
	assertClauseSerialize(t, table1ColBool.IS_NOT_DISTINCT_FROM(Bool(false)), "(table1.col_bool IS NOT DISTINCT FROM $1)", false)
}

func TestBoolExpressionIS_TRUE(t *testing.T) {
	assertClauseSerialize(t, table1ColBool.IS_TRUE(), "table1.col_bool IS TRUE")
	assertClauseSerialize(t, (Int(2).EQ(table1ColInt)).IS_TRUE(),
		`($1 = table1.col_int) IS TRUE`, int64(2))
	assertClauseSerialize(t, (Int(2).EQ(table1ColInt)).IS_TRUE().AND(Int(4).EQ(table2ColInt)),
		`(($1 = table1.col_int) IS TRUE AND ($2 = table2.col_int))`, int64(2), int64(4))
}

func TestBoolExpressionIS_NOT_TRUE(t *testing.T) {
	assertClauseSerialize(t, table1ColBool.IS_NOT_TRUE(), "table1.col_bool IS NOT TRUE")
}

func TestBoolExpressionIS_FALSE(t *testing.T) {
	assertClauseSerialize(t, table1ColBool.IS_FALSE(), "table1.col_bool IS FALSE")
}

func TestBoolExpressionIS_NOT_FALSE(t *testing.T) {
	assertClauseSerialize(t, table1ColBool.IS_NOT_FALSE(), "table1.col_bool IS NOT FALSE")
}

func TestBoolExpressionIS_UNKNOWN(t *testing.T) {
	assertClauseSerialize(t, table1ColBool.IS_UNKNOWN(), "table1.col_bool IS UNKNOWN")
}

func TestBoolExpressionIS_NOT_UNKNOWN(t *testing.T) {
	assertClauseSerialize(t, table1ColBool.IS_NOT_UNKNOWN(), "table1.col_bool IS NOT UNKNOWN")
}

func TestBinaryBoolExpression(t *testing.T) {
	boolExpression := Int(2).EQ(Int(3))

	assertClauseSerialize(t, boolExpression, "($1 = $2)", int64(2), int64(3))

	assertProjectionSerialize(t, boolExpression, "$1 = $2", int64(2), int64(3))
	assertProjectionSerialize(t, boolExpression.AS("alias_eq_expression"),
		`($1 = $2) AS "alias_eq_expression"`, int64(2), int64(3))
	assertClauseSerialize(t, boolExpression.AND(Int(4).EQ(Int(5))),
		"(($1 = $2) AND ($3 = $4))", int64(2), int64(3), int64(4), int64(5))
	assertClauseSerialize(t, boolExpression.OR(Int(4).EQ(Int(5))),
		"(($1 = $2) OR ($3 = $4))", int64(2), int64(3), int64(4), int64(5))
}

func TestBoolLiteral(t *testing.T) {
	assertClauseSerialize(t, Bool(true), "$1", true)
	assertClauseSerialize(t, Bool(false), "$1", false)
}

func TestBoolExp(t *testing.T) {
	assertClauseSerialize(t, BoolExp(String("true")), "$1", "true")
	assertClauseSerialize(t, BoolExp(String("true")).IS_TRUE(), "$1 IS TRUE", "true")
}
