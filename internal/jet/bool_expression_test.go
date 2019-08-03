package jet

import (
	"testing"
)

func TestBoolExpressionEQ(t *testing.T) {
	assertClauseSerializeErr(t, table1ColBool.EQ(nil), "jet: nil rhs")
	assertClauseSerialize(t, table1ColBool.EQ(table2ColBool), "(table1.col_bool = table2.col_bool)")
}

func TestBoolExpressionNOT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColBool.NOT_EQ(table2ColBool), "(table1.col_bool != table2.col_bool)")
	assertClauseSerialize(t, table1ColBool.NOT_EQ(Bool(true)), "(table1.col_bool != TRUE)")
}

func TestBoolExpressionIS_DISTINCT_FROM(t *testing.T) {
	assertClauseSerialize(t, table1ColBool.IS_DISTINCT_FROM(table2ColBool), "(table1.col_bool IS DISTINCT FROM table2.col_bool)")
	assertClauseSerialize(t, table1ColBool.IS_DISTINCT_FROM(Bool(false)), "(table1.col_bool IS DISTINCT FROM FALSE)")
}

func TestBoolExpressionIS_NOT_DISTINCT_FROM(t *testing.T) {
	assertClauseSerialize(t, table1ColBool.IS_NOT_DISTINCT_FROM(table2ColBool), "(table1.col_bool IS NOT DISTINCT FROM table2.col_bool)")
	assertClauseSerialize(t, table1ColBool.IS_NOT_DISTINCT_FROM(Bool(false)), "(table1.col_bool IS NOT DISTINCT FROM FALSE)")
}

func TestBoolExpressionIS_TRUE(t *testing.T) {
	assertClauseSerialize(t, table1ColBool.IS_TRUE(), "table1.col_bool IS TRUE")
	assertClauseSerialize(t, (Int(2).EQ(table1ColInt)).IS_TRUE(),
		`(2 = table1.col_int) IS TRUE`)
	assertClauseSerialize(t, (Int(2).EQ(table1ColInt)).IS_TRUE().AND(Int(4).EQ(table2ColInt)),
		`((2 = table1.col_int) IS TRUE AND (4 = table2.col_int))`)
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

	assertClauseSerialize(t, boolExpression, "(2 = 3)")

	assertProjectionSerialize(t, boolExpression, "2 = 3")
	assertProjectionSerialize(t, boolExpression.AS("alias_eq_expression"),
		`(2 = 3) AS "alias_eq_expression"`)
	assertClauseSerialize(t, boolExpression.AND(Int(4).EQ(Int(5))),
		"((2 = 3) AND (4 = 5))")
	assertClauseSerialize(t, boolExpression.OR(Int(4).EQ(Int(5))),
		"((2 = 3) OR (4 = 5))")
}

func TestBoolLiteral(t *testing.T) {
	assertClauseSerialize(t, Bool(true), "TRUE", true)
	assertClauseSerialize(t, Bool(false), "FALSE", false)
}

func TestExists(t *testing.T) {
	assertClauseSerialize(t, EXISTS(
		table2.
			SELECT(Int(1)).
			WHERE(table1Col1.EQ(table2Col3)),
	),
		`(EXISTS (
     SELECT 1
     FROM db.table2
     WHERE table1.col1 = table2.col3
))`)
}

func TestBoolExp(t *testing.T) {
	assertClauseSerialize(t, BoolExp(String("true")), "'true'")
	assertClauseSerialize(t, BoolExp(String("true")).IS_TRUE(), "'true' IS TRUE")
}
