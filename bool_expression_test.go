package jet

import (
	"testing"
)

func TestBoolExpressionEQ(t *testing.T) {
	assertClauseSerializeErr(t, table1ColBool.EQ(nil), "jet: nil rhs")
	AssertPostgreClauseSerialize(t, table1ColBool.EQ(table2ColBool), "(table1.col_bool = table2.col_bool)")
	AssertPostgreClauseSerialize(t, table1ColBool.EQ(Bool(true)), "(table1.col_bool = $1)", true)

	AssertMySQLClauseSerialize(t, table1ColBool.EQ(table2ColBool), "(table1.col_bool = table2.col_bool)")
	AssertMySQLClauseSerialize(t, table1ColBool.EQ(Bool(true)), "(table1.col_bool = ?)", true)
}

func TestBoolExpressionNOT_EQ(t *testing.T) {
	AssertPostgreClauseSerialize(t, table1ColBool.NOT_EQ(table2ColBool), "(table1.col_bool != table2.col_bool)")
	AssertPostgreClauseSerialize(t, table1ColBool.NOT_EQ(Bool(true)), "(table1.col_bool != $1)", true)

	AssertMySQLClauseSerialize(t, table1ColBool.NOT_EQ(table2ColBool), "(table1.col_bool != table2.col_bool)")
	AssertMySQLClauseSerialize(t, table1ColBool.NOT_EQ(Bool(true)), "(table1.col_bool != ?)", true)
}

func TestBoolExpressionIS_DISTINCT_FROM(t *testing.T) {
	AssertPostgreClauseSerialize(t, table1ColBool.IS_DISTINCT_FROM(table2ColBool), "(table1.col_bool IS DISTINCT FROM table2.col_bool)")
	AssertPostgreClauseSerialize(t, table1ColBool.IS_DISTINCT_FROM(Bool(false)), "(table1.col_bool IS DISTINCT FROM $1)", false)

	AssertMySQLClauseSerialize(t, table1ColBool.IS_DISTINCT_FROM(table2ColBool), "(NOT table1.col_bool <=> table2.col_bool)")
	AssertMySQLClauseSerialize(t, table1ColBool.IS_DISTINCT_FROM(Bool(false)), "(NOT table1.col_bool <=> ?)", false)
}

func TestBoolExpressionIS_NOT_DISTINCT_FROM(t *testing.T) {
	AssertPostgreClauseSerialize(t, table1ColBool.IS_NOT_DISTINCT_FROM(table2ColBool), "(table1.col_bool IS NOT DISTINCT FROM table2.col_bool)")
	AssertPostgreClauseSerialize(t, table1ColBool.IS_NOT_DISTINCT_FROM(Bool(false)), "(table1.col_bool IS NOT DISTINCT FROM $1)", false)

	AssertMySQLClauseSerialize(t, table1ColBool.IS_NOT_DISTINCT_FROM(table2ColBool), "(table1.col_bool <=> table2.col_bool)")
	AssertMySQLClauseSerialize(t, table1ColBool.IS_NOT_DISTINCT_FROM(Bool(false)), "(table1.col_bool <=> ?)", false)

}

func TestBoolExpressionIS_TRUE(t *testing.T) {
	AssertPostgreClauseSerialize(t, table1ColBool.IS_TRUE(), "table1.col_bool IS TRUE")
	AssertPostgreClauseSerialize(t, (Int(2).EQ(table1ColInt)).IS_TRUE(),
		`($1 = table1.col_int) IS TRUE`, int64(2))
	AssertPostgreClauseSerialize(t, (Int(2).EQ(table1ColInt)).IS_TRUE().AND(Int(4).EQ(table2ColInt)),
		`(($1 = table1.col_int) IS TRUE AND ($2 = table2.col_int))`, int64(2), int64(4))

	AssertMySQLClauseSerialize(t, table1ColBool.IS_TRUE(), "table1.col_bool IS TRUE")
	AssertMySQLClauseSerialize(t, (Int(2).EQ(table1ColInt)).IS_TRUE(),
		`(? = table1.col_int) IS TRUE`, int64(2))
	AssertMySQLClauseSerialize(t, (Int(2).EQ(table1ColInt)).IS_TRUE().AND(Int(4).EQ(table2ColInt)),
		`((? = table1.col_int) IS TRUE AND (? = table2.col_int))`, int64(2), int64(4))
}

func TestBoolExpressionIS_NOT_TRUE(t *testing.T) {
	AssertPostgreClauseSerialize(t, table1ColBool.IS_NOT_TRUE(), "table1.col_bool IS NOT TRUE")

	AssertMySQLClauseSerialize(t, table1ColBool.IS_NOT_TRUE(), "table1.col_bool IS NOT TRUE")
}

func TestBoolExpressionIS_FALSE(t *testing.T) {
	AssertPostgreClauseSerialize(t, table1ColBool.IS_FALSE(), "table1.col_bool IS FALSE")

	AssertMySQLClauseSerialize(t, table1ColBool.IS_FALSE(), "table1.col_bool IS FALSE")
}

func TestBoolExpressionIS_NOT_FALSE(t *testing.T) {
	AssertPostgreClauseSerialize(t, table1ColBool.IS_NOT_FALSE(), "table1.col_bool IS NOT FALSE")
}

func TestBoolExpressionIS_UNKNOWN(t *testing.T) {
	AssertPostgreClauseSerialize(t, table1ColBool.IS_UNKNOWN(), "table1.col_bool IS UNKNOWN")

	AssertMySQLClauseSerialize(t, table1ColBool.IS_UNKNOWN(), "table1.col_bool IS UNKNOWN")
}

func TestBoolExpressionIS_NOT_UNKNOWN(t *testing.T) {
	AssertPostgreClauseSerialize(t, table1ColBool.IS_NOT_UNKNOWN(), "table1.col_bool IS NOT UNKNOWN")

	AssertMySQLClauseSerialize(t, table1ColBool.IS_NOT_UNKNOWN(), "table1.col_bool IS NOT UNKNOWN")
}

func TestBinaryBoolExpression(t *testing.T) {
	boolExpression := Int(2).EQ(Int(3))

	AssertPostgreClauseSerialize(t, boolExpression, "($1 = $2)", int64(2), int64(3))

	assertProjectionSerialize(t, boolExpression, "$1 = $2", int64(2), int64(3))
	assertProjectionSerialize(t, boolExpression.AS("alias_eq_expression"),
		`($1 = $2) AS "alias_eq_expression"`, int64(2), int64(3))
	AssertPostgreClauseSerialize(t, boolExpression.AND(Int(4).EQ(Int(5))),
		"(($1 = $2) AND ($3 = $4))", int64(2), int64(3), int64(4), int64(5))
	AssertPostgreClauseSerialize(t, boolExpression.OR(Int(4).EQ(Int(5))),
		"(($1 = $2) OR ($3 = $4))", int64(2), int64(3), int64(4), int64(5))
}

func TestBoolLiteral(t *testing.T) {
	AssertPostgreClauseSerialize(t, Bool(true), "$1", true)
	AssertPostgreClauseSerialize(t, Bool(false), "$1", false)

	AssertMySQLClauseSerialize(t, Bool(true), "?", true)
	AssertMySQLClauseSerialize(t, Bool(false), "?", false)
}

func TestExists(t *testing.T) {

	AssertPostgreClauseSerialize(t, EXISTS(
		table2.
			SELECT(Int(1)).
			WHERE(table1Col1.EQ(table2Col3)),
	),
		`(EXISTS (
     SELECT $1
     FROM db.table2
     WHERE table1.col1 = table2.col3
))`, int64(1))

	AssertMySQLClauseSerialize(t, EXISTS(
		table2.
			SELECT(Int(1)).
			WHERE(table1Col1.EQ(table2Col3)),
	),
		`(EXISTS (
     SELECT ?
     FROM db.table2
     WHERE table1.col1 = table2.col3
))`, int64(1))
}

func TestBoolExp(t *testing.T) {
	AssertPostgreClauseSerialize(t, BoolExp(String("true")), "$1", "true")
	AssertPostgreClauseSerialize(t, BoolExp(String("true")).IS_TRUE(), "$1 IS TRUE", "true")

	AssertMySQLClauseSerialize(t, BoolExp(String("true")), "?", "true")
	AssertMySQLClauseSerialize(t, BoolExp(String("true")).IS_TRUE(), "? IS TRUE", "true")
}
