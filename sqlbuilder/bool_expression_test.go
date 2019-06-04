package sqlbuilder

import (
	"testing"
)

func TestBoolExpressionEQ(t *testing.T) {
	assertExpressionSerialize(t, table1ColBool.EQ(table2ColBool), "(table1.colBool = table2.colBool)")
	assertExpressionSerialize(t, table1ColBool.EQ(Bool(true)), "(table1.colBool = $1)", true)
}

func TestBoolExpressionNOT_EQ(t *testing.T) {
	assertExpressionSerialize(t, table1ColBool.NOT_EQ(table2ColBool), "(table1.colBool != table2.colBool)")
	assertExpressionSerialize(t, table1ColBool.NOT_EQ(Bool(true)), "(table1.colBool != $1)", true)
}

func TestBoolExpressionIS_TRUE(t *testing.T) {
	assertExpressionSerialize(t, table1ColBool.IS_TRUE(), "table1.colBool IS TRUE")
	assertExpressionSerialize(t, (Int(2).EQ(table1ColInt)).IS_TRUE(),
		`($1 = table1.colInt) IS TRUE`, int64(2))
	assertExpressionSerialize(t, (Int(2).EQ(table1ColInt)).IS_TRUE().AND(Int(4).EQ(table2ColInt)),
		`(($1 = table1.colInt) IS TRUE AND ($2 = table2.colInt))`, int64(2), int64(4))
}

func TestBoolExpressionIS_NOT_TRUE(t *testing.T) {
	assertExpressionSerialize(t, table1ColBool.IS_NOT_TRUE(), "table1.colBool IS NOT TRUE")
}

func TestBoolExpressionIS_FALSE(t *testing.T) {
	assertExpressionSerialize(t, table1ColBool.IS_FALSE(), "table1.colBool IS FALSE")
}

func TestBoolExpressionIS_NOT_FALSE(t *testing.T) {
	assertExpressionSerialize(t, table1ColBool.IS_NOT_FALSE(), "table1.colBool IS NOT FALSE")
}

func TestBoolExpressionIS_UNKNOWN(t *testing.T) {
	assertExpressionSerialize(t, table1ColBool.IS_UNKNOWN(), "table1.colBool IS UNKNOWN")
}

func TestBoolExpressionIS_NOT_UNKNOWN(t *testing.T) {
	assertExpressionSerialize(t, table1ColBool.IS_NOT_UNKNOWN(), "table1.colBool IS NOT UNKNOWN")
}

func TestBinaryBoolExpression(t *testing.T) {
	boolExpression := Int(2).EQ(Int(3))

	assertExpressionSerialize(t, boolExpression, "($1 = $2)", int64(2), int64(3))
	assertProjectionSerialize(t, boolExpression.AS("alias_eq_expression"),
		`$1 = $2 AS "alias_eq_expression"`, int64(2), int64(3))
	assertExpressionSerialize(t, boolExpression.AND(Int(4).EQ(Int(5))),
		"(($1 = $2) AND ($3 = $4))", int64(2), int64(3), int64(4), int64(5))
	assertExpressionSerialize(t, boolExpression.OR(Int(4).EQ(Int(5))),
		"(($1 = $2) OR ($3 = $4))", int64(2), int64(3), int64(4), int64(5))
}
func TestBoolLiteral(t *testing.T) {
	assertExpressionSerialize(t, Bool(true), "$1", true)
	assertExpressionSerialize(t, Bool(false), "$1", false)
}

func TestExists(t *testing.T) {

	assertExpressionSerialize(t, EXISTS(
		table2.
			SELECT(Int(1)).
			WHERE(table1Col1.EQ(table2Col3)),
	),
		`EXISTS (
     SELECT $1
     FROM db.table2
     WHERE table1.col1 = table2.col3
)`, int64(1))
}
