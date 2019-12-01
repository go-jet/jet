package mysql

import (
	"testing"
)

func TestBoolExpressionIS_DISTINCT_FROM(t *testing.T) {
	assertSerialize(t, table1ColBool.IS_DISTINCT_FROM(table2ColBool), "(NOT(table1.col_bool <=> table2.col_bool))")
	assertSerialize(t, table1ColBool.IS_DISTINCT_FROM(Bool(false)), "(NOT(table1.col_bool <=> ?))", false)
}

func TestBoolExpressionIS_NOT_DISTINCT_FROM(t *testing.T) {
	assertSerialize(t, table1ColBool.IS_NOT_DISTINCT_FROM(table2ColBool), "(table1.col_bool <=> table2.col_bool)")
	assertSerialize(t, table1ColBool.IS_NOT_DISTINCT_FROM(Bool(false)), "(table1.col_bool <=> ?)", false)
}

func TestBoolLiteral(t *testing.T) {
	assertSerialize(t, Bool(true), "?", true)
	assertSerialize(t, Bool(false), "?", false)
}

func TestIntegerExpressionDIV(t *testing.T) {
	assertSerialize(t, table1ColInt.DIV(table2ColInt), "(table1.col_int DIV table2.col_int)")
	assertSerialize(t, table1ColInt.DIV(Int(11)), "(table1.col_int DIV ?)", int64(11))
}

func TestIntExpressionPOW(t *testing.T) {
	assertSerialize(t, table1ColInt.POW(table2ColInt), "POW(table1.col_int, table2.col_int)")
	assertSerialize(t, table1ColInt.POW(Int(11)), "POW(table1.col_int, ?)", int64(11))
}

func TestIntExpressionBIT_XOR(t *testing.T) {
	assertSerialize(t, table1ColInt.BIT_XOR(table2ColInt), "(table1.col_int ^ table2.col_int)")
	assertSerialize(t, table1ColInt.BIT_XOR(Int(11)), "(table1.col_int ^ ?)", int64(11))
}

func TestExists(t *testing.T) {
	assertSerialize(t, EXISTS(
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

func TestString_REGEXP_LIKE_operator(t *testing.T) {
	assertSerialize(t, table3StrCol.REGEXP_LIKE(table2ColStr), "(table3.col2 REGEXP table2.col_str)")
	assertSerialize(t, table3StrCol.REGEXP_LIKE(String("JOHN")), "(table3.col2 REGEXP ?)", "JOHN")
	assertSerialize(t, table3StrCol.REGEXP_LIKE(String("JOHN"), false), "(table3.col2 REGEXP ?)", "JOHN")
	assertSerialize(t, table3StrCol.REGEXP_LIKE(String("JOHN"), true), "(table3.col2 REGEXP BINARY ?)", "JOHN")
}

func TestString_NOT_REGEXP_LIKE_operator(t *testing.T) {
	assertSerialize(t, table3StrCol.NOT_REGEXP_LIKE(table2ColStr), "(table3.col2 NOT REGEXP table2.col_str)")
	assertSerialize(t, table3StrCol.NOT_REGEXP_LIKE(String("JOHN")), "(table3.col2 NOT REGEXP ?)", "JOHN")
	assertSerialize(t, table3StrCol.NOT_REGEXP_LIKE(String("JOHN"), false), "(table3.col2 NOT REGEXP ?)", "JOHN")
	assertSerialize(t, table3StrCol.NOT_REGEXP_LIKE(String("JOHN"), true), "(table3.col2 NOT REGEXP BINARY ?)", "JOHN")
}
