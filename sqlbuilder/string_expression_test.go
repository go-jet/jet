package sqlbuilder

import (
	"testing"
)

func TestStringEQ(t *testing.T) {
	exp := table3StrCol.EQ(table2ColStr)
	assertExpressionSerialize(t, exp, "(table3.col2 = table2.colStr)")
	exp = table3StrCol.EQ(String("JOHN"))
	assertExpressionSerialize(t, exp, "(table3.col2 = $1)", "JOHN")
}

func TestStringNOT_EQ(t *testing.T) {
	exp := table3StrCol.NOT_EQ(table2ColStr)
	assertExpressionSerialize(t, exp, "(table3.col2 != table2.colStr)")
	assertExpressionSerialize(t, table3StrCol.NOT_EQ(String("JOHN")), "(table3.col2 != $1)", "JOHN")
}

func TestStringGT(t *testing.T) {
	exp := table3StrCol.GT(table2ColStr)
	assertExpressionSerialize(t, exp, "(table3.col2 > table2.colStr)")
	assertExpressionSerialize(t, table3StrCol.GT(String("JOHN")), "(table3.col2 > $1)", "JOHN")
}

func TestStringGT_EQ(t *testing.T) {
	exp := table3StrCol.GT_EQ(table2ColStr)
	assertExpressionSerialize(t, exp, "(table3.col2 >= table2.colStr)")
	assertExpressionSerialize(t, table3StrCol.GT_EQ(String("JOHN")), "(table3.col2 >= $1)", "JOHN")
}

func TestStringLT(t *testing.T) {
	exp := table3StrCol.LT(table2ColStr)
	assertExpressionSerialize(t, exp, "(table3.col2 < table2.colStr)")
	assertExpressionSerialize(t, table3StrCol.LT(String("JOHN")), "(table3.col2 < $1)", "JOHN")
}

func TestStringLT_EQ(t *testing.T) {
	exp := table3StrCol.LT_EQ(table2ColStr)
	assertExpressionSerialize(t, exp, "(table3.col2 <= table2.colStr)")
	assertExpressionSerialize(t, table3StrCol.LT_EQ(String("JOHN")), "(table3.col2 <= $1)", "JOHN")
}
