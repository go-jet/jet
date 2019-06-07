package sqlbuilder

import (
	"testing"
)

func TestStringEQ(t *testing.T) {
	exp := table3StrCol.EQ(table2ColStr)
	assertClauseSerialize(t, exp, "(table3.col2 = table2.colStr)")
	exp = table3StrCol.EQ(String("JOHN"))
	assertClauseSerialize(t, exp, "(table3.col2 = $1)", "JOHN")
}

func TestStringNOT_EQ(t *testing.T) {
	exp := table3StrCol.NOT_EQ(table2ColStr)
	assertClauseSerialize(t, exp, "(table3.col2 != table2.colStr)")
	assertClauseSerialize(t, table3StrCol.NOT_EQ(String("JOHN")), "(table3.col2 != $1)", "JOHN")
}

func TestStringGT(t *testing.T) {
	exp := table3StrCol.GT(table2ColStr)
	assertClauseSerialize(t, exp, "(table3.col2 > table2.colStr)")
	assertClauseSerialize(t, table3StrCol.GT(String("JOHN")), "(table3.col2 > $1)", "JOHN")
}

func TestStringGT_EQ(t *testing.T) {
	exp := table3StrCol.GT_EQ(table2ColStr)
	assertClauseSerialize(t, exp, "(table3.col2 >= table2.colStr)")
	assertClauseSerialize(t, table3StrCol.GT_EQ(String("JOHN")), "(table3.col2 >= $1)", "JOHN")
}

func TestStringLT(t *testing.T) {
	exp := table3StrCol.LT(table2ColStr)
	assertClauseSerialize(t, exp, "(table3.col2 < table2.colStr)")
	assertClauseSerialize(t, table3StrCol.LT(String("JOHN")), "(table3.col2 < $1)", "JOHN")
}

func TestStringLT_EQ(t *testing.T) {
	exp := table3StrCol.LT_EQ(table2ColStr)
	assertClauseSerialize(t, exp, "(table3.col2 <= table2.colStr)")
	assertClauseSerialize(t, table3StrCol.LT_EQ(String("JOHN")), "(table3.col2 <= $1)", "JOHN")
}

func TestStringCONCAT(t *testing.T) {
	assertClauseSerialize(t, table3StrCol.CONCAT(table2ColStr), "(table3.col2 || table2.colStr)")
	assertClauseSerialize(t, table3StrCol.CONCAT(String("JOHN")), "(table3.col2 || $1)", "JOHN")
}

func TestStringLIKE(t *testing.T) {
	assertClauseSerialize(t, table3StrCol.LIKE(table2ColStr), "(table3.col2 LIKE table2.colStr)")
	assertClauseSerialize(t, table3StrCol.LIKE(String("JOHN")), "(table3.col2 LIKE $1)", "JOHN")
}

func TestStringNOT_LIKE(t *testing.T) {
	assertClauseSerialize(t, table3StrCol.NOT_LIKE(table2ColStr), "(table3.col2 NOT LIKE table2.colStr)")
	assertClauseSerialize(t, table3StrCol.NOT_LIKE(String("JOHN")), "(table3.col2 NOT LIKE $1)", "JOHN")
}

func TestStringSIMILAR_TO(t *testing.T) {
	assertClauseSerialize(t, table3StrCol.SIMILAR_TO(table2ColStr), "(table3.col2 SIMILAR TO table2.colStr)")
	assertClauseSerialize(t, table3StrCol.SIMILAR_TO(String("JOHN")), "(table3.col2 SIMILAR TO $1)", "JOHN")
}

func TestStringNOT_SIMILAR_TO(t *testing.T) {
	assertClauseSerialize(t, table3StrCol.NOT_SIMILAR_TO(table2ColStr), "(table3.col2 NOT SIMILAR TO table2.colStr)")
	assertClauseSerialize(t, table3StrCol.NOT_SIMILAR_TO(String("JOHN")), "(table3.col2 NOT SIMILAR TO $1)", "JOHN")
}

func TestStringExp(t *testing.T) {
	assertClauseSerialize(t, StringExp(table2ColFloat), "table2.colFloat")
	assertClauseSerialize(t, StringExp(table2ColFloat).NOT_LIKE(String("abc")), "(table2.colFloat NOT LIKE $1)", "abc")
}
