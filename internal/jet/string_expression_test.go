package jet

import (
	"testing"
)

func TestStringEQ(t *testing.T) {
	exp := table3StrCol.EQ(table2ColStr)
	assertClauseSerialize(t, exp, "(table3.col2 = table2.col_str)")
	exp = table3StrCol.EQ(String("JOHN"))
	assertClauseSerialize(t, exp, "(table3.col2 = $1)", "JOHN")
}

func TestStringNOT_EQ(t *testing.T) {
	exp := table3StrCol.NOT_EQ(table2ColStr)
	assertClauseSerialize(t, exp, "(table3.col2 != table2.col_str)")
	assertClauseSerialize(t, table3StrCol.NOT_EQ(String("JOHN")), "(table3.col2 != $1)", "JOHN")
}

func TestStringExpressionIS_DISTINCT_FROM(t *testing.T) {
	assertClauseSerialize(t, table3StrCol.IS_DISTINCT_FROM(table2ColStr), "(table3.col2 IS DISTINCT FROM table2.col_str)")
	assertClauseSerialize(t, table3StrCol.IS_DISTINCT_FROM(String("JOHN")), "(table3.col2 IS DISTINCT FROM $1)", "JOHN")
}

func TestStringExpressionIS_NOT_DISTINCT_FROM(t *testing.T) {
	assertClauseSerialize(t, table3StrCol.IS_NOT_DISTINCT_FROM(table2ColStr), "(table3.col2 IS NOT DISTINCT FROM table2.col_str)")
	assertClauseSerialize(t, table3StrCol.IS_NOT_DISTINCT_FROM(String("JOHN")), "(table3.col2 IS NOT DISTINCT FROM $1)", "JOHN")
}

func TestStringGT(t *testing.T) {
	exp := table3StrCol.GT(table2ColStr)
	assertClauseSerialize(t, exp, "(table3.col2 > table2.col_str)")
	assertClauseSerialize(t, table3StrCol.GT(String("JOHN")), "(table3.col2 > $1)", "JOHN")
}

func TestStringGT_EQ(t *testing.T) {
	exp := table3StrCol.GT_EQ(table2ColStr)
	assertClauseSerialize(t, exp, "(table3.col2 >= table2.col_str)")
	assertClauseSerialize(t, table3StrCol.GT_EQ(String("JOHN")), "(table3.col2 >= $1)", "JOHN")
}

func TestStringLT(t *testing.T) {
	exp := table3StrCol.LT(table2ColStr)
	assertClauseSerialize(t, exp, "(table3.col2 < table2.col_str)")
	assertClauseSerialize(t, table3StrCol.LT(String("JOHN")), "(table3.col2 < $1)", "JOHN")
}

func TestStringLT_EQ(t *testing.T) {
	exp := table3StrCol.LT_EQ(table2ColStr)
	assertClauseSerialize(t, exp, "(table3.col2 <= table2.col_str)")
	assertClauseSerialize(t, table3StrCol.LT_EQ(String("JOHN")), "(table3.col2 <= $1)", "JOHN")
}

func TestStringCONCAT(t *testing.T) {
	assertClauseSerialize(t, table3StrCol.CONCAT(table2ColStr), "(table3.col2 || table2.col_str)")
	assertClauseSerialize(t, table3StrCol.CONCAT(String("JOHN")), "(table3.col2 || $1)", "JOHN")
}

func TestStringLIKE(t *testing.T) {
	assertClauseSerialize(t, table3StrCol.LIKE(table2ColStr), "(table3.col2 LIKE table2.col_str)")
	assertClauseSerialize(t, table3StrCol.LIKE(String("JOHN")), "(table3.col2 LIKE $1)", "JOHN")
}

func TestStringNOT_LIKE(t *testing.T) {
	assertClauseSerialize(t, table3StrCol.NOT_LIKE(table2ColStr), "(table3.col2 NOT LIKE table2.col_str)")
	assertClauseSerialize(t, table3StrCol.NOT_LIKE(String("JOHN")), "(table3.col2 NOT LIKE $1)", "JOHN")
}

func TestStringREGEXP_LIKE(t *testing.T) {
	assertClauseSerialize(t, table3StrCol.REGEXP_LIKE(table2ColStr), "(table3.col2 REGEXP table2.col_str)")
	assertClauseSerialize(t, table3StrCol.REGEXP_LIKE(String("JOHN"), true), "(table3.col2 REGEXP $1)", "JOHN")
}

func TestStringNOT_REGEXP_LIKE(t *testing.T) {
	assertClauseSerialize(t, table3StrCol.NOT_REGEXP_LIKE(table2ColStr), "(table3.col2 NOT REGEXP table2.col_str)")
	assertClauseSerialize(t, table3StrCol.NOT_REGEXP_LIKE(String("JOHN"), true), "(table3.col2 NOT REGEXP $1)", "JOHN")
}

func TestStringExp(t *testing.T) {
	assertClauseSerialize(t, StringExp(table2ColFloat), "table2.col_float")
	assertClauseSerialize(t, StringExp(table2ColFloat).NOT_LIKE(String("abc")), "(table2.col_float NOT LIKE $1)", "abc")
}
