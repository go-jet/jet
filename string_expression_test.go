package jet

import (
	"testing"
)

func TestStringEQ(t *testing.T) {
	exp := table3StrCol.EQ(table2ColStr)
	assertPostgreClauseSerialize(t, exp, "(table3.col2 = table2.col_str)")
	exp = table3StrCol.EQ(String("JOHN"))
	assertPostgreClauseSerialize(t, exp, "(table3.col2 = $1)", "JOHN")
}

func TestStringNOT_EQ(t *testing.T) {
	exp := table3StrCol.NOT_EQ(table2ColStr)
	assertPostgreClauseSerialize(t, exp, "(table3.col2 != table2.col_str)")
	assertPostgreClauseSerialize(t, table3StrCol.NOT_EQ(String("JOHN")), "(table3.col2 != $1)", "JOHN")
}

func TestStringExpressionIS_DISTINCT_FROM(t *testing.T) {
	assertPostgreClauseSerialize(t, table3StrCol.IS_DISTINCT_FROM(table2ColStr), "(table3.col2 IS DISTINCT FROM table2.col_str)")
	assertPostgreClauseSerialize(t, table3StrCol.IS_DISTINCT_FROM(String("JOHN")), "(table3.col2 IS DISTINCT FROM $1)", "JOHN")
}

func TestStringExpressionIS_NOT_DISTINCT_FROM(t *testing.T) {
	assertPostgreClauseSerialize(t, table3StrCol.IS_NOT_DISTINCT_FROM(table2ColStr), "(table3.col2 IS NOT DISTINCT FROM table2.col_str)")
	assertPostgreClauseSerialize(t, table3StrCol.IS_NOT_DISTINCT_FROM(String("JOHN")), "(table3.col2 IS NOT DISTINCT FROM $1)", "JOHN")
}

func TestStringGT(t *testing.T) {
	exp := table3StrCol.GT(table2ColStr)
	assertPostgreClauseSerialize(t, exp, "(table3.col2 > table2.col_str)")
	assertPostgreClauseSerialize(t, table3StrCol.GT(String("JOHN")), "(table3.col2 > $1)", "JOHN")
}

func TestStringGT_EQ(t *testing.T) {
	exp := table3StrCol.GT_EQ(table2ColStr)
	assertPostgreClauseSerialize(t, exp, "(table3.col2 >= table2.col_str)")
	assertPostgreClauseSerialize(t, table3StrCol.GT_EQ(String("JOHN")), "(table3.col2 >= $1)", "JOHN")
}

func TestStringLT(t *testing.T) {
	exp := table3StrCol.LT(table2ColStr)
	assertPostgreClauseSerialize(t, exp, "(table3.col2 < table2.col_str)")
	assertPostgreClauseSerialize(t, table3StrCol.LT(String("JOHN")), "(table3.col2 < $1)", "JOHN")
}

func TestStringLT_EQ(t *testing.T) {
	exp := table3StrCol.LT_EQ(table2ColStr)
	assertPostgreClauseSerialize(t, exp, "(table3.col2 <= table2.col_str)")
	assertPostgreClauseSerialize(t, table3StrCol.LT_EQ(String("JOHN")), "(table3.col2 <= $1)", "JOHN")
}

func TestStringCONCAT(t *testing.T) {
	assertPostgreClauseSerialize(t, table3StrCol.CONCAT(table2ColStr), "(table3.col2 || table2.col_str)")
	assertPostgreClauseSerialize(t, table3StrCol.CONCAT(String("JOHN")), "(table3.col2 || $1)", "JOHN")
}

func TestStringLIKE(t *testing.T) {
	assertPostgreClauseSerialize(t, table3StrCol.LIKE(table2ColStr), "(table3.col2 LIKE table2.col_str)")
	assertPostgreClauseSerialize(t, table3StrCol.LIKE(String("JOHN")), "(table3.col2 LIKE $1)", "JOHN")
}

func TestStringNOT_LIKE(t *testing.T) {
	assertPostgreClauseSerialize(t, table3StrCol.NOT_LIKE(table2ColStr), "(table3.col2 NOT LIKE table2.col_str)")
	assertPostgreClauseSerialize(t, table3StrCol.NOT_LIKE(String("JOHN")), "(table3.col2 NOT LIKE $1)", "JOHN")
}

func TestStringSIMILAR_TO(t *testing.T) {
	assertPostgreClauseSerialize(t, table3StrCol.SIMILAR_TO(table2ColStr), "(table3.col2 SIMILAR TO table2.col_str)")
	assertPostgreClauseSerialize(t, table3StrCol.SIMILAR_TO(String("JOHN")), "(table3.col2 SIMILAR TO $1)", "JOHN")
}

func TestStringNOT_SIMILAR_TO(t *testing.T) {
	assertPostgreClauseSerialize(t, table3StrCol.NOT_SIMILAR_TO(table2ColStr), "(table3.col2 NOT SIMILAR TO table2.col_str)")
	assertPostgreClauseSerialize(t, table3StrCol.NOT_SIMILAR_TO(String("JOHN")), "(table3.col2 NOT SIMILAR TO $1)", "JOHN")
}

func TestStringExp(t *testing.T) {
	assertPostgreClauseSerialize(t, StringExp(table2ColFloat), "table2.col_float")
	assertPostgreClauseSerialize(t, StringExp(table2ColFloat).NOT_LIKE(String("abc")), "(table2.col_float NOT LIKE $1)", "abc")
}
