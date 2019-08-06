package postgres

import "testing"

func TestString_REGEXP_LIKE_operator(t *testing.T) {
	assertClauseSerialize(t, table3StrCol.REGEXP_LIKE(table2ColStr), "table3.col2 ~* table2.col_str")
	assertClauseSerialize(t, table3StrCol.REGEXP_LIKE(String("JOHN"), "c"), "table3.col2 ~ $1", "JOHN")
	assertClauseSerialize(t, table3StrCol.REGEXP_LIKE(String("JOHN"), "i"), "table3.col2 ~* $1", "JOHN")
}

func TestString_REGEXP_LIKE_function(t *testing.T) {
	assertClauseSerialize(t, REGEXP_LIKE(table3StrCol, table2ColStr), "table3.col2 ~* table2.col_str")
	assertClauseSerialize(t, REGEXP_LIKE(table3StrCol, String("JOHN"), "c"), "table3.col2 ~ $1", "JOHN")
	assertClauseSerialize(t, REGEXP_LIKE(table3StrCol, String("JOHN"), "i"), "table3.col2 ~* $1", "JOHN")
}
