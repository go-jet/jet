package jet

import (
	"testing"
)

func TestExpressionIS_NULL(t *testing.T) {
	AssertPostgreClauseSerialize(t, table2Col3.IS_NULL(), "table2.col3 IS NULL")
	AssertPostgreClauseSerialize(t, table2Col3.ADD(table2Col3).IS_NULL(), "(table2.col3 + table2.col3) IS NULL")
	assertClauseSerializeErr(t, table2Col3.ADD(nil), "jet: nil rhs")
}

func TestExpressionIS_NOT_NULL(t *testing.T) {
	AssertPostgreClauseSerialize(t, table2Col3.IS_NOT_NULL(), "table2.col3 IS NOT NULL")
	AssertPostgreClauseSerialize(t, table2Col3.ADD(table2Col3).IS_NOT_NULL(), "(table2.col3 + table2.col3) IS NOT NULL")
}

func TestExpressionIS_DISTINCT_FROM(t *testing.T) {
	AssertPostgreClauseSerialize(t, table2Col3.IS_DISTINCT_FROM(table2Col4), "(table2.col3 IS DISTINCT FROM table2.col4)")
	AssertPostgreClauseSerialize(t, table2Col3.ADD(table2Col3).IS_DISTINCT_FROM(Int(23)), "((table2.col3 + table2.col3) IS DISTINCT FROM $1)", int64(23))
}

func TestExpressionIS_NOT_DISTINCT_FROM(t *testing.T) {
	AssertPostgreClauseSerialize(t, table2Col3.IS_NOT_DISTINCT_FROM(table2Col4), "(table2.col3 IS NOT DISTINCT FROM table2.col4)")
	AssertPostgreClauseSerialize(t, table2Col3.ADD(table2Col3).IS_NOT_DISTINCT_FROM(Int(23)), "((table2.col3 + table2.col3) IS NOT DISTINCT FROM $1)", int64(23))
}

func TestIN(t *testing.T) {

	AssertPostgreClauseSerialize(t, Float(1.11).IN(table1.SELECT(table1Col1)),
		`($1 IN ((
     SELECT table1.col1 AS "table1.col1"
     FROM db.table1
)))`, float64(1.11))

	AssertPostgreClauseSerialize(t, ROW(Int(12), table1Col1).IN(table2.SELECT(table2Col3, table3Col1)),
		`(ROW($1, table1.col1) IN ((
     SELECT table2.col3 AS "table2.col3",
          table3.col1 AS "table3.col1"
     FROM db.table2
)))`, int64(12))
}

func TestNOT_IN(t *testing.T) {

	AssertPostgreClauseSerialize(t, Float(1.11).NOT_IN(table1.SELECT(table1Col1)),
		`($1 NOT IN ((
     SELECT table1.col1 AS "table1.col1"
     FROM db.table1
)))`, float64(1.11))

	AssertPostgreClauseSerialize(t, ROW(Int(12), table1Col1).NOT_IN(table2.SELECT(table2Col3, table3Col1)),
		`(ROW($1, table1.col1) NOT IN ((
     SELECT table2.col3 AS "table2.col3",
          table3.col1 AS "table3.col1"
     FROM db.table2
)))`, int64(12))
}
