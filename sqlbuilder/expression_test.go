package sqlbuilder

import (
	"testing"
)

func TestExpressionIS_NULL(t *testing.T) {
	assertClauseSerialize(t, table2Col3.IS_NULL(), "table2.col3 IS NULL")
	assertClauseSerialize(t, table2Col3.ADD(table2Col3).IS_NULL(), "(table2.col3 + table2.col3) IS NULL")
	assertClauseSerializeErr(t, table2Col3.ADD(nil), "nil rhs")
}

func TestExpressionIS_NOT_NULL(t *testing.T) {
	assertClauseSerialize(t, table2Col3.IS_NOT_NULL(), "table2.col3 IS NOT NULL")
	assertClauseSerialize(t, table2Col3.ADD(table2Col3).IS_NOT_NULL(), "(table2.col3 + table2.col3) IS NOT NULL")
}

func TestExpressionIS_DISTINCT_FROM(t *testing.T) {
	assertClauseSerialize(t, table2Col3.IS_DISTINCT_FROM(table2Col4), "(table2.col3 IS DISTINCT FROM table2.col4)")
	assertClauseSerialize(t, table2Col3.ADD(table2Col3).IS_DISTINCT_FROM(Int(23)), "((table2.col3 + table2.col3) IS DISTINCT FROM $1)", int64(23))
}

func TestExpressionIS_NOT_DISTINCT_FROM(t *testing.T) {
	assertClauseSerialize(t, table2Col3.IS_NOT_DISTINCT_FROM(table2Col4), "(table2.col3 IS NOT DISTINCT FROM table2.col4)")
	assertClauseSerialize(t, table2Col3.ADD(table2Col3).IS_NOT_DISTINCT_FROM(Int(23)), "((table2.col3 + table2.col3) IS NOT DISTINCT FROM $1)", int64(23))
}

func TestExpressionCAST_TO_BOOL(t *testing.T) {
	assertClauseSerialize(t, table2Col3.TO_BOOL(), "table2.col3::boolean")
	assertClauseSerialize(t, table2Col3.ADD(table2Col3).TO_BOOL(), "(table2.col3 + table2.col3)::boolean")
}

func TestExpressionCAST_TO_SMALLINT(t *testing.T) {
	assertClauseSerialize(t, table2Col3.TO_SMALLINT(), "table2.col3::smallint")
}

func TestExpressionCAST_TO_INTEGER(t *testing.T) {
	assertClauseSerialize(t, table2Col3.TO_INTEGER(), "table2.col3::integer")
}

func TestExpressionCAST_TO_BIGINT(t *testing.T) {
	assertClauseSerialize(t, table2Col3.TO_BIGINT(), "table2.col3::bigint")
}

func TestExpressionCAST_TO_NUMERIC(t *testing.T) {
	assertClauseSerialize(t, table2Col3.TO_NUMERIC(11, 11), "table2.col3::numeric(11, 11)")
	assertClauseSerialize(t, table2Col3.TO_NUMERIC(11), "table2.col3::numeric(11)")
}

func TestExpressionCAST_TO_REAL(t *testing.T) {
	assertClauseSerialize(t, table2Col3.TO_REAL(), "table2.col3::real")
}

func TestExpressionCAST_TO_DOUBLE(t *testing.T) {
	assertClauseSerialize(t, table2Col3.TO_DOUBLE(), "table2.col3::double precision")
}

func TestExpressionCAST_TO_TEXT(t *testing.T) {
	assertClauseSerialize(t, table2Col3.TO_TEXT(), "table2.col3::text")
}

func TestExpressionCAST_TO_DATE(t *testing.T) {
	assertClauseSerialize(t, table2Col3.TO_DATE(), "table2.col3::date")
}

func TestExpressionCAST_TO_TIME(t *testing.T) {
	assertClauseSerialize(t, table2Col3.TO_TIME(), "table2.col3::time without time zone")
}

func TestExpressionCAST_TO_TIMEZ(t *testing.T) {
	assertClauseSerialize(t, table2Col3.TO_TIMEZ(), "table2.col3::time with time zone")
}

func TestExpressionCAST_TO_TIMESTAMP(t *testing.T) {
	assertClauseSerialize(t, table2Col3.TO_TIMESTAMP(), "table2.col3::timestamp without time zone")
}

func TestExpressionCAST_TO_TIMESTAMPZ(t *testing.T) {
	assertClauseSerialize(t, table2Col3.TO_TIMESTAMPZ(), "table2.col3::timestamp with time zone")
}

func TestIN(t *testing.T) {

	assertClauseSerialize(t, Float(1.11).IN(table1.SELECT(table1Col1)),
		`($1 IN ((
     SELECT table1.col1 AS "table1.col1"
     FROM db.table1
)))`, float64(1.11))

	assertClauseSerialize(t, ROW(Int(12), table1Col1).IN(table2.SELECT(table2Col3, table3Col1)),
		`(ROW($1, table1.col1) IN ((
     SELECT table2.col3 AS "table2.col3",
          table3.col1 AS "table3.col1"
     FROM db.table2
)))`, int64(12))
}

func TestNOT_IN(t *testing.T) {

	assertClauseSerialize(t, Float(1.11).NOT_IN(table1.SELECT(table1Col1)),
		`($1 NOT IN ((
     SELECT table1.col1 AS "table1.col1"
     FROM db.table1
)))`, float64(1.11))

	assertClauseSerialize(t, ROW(Int(12), table1Col1).NOT_IN(table2.SELECT(table2Col3, table3Col1)),
		`(ROW($1, table1.col1) NOT IN ((
     SELECT table2.col3 AS "table2.col3",
          table3.col1 AS "table3.col1"
     FROM db.table2
)))`, int64(12))
}
