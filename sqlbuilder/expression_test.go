package sqlbuilder

import (
	"testing"
)

func TestExpressionIS_NULL(t *testing.T) {
	assertExpressionSerialize(t, table2Col3.IS_NULL(), "table2.col3 IS NULL")
	assertExpressionSerialize(t, table2Col3.ADD(table2Col3).IS_NULL(), "(table2.col3 + table2.col3) IS NULL")
}

func TestExpressionIS_NOT_NULL(t *testing.T) {
	assertExpressionSerialize(t, table2Col3.IS_NOT_NULL(), "table2.col3 IS NOT NULL")
	assertExpressionSerialize(t, table2Col3.ADD(table2Col3).IS_NOT_NULL(), "(table2.col3 + table2.col3) IS NOT NULL")
}

func TestExpressionIS_DISTINCT_FROM(t *testing.T) {
	assertExpressionSerialize(t, table2Col3.IS_DISTINCT_FROM(table2Col4), "(table2.col3 IS DISTINCT FROM table2.col4)")
	assertExpressionSerialize(t, table2Col3.ADD(table2Col3).IS_DISTINCT_FROM(Int(23)), "((table2.col3 + table2.col3) IS DISTINCT FROM $1)", int64(23))
}

func TestExpressionIS_NOT_DISTINCT_FROM(t *testing.T) {
	assertExpressionSerialize(t, table2Col3.IS_NOT_DISTINCT_FROM(table2Col4), "(table2.col3 IS NOT DISTINCT FROM table2.col4)")
	assertExpressionSerialize(t, table2Col3.ADD(table2Col3).IS_NOT_DISTINCT_FROM(Int(23)), "((table2.col3 + table2.col3) IS NOT DISTINCT FROM $1)", int64(23))
}

func TestExpressionCAST_TO_BOOL(t *testing.T) {
	assertExpressionSerialize(t, table2Col3.CAST_TO_BOOL(), "table2.col3::boolean")
	assertExpressionSerialize(t, table2Col3.ADD(table2Col3).CAST_TO_BOOL(), "(table2.col3 + table2.col3)::boolean")
}

func TestExpressionCAST_TO_INTEGER(t *testing.T) {
	assertExpressionSerialize(t, table2Col3.CAST_TO_INTEGER(), "table2.col3::integer")
}

func TestExpressionCAST_TO_DOUBLE(t *testing.T) {
	assertExpressionSerialize(t, table2Col3.CAST_TO_DOUBLE(), "table2.col3::double precision")
}

func TestExpressionCAST_TO_TEXT(t *testing.T) {
	assertExpressionSerialize(t, table2Col3.CAST_TO_TEXT(), "table2.col3::text")
}

func TestExpressionCAST_TO_DATE(t *testing.T) {
	assertExpressionSerialize(t, table2Col3.CAST_TO_DATE(), "table2.col3::date")
}

func TestExpressionCAST_TO_TIME(t *testing.T) {
	assertExpressionSerialize(t, table2Col3.CAST_TO_TIME(), "table2.col3::time without time zone")
}

func TestExpressionCAST_TO_TIMEZ(t *testing.T) {
	assertExpressionSerialize(t, table2Col3.CAST_TO_TIMEZ(), "table2.col3::time with time zone")
}

func TestExpressionCAST_TO_TIMESTAMP(t *testing.T) {
	assertExpressionSerialize(t, table2Col3.CAST_TO_TIMESTAMP(), "table2.col3::timestamp without time zone")
}

func TestExpressionCAST_TO_TIMESTAMPZ(t *testing.T) {
	assertExpressionSerialize(t, table2Col3.CAST_TO_TIMESTAMPZ(), "table2.col3::timestamp with time zone")
}
