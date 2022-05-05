package postgres

import (
	"testing"
)

func TestExpressionCAST_AS(t *testing.T) {
	assertSerialize(t, CAST(Int(11)).AS("text"), `$1::text`, int64(11))
}

func TestExpressionCAST_AS_BOOL(t *testing.T) {
	assertSerialize(t, CAST(Int(1)).AS_BOOL(), "$1::boolean", int64(1))
	assertSerialize(t, CAST(table2Col3).AS_BOOL(), "table2.col3::boolean")
	assertSerialize(t, CAST(table2Col3.ADD(table2Col3)).AS_BOOL(), "(table2.col3 + table2.col3)::boolean")
}

func TestExpressionCAST_AS_SMALLINT(t *testing.T) {
	assertSerialize(t, CAST(table2Col3).AS_SMALLINT(), "table2.col3::smallint")
}

func TestExpressionCAST_AS_INTEGER(t *testing.T) {
	assertSerialize(t, CAST(table2Col3).AS_INTEGER(), "table2.col3::integer")
}

func TestExpressionCAST_AS_BIGINT(t *testing.T) {
	assertSerialize(t, CAST(table2Col3).AS_BIGINT(), "table2.col3::bigint")
}

func TestExpressionCAST_AS_NUMERIC(t *testing.T) {
	assertSerialize(t, CAST(table2Col3).AS_NUMERIC(11, 11), "table2.col3::numeric(11, 11)")
	assertSerialize(t, CAST(table2Col3).AS_NUMERIC(11), "table2.col3::numeric(11)")
}

func TestExpressionCAST_AS_REAL(t *testing.T) {
	assertSerialize(t, CAST(table2Col3).AS_REAL(), "table2.col3::real")
}

func TestExpressionCAST_AS_DOUBLE(t *testing.T) {
	assertSerialize(t, CAST(table2Col3).AS_DOUBLE(), "table2.col3::double precision")
}

func TestExpressionCAST_AS_TEXT(t *testing.T) {
	assertSerialize(t, CAST(table2Col3).AS_TEXT(), "table2.col3::text")
}

func TestExpressionCAST_AS_DATE(t *testing.T) {
	assertSerialize(t, CAST(table2Col3).AS_DATE(), "table2.col3::date")
}

func TestExpressionCAST_AS_TIME(t *testing.T) {
	assertSerialize(t, CAST(table2Col3).AS_TIME(), "table2.col3::time without time zone")
}

func TestExpressionCAST_AS_TIMEZ(t *testing.T) {
	assertSerialize(t, CAST(table2Col3).AS_TIMEZ(), "table2.col3::time with time zone")
}

func TestExpressionCAST_AS_TIMESTAMP(t *testing.T) {
	assertSerialize(t, CAST(table2Col3).AS_TIMESTAMP(), "table2.col3::timestamp without time zone")
}

func TestExpressionCAST_AS_TIMESTAMPZ(t *testing.T) {
	assertSerialize(t, CAST(table2Col3).AS_TIMESTAMPZ(), "table2.col3::timestamp with time zone")
}

func TestExpressionCAST_AS_INTERVAL(t *testing.T) {
	assertSerialize(t, CAST(table2ColTimez).AS_INTERVAL(), "table2.col_timez::interval")
	assertSerialize(t, CAST(Time(20, 11, 10)).AS_INTERVAL(), "$1::time without time zone::interval", "20:11:10")
	assertSerialize(t, table2ColDate.SUB(CAST(Time(20, 11, 10)).AS_INTERVAL()),
		"(table2.col_date - $1::time without time zone::interval)", "20:11:10")
}
