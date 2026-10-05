package jet

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFrameExtent(t *testing.T) {
	assertClauseSerialize(t, PRECEDING(Int(2)), "$1 PRECEDING", int64(2))
	assertClauseSerialize(t, FOLLOWING(Int(4)), "$1 FOLLOWING", int64(4))
}

func TestWindowFunctions(t *testing.T) {
	assertClauseSerialize(t, PARTITION_BY(table1Col1), "(PARTITION BY table1.col1)")
	assertClauseSerialize(t, PARTITION_BY(table1Col3).ORDER_BY(table1Col1), "(PARTITION BY table1.col3 ORDER BY table1.col1)")
	assertClauseSerialize(t, ORDER_BY(table1Col1), "(ORDER BY table1.col1)")
	assertClauseSerialize(t, ORDER_BY(table1Col1).ROWS(PRECEDING(Int(1))), "(ORDER BY table1.col1 ROWS $1 PRECEDING)", int64(1))
	assertClauseSerialize(t, ORDER_BY(table1Col1).ROWS(PRECEDING(Int(1)), FOLLOWING(Int(33))),
		"(ORDER BY table1.col1 ROWS BETWEEN $1 PRECEDING AND $2 FOLLOWING)", int64(1), int64(33))
	assertClauseSerialize(t, ORDER_BY(table1Col1).RANGE(PRECEDING(UNBOUNDED), FOLLOWING(UNBOUNDED)),
		"(ORDER BY table1.col1 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)")
	assertClauseSerialize(t, ORDER_BY(table1Col1).RANGE(PRECEDING(UNBOUNDED), CURRENT_ROW),
		"(ORDER BY table1.col1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)")
}

func TestWindowFunctionChainedOperators(t *testing.T) {
	assertClauseSerialize(t,
		ROW_NUMBER().OVER(ORDER_BY(table1ColInt)).ADD(Int(1)),
		"(ROW_NUMBER() OVER (ORDER BY table1.col_int) + $1)", int64(1))
	assertClauseSerialize(t,
		SUMf(table1ColFloat).OVER(PARTITION_BY(table1ColInt)).SUB(
			SUMf(table2ColFloat).OVER(PARTITION_BY(table1ColInt)),
		),
		"(SUM(table1.col_float) OVER (PARTITION BY table1.col_int) - SUM(table2.col_float) OVER (PARTITION BY table1.col_int))")
	assertClauseSerialize(t,
		BOOL_AND(table1ColBool).OVER(PARTITION_BY(table1ColInt)).AND(table2ColBool),
		"(BOOL_AND(table1.col_bool) OVER (PARTITION BY table1.col_int) AND table2.col_bool)")
}

func TestWindowFunctionExpressionMethods(t *testing.T) {
	assertProjectionSerialize(t, ROW_NUMBER().OVER(), "ROW_NUMBER() OVER ()")
	assertProjectionSerialize(t, SUMf(table1ColFloat).OVER(PARTITION_BY(table1ColInt)).AS("sum"),
		`SUM(table1.col_float) OVER (PARTITION BY table1.col_int) AS "sum"`)
	assertProjectionSerialize(t, FIRST_VALUE(table1ColInt).OVER().AS("first"), `FIRST_VALUE(table1.col_int) OVER () AS "first"`)
	assertClauseSerialize(t, BOOL_OR(table1ColBool).OVER().IS_NULL(), "(BOOL_OR(table1.col_bool) OVER () IS NULL)")
	assertClauseSerialize(t, ROW_NUMBER().OVER().IN(Int(1), Int(2)), "(ROW_NUMBER() OVER () IN ($1, $2))", int64(1), int64(2))

	out := SQLBuilder{Dialect: defaultDialect}
	SUMf(table1ColFloat).OVER().DESC().serializeForOrderBy(SelectStatementType, &out)
	require.Equal(t, "SUM(table1.col_float) OVER () DESC", out.Buff.String())
}
