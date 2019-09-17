package jet

import "testing"

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
