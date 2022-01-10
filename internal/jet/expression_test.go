package jet

import (
	"testing"
)

func TestExpressionIS_NULL(t *testing.T) {
	assertClauseSerialize(t, table2Col3.IS_NULL(), "table2.col3 IS NULL")
	assertClauseSerialize(t, table2Col3.ADD(table2Col3).IS_NULL(), "(table2.col3 + table2.col3) IS NULL")
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

func TestIN(t *testing.T) {
	assertClauseSerialize(t, table2ColInt.IN(Int(1), Int(2), Int(3)),
		`(table2.col_int IN ($1, $2, $3))`, int64(1), int64(2), int64(3))

}

func TestNOT_IN(t *testing.T) {

	assertClauseSerialize(t, table2ColInt.NOT_IN(Int(1), Int(2), Int(3)),
		`(table2.col_int NOT IN ($1, $2, $3))`, int64(1), int64(2), int64(3))

}
