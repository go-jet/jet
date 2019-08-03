package jet

import "testing"

var timestamp = Timestamp(2000, 1, 31, 10, 20, 0, 0)

func TestTimestampExpressionEQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTimestamp.EQ(table2ColTimestamp), "(table1.col_timestamp = table2.col_timestamp)")
	assertClauseSerialize(t, table1ColTimestamp.EQ(timestamp),
		"(table1.col_timestamp = $1)", "2000-01-31 10:20:00.000")
}

func TestTimestampExpressionNOT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTimestamp.NOT_EQ(table2ColTimestamp), "(table1.col_timestamp != table2.col_timestamp)")
	assertClauseSerialize(t, table1ColTimestamp.NOT_EQ(timestamp), "(table1.col_timestamp != $1)", "2000-01-31 10:20:00.000")
}

func TestTimestampExpressionIS_DISTINCT_FROM(t *testing.T) {
	assertClauseSerialize(t, table1ColTimestamp.IS_DISTINCT_FROM(table2ColTimestamp), "(table1.col_timestamp IS DISTINCT FROM table2.col_timestamp)")
	assertClauseSerialize(t, table1ColTimestamp.IS_DISTINCT_FROM(timestamp), "(table1.col_timestamp IS DISTINCT FROM $1)", "2000-01-31 10:20:00.000")
}

func TestTimestampExpressionIS_NOT_DISTINCT_FROM(t *testing.T) {
	assertClauseSerialize(t, table1ColTimestamp.IS_NOT_DISTINCT_FROM(table2ColTimestamp), "(table1.col_timestamp IS NOT DISTINCT FROM table2.col_timestamp)")
	assertClauseSerialize(t, table1ColTimestamp.IS_NOT_DISTINCT_FROM(timestamp), "(table1.col_timestamp IS NOT DISTINCT FROM $1)", "2000-01-31 10:20:00.000")
}

func TestTimestampExpressionLT(t *testing.T) {
	assertClauseSerialize(t, table1ColTimestamp.LT(table2ColTimestamp), "(table1.col_timestamp < table2.col_timestamp)")
	assertClauseSerialize(t, table1ColTimestamp.LT(timestamp), "(table1.col_timestamp < $1)", "2000-01-31 10:20:00.000")
}

func TestTimestampExpressionLT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTimestamp.LT_EQ(table2ColTimestamp), "(table1.col_timestamp <= table2.col_timestamp)")
	assertClauseSerialize(t, table1ColTimestamp.LT_EQ(timestamp), "(table1.col_timestamp <= $1)", "2000-01-31 10:20:00.000")
}

func TestTimestampExpressionGT(t *testing.T) {
	assertClauseSerialize(t, table1ColTimestamp.GT(table2ColTimestamp), "(table1.col_timestamp > table2.col_timestamp)")
	assertClauseSerialize(t, table1ColTimestamp.GT(timestamp), "(table1.col_timestamp > $1)", "2000-01-31 10:20:00.000")
}

func TestTimestampExpressionGT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTimestamp.GT_EQ(table2ColTimestamp), "(table1.col_timestamp >= table2.col_timestamp)")
	assertClauseSerialize(t, table1ColTimestamp.GT_EQ(timestamp), "(table1.col_timestamp >= $1)", "2000-01-31 10:20:00.000")
}

func TestTimestampExp(t *testing.T) {
	assertClauseSerialize(t, TimestampExp(table1ColFloat), "table1.col_float")
	assertClauseSerialize(t, TimestampExp(table1ColFloat).LT(timestamp),
		"(table1.col_float < $1)", "2000-01-31 10:20:00.000")
}
