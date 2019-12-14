package jet

import (
	"testing"
	"time"
)

var timestampz = Timestampz(2000, 1, 31, 10, 20, 5, 23*time.Microsecond, "+200")

func TestTimestampzExpressionEQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTimestampz.EQ(table2ColTimestampz), "(table1.col_timestampz = table2.col_timestampz)")
	assertClauseSerialize(t, table1ColTimestampz.EQ(timestampz),
		"(table1.col_timestampz = $1)", "2000-01-31 10:20:05.000023 +200")
}

func TestTimestampzExpressionNOT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTimestampz.NOT_EQ(table2ColTimestampz), "(table1.col_timestampz != table2.col_timestampz)")
	assertClauseSerialize(t, table1ColTimestampz.NOT_EQ(timestampz), "(table1.col_timestampz != $1)", "2000-01-31 10:20:05.000023 +200")
}

func TestTimestampzExpressionIS_DISTINCT_FROM(t *testing.T) {
	assertClauseSerialize(t, table1ColTimestampz.IS_DISTINCT_FROM(table2ColTimestampz), "(table1.col_timestampz IS DISTINCT FROM table2.col_timestampz)")
	assertClauseSerialize(t, table1ColTimestampz.IS_DISTINCT_FROM(timestampz), "(table1.col_timestampz IS DISTINCT FROM $1)", "2000-01-31 10:20:05.000023 +200")
}

func TestTimestampzExpressionIS_NOT_DISTINCT_FROM(t *testing.T) {
	assertClauseSerialize(t, table1ColTimestampz.IS_NOT_DISTINCT_FROM(table2ColTimestampz), "(table1.col_timestampz IS NOT DISTINCT FROM table2.col_timestampz)")
	assertClauseSerialize(t, table1ColTimestampz.IS_NOT_DISTINCT_FROM(timestampz), "(table1.col_timestampz IS NOT DISTINCT FROM $1)", "2000-01-31 10:20:05.000023 +200")
}

func TestTimestampzExpressionLT(t *testing.T) {
	assertClauseSerialize(t, table1ColTimestampz.LT(table2ColTimestampz), "(table1.col_timestampz < table2.col_timestampz)")
	assertClauseSerialize(t, table1ColTimestampz.LT(timestampz), "(table1.col_timestampz < $1)", "2000-01-31 10:20:05.000023 +200")
}

func TestTimestampzExpressionLT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTimestampz.LT_EQ(table2ColTimestampz), "(table1.col_timestampz <= table2.col_timestampz)")
	assertClauseSerialize(t, table1ColTimestampz.LT_EQ(timestampz), "(table1.col_timestampz <= $1)", "2000-01-31 10:20:05.000023 +200")
}

func TestTimestampzExpressionGT(t *testing.T) {
	assertClauseSerialize(t, table1ColTimestampz.GT(table2ColTimestampz), "(table1.col_timestampz > table2.col_timestampz)")
	assertClauseSerialize(t, table1ColTimestampz.GT(timestampz), "(table1.col_timestampz > $1)", "2000-01-31 10:20:05.000023 +200")
}

func TestTimestampzExpressionGT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTimestampz.GT_EQ(table2ColTimestampz), "(table1.col_timestampz >= table2.col_timestampz)")
	assertClauseSerialize(t, table1ColTimestampz.GT_EQ(timestampz), "(table1.col_timestampz >= $1)", "2000-01-31 10:20:05.000023 +200")
}

func TestTimestampzExp(t *testing.T) {
	assertClauseSerialize(t, TimestampzExp(table1ColFloat), "table1.col_float")
	assertClauseSerialize(t, TimestampzExp(table1ColFloat).LT(timestampz),
		"(table1.col_float < $1)", "2000-01-31 10:20:05.000023 +200")
}

func TestTimestampzArithmetic(t *testing.T) {
	timestampz := Timestampz(2000, 1, 1, 0, 0, 0, 100, "UTC")
	assertClauseDebugSerialize(t, table1ColTimestampz.ADD(NewInterval(String("1 HOUR"))).EQ(timestampz),
		"((table1.col_timestampz + INTERVAL '1 HOUR') = '2000-01-01 00:00:00.0000001 UTC')")
	assertClauseDebugSerialize(t, table1ColTimestampz.SUB(NewInterval(String("1 HOUR"))).EQ(timestampz),
		"((table1.col_timestampz - INTERVAL '1 HOUR') = '2000-01-01 00:00:00.0000001 UTC')")
}
