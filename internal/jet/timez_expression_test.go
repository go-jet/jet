package jet

import (
	"testing"
)

var timezVar = Timez(10, 20, 0, 0, "+4:00")

func TestTimezExpressionEQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTimez.EQ(table2ColTimez), "(table1.col_timez = table2.col_timez)")
	assertClauseSerialize(t, table1ColTimez.EQ(timezVar), "(table1.col_timez = $1)", "10:20:00 +4:00")
}

func TestTimezExpressionNOT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTimez.NOT_EQ(table2ColTimez), "(table1.col_timez != table2.col_timez)")
	assertClauseSerialize(t, table1ColTimez.NOT_EQ(timezVar), "(table1.col_timez != $1)", "10:20:00 +4:00")
}

func TestTimezExpressionIS_DISTINCT_FROM(t *testing.T) {
	assertClauseSerialize(t, table1ColTimez.IS_DISTINCT_FROM(table2ColTimez), "(table1.col_timez IS DISTINCT FROM table2.col_timez)")
	assertClauseSerialize(t, table1ColTimez.IS_DISTINCT_FROM(timezVar), "(table1.col_timez IS DISTINCT FROM $1)", "10:20:00 +4:00")
}

func TestTimezExpressionIS_NOT_DISTINCT_FROM(t *testing.T) {
	assertClauseSerialize(t, table1ColTimez.IS_NOT_DISTINCT_FROM(table2ColTimez), "(table1.col_timez IS NOT DISTINCT FROM table2.col_timez)")
	assertClauseSerialize(t, table1ColTimez.IS_NOT_DISTINCT_FROM(timezVar), "(table1.col_timez IS NOT DISTINCT FROM $1)", "10:20:00 +4:00")
}

func TestTimezExpressionLT(t *testing.T) {
	assertClauseSerialize(t, table1ColTimez.LT(table2ColTimez), "(table1.col_timez < table2.col_timez)")
	assertClauseSerialize(t, table1ColTimez.LT(timezVar), "(table1.col_timez < $1)", "10:20:00 +4:00")
}

func TestTimezExpressionLT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTimez.LT_EQ(table2ColTimez), "(table1.col_timez <= table2.col_timez)")
	assertClauseSerialize(t, table1ColTimez.LT_EQ(timezVar), "(table1.col_timez <= $1)", "10:20:00 +4:00")
}

func TestTimezExpressionGT(t *testing.T) {
	assertClauseSerialize(t, table1ColTimez.GT(table2ColTimez), "(table1.col_timez > table2.col_timez)")
	assertClauseSerialize(t, table1ColTimez.GT(timezVar), "(table1.col_timez > $1)", "10:20:00 +4:00")
}

func TestTimezExpressionGT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTimez.GT_EQ(table2ColTimez), "(table1.col_timez >= table2.col_timez)")
	assertClauseSerialize(t, table1ColTimez.GT_EQ(timezVar), "(table1.col_timez >= $1)", "10:20:00 +4:00")
}

func TestTimezExp(t *testing.T) {
	assertClauseSerialize(t, TimezExp(table1ColFloat), "table1.col_float")
	assertClauseSerialize(t, TimezExp(table1ColFloat).LT(Timez(1, 1, 1, 1, "+4:00")),
		"(table1.col_float < $1)", string("01:01:01.000000001 +4:00"))
}

func TestTimezArithmetic(t *testing.T) {
	timez := Timez(0, 0, 0, 100, "UTC")
	assertClauseDebugSerialize(t, table1ColTimez.ADD(NewInterval(String("1 HOUR"))).EQ(timez),
		"((table1.col_timez + INTERVAL '1 HOUR') = '00:00:00.0000001 UTC')")
	assertClauseDebugSerialize(t, table1ColTimez.SUB(NewInterval(String("1 HOUR"))).EQ(timez),
		"((table1.col_timez - INTERVAL '1 HOUR') = '00:00:00.0000001 UTC')")
}
