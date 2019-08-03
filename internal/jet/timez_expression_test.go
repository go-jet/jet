// +build TODO

package jet

import "testing"

var timezVar = Timez(10, 20, 0, 0, 4)

func TestTimezExpressionEQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTimez.EQ(table2ColTimez), "(table1.col_timez = table2.col_timez)")
	assertClauseSerialize(t, table1ColTimez.EQ(timezVar), "(table1.col_timez = $1::time with time zone)", "10:20:00.000 +04")
}

func TestTimezExpressionNOT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTimez.NOT_EQ(table2ColTimez), "(table1.col_timez != table2.col_timez)")
	assertClauseSerialize(t, table1ColTimez.NOT_EQ(timezVar), "(table1.col_timez != $1::time with time zone)", "10:20:00.000 +04")
}

func TestTimezExpressionIS_DISTINCT_FROM(t *testing.T) {
	assertClauseSerialize(t, table1ColTimez.IS_DISTINCT_FROM(table2ColTimez), "(table1.col_timez IS DISTINCT FROM table2.col_timez)")
	assertClauseSerialize(t, table1ColTimez.IS_DISTINCT_FROM(timezVar), "(table1.col_timez IS DISTINCT FROM $1::time with time zone)", "10:20:00.000 +04")
}

func TestTimezExpressionIS_NOT_DISTINCT_FROM(t *testing.T) {
	assertClauseSerialize(t, table1ColTimez.IS_NOT_DISTINCT_FROM(table2ColTimez), "(table1.col_timez IS NOT DISTINCT FROM table2.col_timez)")
	assertClauseSerialize(t, table1ColTimez.IS_NOT_DISTINCT_FROM(timezVar), "(table1.col_timez IS NOT DISTINCT FROM $1::time with time zone)", "10:20:00.000 +04")
}

func TestTimezExpressionLT(t *testing.T) {
	assertClauseSerialize(t, table1ColTimez.LT(table2ColTimez), "(table1.col_timez < table2.col_timez)")
	assertClauseSerialize(t, table1ColTimez.LT(timezVar), "(table1.col_timez < $1::time with time zone)", "10:20:00.000 +04")
}

func TestTimezExpressionLT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTimez.LT_EQ(table2ColTimez), "(table1.col_timez <= table2.col_timez)")
	assertClauseSerialize(t, table1ColTimez.LT_EQ(timezVar), "(table1.col_timez <= $1::time with time zone)", "10:20:00.000 +04")
}

func TestTimezExpressionGT(t *testing.T) {
	assertClauseSerialize(t, table1ColTimez.GT(table2ColTimez), "(table1.col_timez > table2.col_timez)")
	assertClauseSerialize(t, table1ColTimez.GT(timezVar), "(table1.col_timez > $1::time with time zone)", "10:20:00.000 +04")
}

func TestTimezExpressionGT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTimez.GT_EQ(table2ColTimez), "(table1.col_timez >= table2.col_timez)")
	assertClauseSerialize(t, table1ColTimez.GT_EQ(timezVar), "(table1.col_timez >= $1::time with time zone)", "10:20:00.000 +04")
}

func TestTimezExp(t *testing.T) {
	assertClauseSerialize(t, TimezExp(table1ColFloat), "table1.col_float")
	assertClauseSerialize(t, TimezExp(table1ColFloat).LT(Timez(1, 1, 1, 1, 4)),
		"(table1.col_float < $1::time with time zone)", string("01:01:01.001 +04"))
}
