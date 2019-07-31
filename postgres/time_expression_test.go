package postgres

import (
	"github.com/go-jet/jet"
	"testing"
)

var timeVar = Time(10, 20, 0, 0)

func TestTimeExpressionEQ(t *testing.T) {
	jet.AssertPostgreClauseSerialize(t, table1ColTime.EQ(table2ColTime), "(table1.col_time = table2.col_time)")
	jet.AssertPostgreClauseSerialize(t, table1ColTime.EQ(timeVar), "(table1.col_time = $1::time without time zone)", "10:20:00.000")
}

func TestTimeExpressionNOT_EQ(t *testing.T) {
	jet.AssertPostgreClauseSerialize(t, table1ColTime.NOT_EQ(table2ColTime), "(table1.col_time != table2.col_time)")
	jet.AssertPostgreClauseSerialize(t, table1ColTime.NOT_EQ(timeVar), "(table1.col_time != $1::time without time zone)", "10:20:00.000")
}

func TestTimeExpressionIS_DISTINCT_FROM(t *testing.T) {
	jet.AssertPostgreClauseSerialize(t, table1ColTime.IS_DISTINCT_FROM(table2ColTime), "(table1.col_time IS DISTINCT FROM table2.col_time)")
	jet.AssertPostgreClauseSerialize(t, table1ColTime.IS_DISTINCT_FROM(timeVar), "(table1.col_time IS DISTINCT FROM $1::time without time zone)", "10:20:00.000")
}

func TestTimeExpressionIS_NOT_DISTINCT_FROM(t *testing.T) {
	jet.AssertPostgreClauseSerialize(t, table1ColTime.IS_NOT_DISTINCT_FROM(table2ColTime), "(table1.col_time IS NOT DISTINCT FROM table2.col_time)")
	jet.AssertPostgreClauseSerialize(t, table1ColTime.IS_NOT_DISTINCT_FROM(timeVar), "(table1.col_time IS NOT DISTINCT FROM $1::time without time zone)", "10:20:00.000")
}

func TestTimeExpressionLT(t *testing.T) {
	jet.AssertPostgreClauseSerialize(t, table1ColTime.LT(table2ColTime), "(table1.col_time < table2.col_time)")
	jet.AssertPostgreClauseSerialize(t, table1ColTime.LT(timeVar), "(table1.col_time < $1::time without time zone)", "10:20:00.000")
}

func TestTimeExpressionLT_EQ(t *testing.T) {
	jet.AssertPostgreClauseSerialize(t, table1ColTime.LT_EQ(table2ColTime), "(table1.col_time <= table2.col_time)")
	jet.AssertPostgreClauseSerialize(t, table1ColTime.LT_EQ(timeVar), "(table1.col_time <= $1::time without time zone)", "10:20:00.000")
}

func TestTimeExpressionGT(t *testing.T) {
	jet.AssertPostgreClauseSerialize(t, table1ColTime.GT(table2ColTime), "(table1.col_time > table2.col_time)")
	jet.AssertPostgreClauseSerialize(t, table1ColTime.GT(timeVar), "(table1.col_time > $1::time without time zone)", "10:20:00.000")
}

func TestTimeExpressionGT_EQ(t *testing.T) {
	jet.AssertPostgreClauseSerialize(t, table1ColTime.GT_EQ(table2ColTime), "(table1.col_time >= table2.col_time)")
	jet.AssertPostgreClauseSerialize(t, table1ColTime.GT_EQ(timeVar), "(table1.col_time >= $1::time without time zone)", "10:20:00.000")
}

func TestTimeExp(t *testing.T) {
	jet.AssertPostgreClauseSerialize(t, TimeExp(table1ColFloat), "table1.col_float")
	jet.AssertPostgreClauseSerialize(t, TimeExp(table1ColFloat).LT(Time(1, 1, 1, 1)),
		"(table1.col_float < $1::time without time zone)", string("01:01:01.001"))
}
