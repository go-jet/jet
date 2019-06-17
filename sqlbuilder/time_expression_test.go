package sqlbuilder

import (
	"testing"
)

func TestTimeExpressionEQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTime.EQ(table2ColTime), "(table1.col_time = table2.col_time)")
	assertClauseSerialize(t, table1ColTime.EQ(Time(10, 20, 0, 0)), "(table1.col_time = $1::time without time zone)", "10:20:00.000")
}

func TestTimeExpressionNOT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTime.NOT_EQ(table2ColTime), "(table1.col_time != table2.col_time)")
	assertClauseSerialize(t, table1ColTime.NOT_EQ(Time(10, 20, 0, 0)), "(table1.col_time != $1::time without time zone)", "10:20:00.000")
}

func TestTimeExpressionLT(t *testing.T) {
	assertClauseSerialize(t, table1ColTime.LT(table2ColTime), "(table1.col_time < table2.col_time)")
	assertClauseSerialize(t, table1ColTime.LT(Time(10, 20, 0, 0)), "(table1.col_time < $1::time without time zone)", "10:20:00.000")
}

func TestTimeExpressionLT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTime.LT_EQ(table2ColTime), "(table1.col_time <= table2.col_time)")
	assertClauseSerialize(t, table1ColTime.LT_EQ(Time(10, 20, 0, 0)), "(table1.col_time <= $1::time without time zone)", "10:20:00.000")
}

func TestTimeExpressionGT(t *testing.T) {
	assertClauseSerialize(t, table1ColTime.GT(table2ColTime), "(table1.col_time > table2.col_time)")
	assertClauseSerialize(t, table1ColTime.GT(Time(10, 20, 0, 0)), "(table1.col_time > $1::time without time zone)", "10:20:00.000")
}

func TestTimeExpressionGT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTime.GT_EQ(table2ColTime), "(table1.col_time >= table2.col_time)")
	assertClauseSerialize(t, table1ColTime.GT_EQ(Time(10, 20, 0, 0)), "(table1.col_time >= $1::time without time zone)", "10:20:00.000")
}

func TestTimeExp(t *testing.T) {
	assertClauseSerialize(t, TimeExp(table1ColFloat), "table1.col_float")
	assertClauseSerialize(t, TimeExp(table1ColFloat).LT(Time(1, 1, 1, 1)),
		"(table1.col_float < $1::time without time zone)", string("01:01:01.001"))
}
