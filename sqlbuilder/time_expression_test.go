package sqlbuilder

import (
	"testing"
)

func TestTimeExpressionEQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTime.EQ(table2ColTime), "(table1.colTime = table2.colTime)")
	assertClauseSerialize(t, table1ColTime.EQ(Time(10, 20, 0, 0)), "(table1.colTime = $1::time without time zone)", "10:20:00.000")
}

func TestTimeExpressionNOT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTime.NOT_EQ(table2ColTime), "(table1.colTime != table2.colTime)")
	assertClauseSerialize(t, table1ColTime.NOT_EQ(Time(10, 20, 0, 0)), "(table1.colTime != $1::time without time zone)", "10:20:00.000")
}

func TestTimeExpressionLT(t *testing.T) {
	assertClauseSerialize(t, table1ColTime.LT(table2ColTime), "(table1.colTime < table2.colTime)")
	assertClauseSerialize(t, table1ColTime.LT(Time(10, 20, 0, 0)), "(table1.colTime < $1::time without time zone)", "10:20:00.000")
}

func TestTimeExpressionLT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTime.LT_EQ(table2ColTime), "(table1.colTime <= table2.colTime)")
	assertClauseSerialize(t, table1ColTime.LT_EQ(Time(10, 20, 0, 0)), "(table1.colTime <= $1::time without time zone)", "10:20:00.000")
}

func TestTimeExpressionGT(t *testing.T) {
	assertClauseSerialize(t, table1ColTime.GT(table2ColTime), "(table1.colTime > table2.colTime)")
	assertClauseSerialize(t, table1ColTime.GT(Time(10, 20, 0, 0)), "(table1.colTime > $1::time without time zone)", "10:20:00.000")
}

func TestTimeExpressionGT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTime.GT_EQ(table2ColTime), "(table1.colTime >= table2.colTime)")
	assertClauseSerialize(t, table1ColTime.GT_EQ(Time(10, 20, 0, 0)), "(table1.colTime >= $1::time without time zone)", "10:20:00.000")
}

func TestTimeExp(t *testing.T) {
	assertClauseSerialize(t, TimeExp(table1ColFloat), "table1.colFloat")
	assertClauseSerialize(t, TimeExp(table1ColFloat).LT(Time(1, 1, 1, 1)),
		"(table1.colFloat < $1::time without time zone)", string("01:01:01.001"))
}
