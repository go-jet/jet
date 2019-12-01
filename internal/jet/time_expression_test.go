package jet

import (
	"testing"
	"time"
)

var timeVar = Time(10, 20, 0, 0)

func TestTimeExpressionEQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTime.EQ(table2ColTime), "(table1.col_time = table2.col_time)")
	assertClauseSerialize(t, table1ColTime.EQ(timeVar), "(table1.col_time = $1)", "10:20:00")
}

func TestTimeExpressionNOT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTime.NOT_EQ(table2ColTime), "(table1.col_time != table2.col_time)")
	assertClauseSerialize(t, table1ColTime.NOT_EQ(timeVar), "(table1.col_time != $1)", "10:20:00")
}

func TestTimeExpressionIS_DISTINCT_FROM(t *testing.T) {
	assertClauseSerialize(t, table1ColTime.IS_DISTINCT_FROM(table2ColTime), "(table1.col_time IS DISTINCT FROM table2.col_time)")
	assertClauseSerialize(t, table1ColTime.IS_DISTINCT_FROM(timeVar), "(table1.col_time IS DISTINCT FROM $1)", "10:20:00")
}

func TestTimeExpressionIS_NOT_DISTINCT_FROM(t *testing.T) {
	assertClauseSerialize(t, table1ColTime.IS_NOT_DISTINCT_FROM(table2ColTime), "(table1.col_time IS NOT DISTINCT FROM table2.col_time)")
	assertClauseSerialize(t, table1ColTime.IS_NOT_DISTINCT_FROM(timeVar), "(table1.col_time IS NOT DISTINCT FROM $1)", "10:20:00")
}

func TestTimeExpressionLT(t *testing.T) {
	assertClauseSerialize(t, table1ColTime.LT(table2ColTime), "(table1.col_time < table2.col_time)")
	assertClauseSerialize(t, table1ColTime.LT(timeVar), "(table1.col_time < $1)", "10:20:00")
}

func TestTimeExpressionLT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTime.LT_EQ(table2ColTime), "(table1.col_time <= table2.col_time)")
	assertClauseSerialize(t, table1ColTime.LT_EQ(timeVar), "(table1.col_time <= $1)", "10:20:00")
}

func TestTimeExpressionGT(t *testing.T) {
	assertClauseSerialize(t, table1ColTime.GT(table2ColTime), "(table1.col_time > table2.col_time)")
	assertClauseSerialize(t, table1ColTime.GT(timeVar), "(table1.col_time > $1)", "10:20:00")
}

func TestTimeExpressionGT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColTime.GT_EQ(table2ColTime), "(table1.col_time >= table2.col_time)")
	assertClauseSerialize(t, table1ColTime.GT_EQ(timeVar), "(table1.col_time >= $1)", "10:20:00")
}

func TestTimeExp(t *testing.T) {
	assertClauseSerialize(t, TimeExp(table1ColFloat), "table1.col_float")
	assertClauseSerialize(t, TimeExp(table1ColFloat).LT(Time(1, 1, 1, 1*time.Millisecond)),
		"(table1.col_float < $1)", string("01:01:01.001"))
}

func TestTimeArithmetic(t *testing.T) {
	time := Time(10, 20, 3)
	assertClauseDebugSerialize(t, table1ColTime.ADD(NewInterval(String("1 HOUR"))).EQ(time),
		"((table1.col_time + INTERVAL '1 HOUR') = '10:20:03')")
	assertClauseDebugSerialize(t, table1ColTime.SUB(NewInterval(String("1 HOUR"))).EQ(time),
		"((table1.col_time - INTERVAL '1 HOUR') = '10:20:03')")
}
