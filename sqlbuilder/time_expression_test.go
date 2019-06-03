package sqlbuilder

import (
	"testing"
)

func TestTimeExpressionEQ(t *testing.T) {
	assertExpressionSerialize(t, table1ColTime.EQ(table2ColTime), "(table1.colTime = table2.colTime)")
	assertExpressionSerialize(t, table1ColTime.EQ(Time(10, 20, 0, 0)), "(table1.colTime = $1::time without time zone)", "10:20:00.000")
}

func TestTimeExpressionNOT_EQ(t *testing.T) {
	assertExpressionSerialize(t, table1ColTime.NOT_EQ(table2ColTime), "(table1.colTime != table2.colTime)")
	assertExpressionSerialize(t, table1ColTime.NOT_EQ(Time(10, 20, 0, 0)), "(table1.colTime != $1::time without time zone)", "10:20:00.000")
}

func TestTimeExpressionLT(t *testing.T) {
	assertExpressionSerialize(t, table1ColTime.LT(table2ColTime), "(table1.colTime < table2.colTime)")
	assertExpressionSerialize(t, table1ColTime.LT(Time(10, 20, 0, 0)), "(table1.colTime < $1::time without time zone)", "10:20:00.000")
}

func TestTimeExpressionLT_EQ(t *testing.T) {
	assertExpressionSerialize(t, table1ColTime.LT_EQ(table2ColTime), "(table1.colTime <= table2.colTime)")
	assertExpressionSerialize(t, table1ColTime.LT_EQ(Time(10, 20, 0, 0)), "(table1.colTime <= $1::time without time zone)", "10:20:00.000")
}

func TestTimeExpressionGT(t *testing.T) {
	assertExpressionSerialize(t, table1ColTime.GT(table2ColTime), "(table1.colTime > table2.colTime)")
	assertExpressionSerialize(t, table1ColTime.GT(Time(10, 20, 0, 0)), "(table1.colTime > $1::time without time zone)", "10:20:00.000")
}

func TestTimeExpressionGT_EQ(t *testing.T) {
	assertExpressionSerialize(t, table1ColTime.GT_EQ(table2ColTime), "(table1.colTime >= table2.colTime)")
	assertExpressionSerialize(t, table1ColTime.GT_EQ(Time(10, 20, 0, 0)), "(table1.colTime >= $1::time without time zone)", "10:20:00.000")
}
