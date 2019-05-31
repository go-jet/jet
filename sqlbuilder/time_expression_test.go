package sqlbuilder

import (
	"gotest.tools/assert"
	"testing"
)

func TestTimeExpressionEQ(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColTime.EQ(table2ColTime)), "(table1.colTime = table2.colTime)")
	assert.Equal(t, getTestSerialize(t, table1ColTime.EQ(Time(10, 20, 0, 0))), "(table1.colTime = $1)")
}

func TestTimeExpressionNOT_EQ(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColTime.NOT_EQ(table2ColTime)), "(table1.colTime != table2.colTime)")
	assert.Equal(t, getTestSerialize(t, table1ColTime.NOT_EQ(Time(10, 20, 0, 0))), "(table1.colTime != $1)")
}

func TestTimeExpressionLT(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColTime.LT(table2ColTime)), "(table1.colTime < table2.colTime)")
	assert.Equal(t, getTestSerialize(t, table1ColTime.LT(Time(10, 20, 0, 0))), "(table1.colTime < $1)")
}

func TestTimeExpressionLT_EQ(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColTime.LT_EQ(table2ColTime)), "(table1.colTime <= table2.colTime)")
	assert.Equal(t, getTestSerialize(t, table1ColTime.LT_EQ(Time(10, 20, 0, 0))), "(table1.colTime <= $1)")
}

func TestTimeExpressionGT(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColTime.GT(table2ColTime)), "(table1.colTime > table2.colTime)")
	assert.Equal(t, getTestSerialize(t, table1ColTime.GT(Time(10, 20, 0, 0))), "(table1.colTime > $1)")
}

func TestTimeExpressionGT_EQ(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColTime.GT_EQ(table2ColTime)), "(table1.colTime >= table2.colTime)")
	assert.Equal(t, getTestSerialize(t, table1ColTime.GT_EQ(Time(10, 20, 0, 0))), "(table1.colTime >= $1)")
}
