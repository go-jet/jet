package sqlbuilder

import (
	"gotest.tools/assert"
	"testing"
	"time"
)

func TestTimeExpressionEQ(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColTime.EQ(table2ColTime)), "table1.colTime = table2.colTime")
	assert.Equal(t, getTestSerialize(t, table1ColTime.EQ(Time(time.Now()))), "table1.colTime = $1")
}

func TestTimeExpressionNOT_EQ(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColTime.NOT_EQ(table2ColTime)), "table1.colTime != table2.colTime")
	assert.Equal(t, getTestSerialize(t, table1ColTime.NOT_EQ(Time(time.Now()))), "table1.colTime != $1")
}

func TestTimeExpressionLT(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColTime.LT(table2ColTime)), "table1.colTime < table2.colTime")
	assert.Equal(t, getTestSerialize(t, table1ColTime.LT(Time(time.Now()))), "table1.colTime < $1")
}

func TestTimeExpressionLT_EQ(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColTime.LT_EQ(table2ColTime)), "table1.colTime <= table2.colTime")
	assert.Equal(t, getTestSerialize(t, table1ColTime.LT_EQ(Time(time.Now()))), "table1.colTime <= $1")
}

func TestTimeExpressionGT(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColTime.GT(table2ColTime)), "table1.colTime > table2.colTime")
	assert.Equal(t, getTestSerialize(t, table1ColTime.GT(Time(time.Now()))), "table1.colTime > $1")
}

func TestTimeExpressionGT_EQ(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColTime.GT_EQ(table2ColTime)), "table1.colTime >= table2.colTime")
	assert.Equal(t, getTestSerialize(t, table1ColTime.GT_EQ(Time(time.Now()))), "table1.colTime >= $1")
}
