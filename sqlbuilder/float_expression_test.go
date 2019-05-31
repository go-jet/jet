package sqlbuilder

import (
	"gotest.tools/assert"
	"testing"
)

func TestFloatExpressionEQColumn(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1Col1.EQ(table2Col3)), "(table1.col1 = table2.col3)")
}

func TestFloatExpressionEQInt(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1Col1.EQ(Int(11))), "(table1.col1 = $1)")
}

func TestFloatExpressionEQFloat(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1Col1.EQ(Int(22))), "(table1.col1 = $1)")
}

func TestFloatExpressionNOT_EQ(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1Col1.NOT_EQ(table2Col3)), "(table1.col1 != table2.col3)")
}

func TestFloatExpressionGT(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1Col1.GT(table2Col3)), "(table1.col1 > table2.col3)")
}

func TestFloatExpressionGT_EQ(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1Col1.GT_EQ(table2Col3)), "(table1.col1 >= table2.col3)")
}

func TestFloatExpressionLT(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1Col1.LT(table2Col3)), "(table1.col1 < table2.col3)")
}

func TestFloatExpressionLT_EQ(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1Col1.LT_EQ(table2Col3)), "(table1.col1 <= table2.col3)")
}
