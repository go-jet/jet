package sqlbuilder

import (
	"gotest.tools/assert"
	"testing"
)

func TestFloatExpressionEQColumn(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColFloat.EQ(table2ColFloat)), "(table1.colFloat = table2.colFloat)")
}

func TestFloatExpressionEQFloat(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColFloat.EQ(Float(11))), "(table1.colFloat = $1)")
}

func TestFloatExpressionNOT_EQ(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColFloat.NOT_EQ(table2ColFloat)), "(table1.colFloat != table2.colFloat)")
}

func TestFloatExpressionGT(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColFloat.GT(table2ColFloat)), "(table1.colFloat > table2.colFloat)")
}

func TestFloatExpressionGT_EQ(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColFloat.GT_EQ(table2ColFloat)), "(table1.colFloat >= table2.colFloat)")
}

func TestFloatExpressionLT(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColFloat.LT(table2ColFloat)), "(table1.colFloat < table2.colFloat)")
}

func TestFloatExpressionLT_EQ(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColFloat.LT_EQ(table2ColFloat)), "(table1.colFloat <= table2.colFloat)")
}

func TestFloatExpressionADD(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColFloat.ADD(table2ColFloat)), "(table1.colFloat + table2.colFloat)")
}

func TestFloatExpressionSUB(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColFloat.SUB(table2ColFloat)), "(table1.colFloat - table2.colFloat)")
}

func TestFloatExpressionMUL(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColFloat.MUL(table2ColFloat)), "(table1.colFloat * table2.colFloat)")
}

func TestFloatExpressionDIV(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColFloat.DIV(table2ColFloat)), "(table1.colFloat / table2.colFloat)")
}

func TestFloatExpressionMOD(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColFloat.MOD(table2ColFloat)), "(table1.colFloat % table2.colFloat)")
}

func TestFloatExpressionEXP(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColFloat.POW(table2ColFloat)), "(table1.colFloat ^ table2.colFloat)")
}
