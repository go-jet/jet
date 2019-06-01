package sqlbuilder

import (
	"gotest.tools/assert"
	"testing"
)

func TestIntegerExpressionEQColumn(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColInt.EQ(table2ColInt)), "(table1.colInt = table2.colInt)")
}

func TestIntegerExpressionEQInt(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColInt.EQ(Int(11))), "(table1.colInt = $1)")
}

func TestIntegerExpressionNOT_EQ(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColInt.NOT_EQ(table2ColInt)), "(table1.colInt != table2.colInt)")
}

func TestIntegerExpressionGT(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColInt.GT(table2ColInt)), "(table1.colInt > table2.colInt)")
}

func TestIntegerExpressionGT_EQ(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColInt.GT_EQ(table2ColInt)), "(table1.colInt >= table2.colInt)")
}

func TestIntegerExpressionLT(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColInt.LT(table2ColInt)), "(table1.colInt < table2.colInt)")
}

func TestIntegerExpressionLT_EQ(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColInt.LT_EQ(table2ColInt)), "(table1.colInt <= table2.colInt)")
}

func TestIntegerExpressionADD(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColInt.ADD(table2ColInt)), "(table1.colInt + table2.colInt)")
}

func TestIntegerExpressionSUB(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColInt.SUB(table2ColInt)), "(table1.colInt - table2.colInt)")
}

func TestIntegerExpressionMUL(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColInt.MUL(table2ColInt)), "(table1.colInt * table2.colInt)")
}

func TestIntegerExpressionDIV(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColInt.DIV(table2ColInt)), "(table1.colInt / table2.colInt)")
}

func TestIntExpressionMOD(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColInt.MOD(table2ColInt)), "(table1.colInt % table2.colInt)")
}

func TestIntExpressionEXP(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColInt.POW(table2ColInt)), "(table1.colInt ^ table2.colInt)")
}

func TestIntExpressionBIT_SHIFT_LEFT(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColInt.BIT_SHIFT_LEFT(table2ColInt)), "(table1.colInt << table2.colInt)")
	assert.Equal(t, getTestSerialize(t, table1ColInt.BIT_SHIFT_LEFT(Int(2))), "(table1.colInt << $1)")
}

func TestIntExpressionBIT_SHIFT_RIGHT(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColInt.BIT_SHIFT_RIGHT(table2ColInt)), "(table1.colInt >> table2.colInt)")
	assert.Equal(t, getTestSerialize(t, table1ColInt.BIT_SHIFT_RIGHT(Int(11))), "(table1.colInt >> $1)")
}
