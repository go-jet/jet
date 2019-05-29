package sqlbuilder

import (
	"gotest.tools/assert"
	"testing"
)

func TestNumericEQColumn(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1Col1.EQ(table2Col3)), "table1.col1 = table2.col3")
}

func TestNumericEQInt(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1Col1.EQ(Int(11))), "table1.col1 = $1")
}

func TestNumericEQFloat(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1Col1.EQ(Float(22.333))), "table1.col1 = $1")
}

func TestNumericNOT_EQ(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1Col1.NOT_EQ(table2Col3)), "table1.col1 != table2.col3")
}

func TestNumericGT(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1Col1.GT(table2Col3)), "table1.col1 > table2.col3")
}

func TestNumericGT_EQ(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1Col1.GT_EQ(table2Col3)), "table1.col1 >= table2.col3")
}

func TestNumericLT(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1Col1.LT(table2Col3)), "table1.col1 < table2.col3")
}

func TestNumericLT_EQ(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1Col1.LT_EQ(table2Col3)), "table1.col1 <= table2.col3")
}
