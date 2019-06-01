package sqlbuilder

import (
	"gotest.tools/assert"
	"testing"
)

func TestStringEQColumn(t *testing.T) {
	exp := table3StrCol.EQ(table2ColStr)

	out := queryData{}
	err := exp.serialize(select_statement, &out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "(table3.col2 = table2.colStr)")
}

func TestStringEQString(t *testing.T) {
	exp := table3StrCol.EQ(String("JOHN"))

	out := queryData{}
	err := exp.serialize(select_statement, &out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "(table3.col2 = $1)")
}

func TestStringNOT_EQ(t *testing.T) {
	exp := table3StrCol.NOT_EQ(table2ColStr)

	out := queryData{}
	err := exp.serialize(select_statement, &out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "(table3.col2 != table2.colStr)")
}

func TestStringGT(t *testing.T) {
	exp := table3StrCol.GT(table2ColStr)

	out := queryData{}
	err := exp.serialize(select_statement, &out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "(table3.col2 > table2.colStr)")
}

func TestStringGT_EQ(t *testing.T) {
	exp := table3StrCol.GT_EQ(table2ColStr)

	out := queryData{}
	err := exp.serialize(select_statement, &out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "(table3.col2 >= table2.colStr)")
}

func TestStringLT(t *testing.T) {
	exp := table3StrCol.LT(table2ColStr)

	out := queryData{}
	err := exp.serialize(select_statement, &out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "(table3.col2 < table2.colStr)")
}

func TestStringLT_EQ(t *testing.T) {
	exp := table3StrCol.LT_EQ(table2ColStr)

	out := queryData{}
	err := exp.serialize(select_statement, &out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "(table3.col2 <= table2.colStr)")
}
