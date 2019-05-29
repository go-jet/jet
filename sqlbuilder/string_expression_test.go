package sqlbuilder

import (
	"gotest.tools/assert"
	"testing"
)

func TestStringEQColumn(t *testing.T) {
	exp := table3StrCol.EQ(table2StrCol)

	out := queryData{}
	err := exp.serialize(select_statement, &out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "table3.col2 = table2.col4")
}

func TestStringEQString(t *testing.T) {
	exp := table3StrCol.EQ(String("JOHN"))

	out := queryData{}
	err := exp.serialize(select_statement, &out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "table3.col2 = $1")
}

func TestStringNOT_EQ(t *testing.T) {
	exp := table3StrCol.NOT_EQ(table2StrCol)

	out := queryData{}
	err := exp.serialize(select_statement, &out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "table3.col2 != table2.col4")
}

func TestStringGT(t *testing.T) {
	exp := table3StrCol.GT(table2StrCol)

	out := queryData{}
	err := exp.serialize(select_statement, &out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "table3.col2 > table2.col4")
}

func TestStringGT_EQ(t *testing.T) {
	exp := table3StrCol.GT_EQ(table2StrCol)

	out := queryData{}
	err := exp.serialize(select_statement, &out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "table3.col2 >= table2.col4")
}

func TestStringLT(t *testing.T) {
	exp := table3StrCol.LT(table2StrCol)

	out := queryData{}
	err := exp.serialize(select_statement, &out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "table3.col2 < table2.col4")
}

func TestStringLT_EQ(t *testing.T) {
	exp := table3StrCol.LT_EQ(table2StrCol)

	out := queryData{}
	err := exp.serialize(select_statement, &out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "table3.col2 <= table2.col4")
}
