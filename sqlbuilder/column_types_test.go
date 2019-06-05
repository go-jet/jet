package sqlbuilder

import (
	"gotest.tools/assert"
	"testing"
)

func TestNewBoolColumn(t *testing.T) {
	boolColumn := NewBoolColumn("col", false)

	out := queryData{}
	err := boolColumn.serialize(select_statement, &out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "col")

	out.reset()
	err = boolColumn.serialize(select_statement, &out)
	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "col")

	out.reset()
	boolColumn.setTableName("table1")
	err = boolColumn.DefaultAlias().serializeForProjection(select_statement, &out)
	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), `table1.col AS "table1.col"`)

	out.reset()
	boolColumn.setTableName("table1")
	aliasedBoolColumn := boolColumn.AS("alias1")
	err = aliasedBoolColumn.serializeForProjection(select_statement, &out)
	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), `table1.col AS "alias1"`)
}

func TestNewIntColumn(t *testing.T) {
	integerColumn := NewIntegerColumn("col", false)

	out := queryData{}
	err := integerColumn.serialize(select_statement, &out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "col")

	out.reset()
	err = integerColumn.serialize(select_statement, &out)
	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "col")

	out.reset()
	integerColumn.setTableName("table1")
	err = integerColumn.DefaultAlias().serializeForProjection(select_statement, &out)
	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), `table1.col AS "table1.col"`)

	out.reset()
	integerColumn.setTableName("table1")
	aliasedBoolColumn := integerColumn.AS("alias1")
	err = aliasedBoolColumn.serializeForProjection(select_statement, &out)
	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), `table1.col AS "alias1"`)
}

func TestNewNumericColumnColumn(t *testing.T) {
	numericColumn := NewFloatColumn("col", false)

	out := queryData{}
	err := numericColumn.serialize(select_statement, &out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "col")

	out.reset()
	err = numericColumn.serialize(select_statement, &out)
	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "col")

	out.reset()
	numericColumn.setTableName("table1")
	err = numericColumn.DefaultAlias().serializeForProjection(select_statement, &out)
	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), `table1.col AS "table1.col"`)

	out.reset()
	numericColumn.setTableName("table1")
	aliasedBoolColumn := numericColumn.AS("alias1")
	err = aliasedBoolColumn.serializeForProjection(select_statement, &out)
	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), `table1.col AS "alias1"`)
}
