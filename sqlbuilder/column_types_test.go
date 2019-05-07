package sqlbuilder

import (
	"gotest.tools/assert"
	"testing"
)

func TestNewBoolColumn(t *testing.T) {
	boolColumn := NewBoolColumn("col", Nullable)

	out := queryData{}
	err := boolColumn.serialize(&out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "col")

	out.Reset()
	err = boolColumn.serialize(&out)
	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "col")

	out.Reset()
	boolColumn.setTableName("table1")
	err = boolColumn.DefaultAlias().serializeForProjection(&out)
	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), `table1.col AS "table1.col"`)

	out.Reset()
	boolColumn.setTableName("table1")
	aliasedBoolColumn := boolColumn.AS("alias1")
	err = aliasedBoolColumn.serializeForProjection(&out)
	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), `table1.col AS "alias1"`)
}

func TestNewIntColumn(t *testing.T) {
	integerColumn := NewIntegerColumn("col", Nullable)

	out := queryData{}
	err := integerColumn.serialize(&out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "col")

	out.Reset()
	err = integerColumn.serialize(&out)
	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "col")

	out.Reset()
	integerColumn.setTableName("table1")
	err = integerColumn.DefaultAlias().serializeForProjection(&out)
	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), `table1.col AS "table1.col"`)

	out.Reset()
	integerColumn.setTableName("table1")
	aliasedBoolColumn := integerColumn.AS("alias1")
	err = aliasedBoolColumn.serializeForProjection(&out)
	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), `table1.col AS "alias1"`)
}

func TestNewNumericColumnColumn(t *testing.T) {
	numericColumn := NewNumericColumn("col", Nullable)

	out := queryData{}
	err := numericColumn.serialize(&out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "col")

	out.Reset()
	err = numericColumn.serialize(&out)
	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "col")

	out.Reset()
	numericColumn.setTableName("table1")
	err = numericColumn.DefaultAlias().serializeForProjection(&out)
	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), `table1.col AS "table1.col"`)

	out.Reset()
	numericColumn.setTableName("table1")
	aliasedBoolColumn := numericColumn.AS("alias1")
	err = aliasedBoolColumn.serializeForProjection(&out)
	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), `table1.col AS "alias1"`)
}
