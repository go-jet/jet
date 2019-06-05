package sqlbuilder

import (
	"fmt"
	"gotest.tools/assert"
	"testing"
)

var table1Col1 = NewIntegerColumn("col1", true)
var table1ColInt = NewIntegerColumn("colInt", true)
var table1ColFloat = NewFloatColumn("colFloat", true)
var table1Col3 = NewIntegerColumn("col3", true)
var table1ColTime = NewTimeColumn("colTime", true)
var table1ColBool = NewBoolColumn("colBool", true)

var table1 = NewTable(
	"db",
	"table1",
	table1Col1,
	table1ColInt,
	table1ColFloat,
	table1Col3,
	table1ColTime,
	table1ColBool)

var table2Col3 = NewIntegerColumn("col3", true)
var table2Col4 = NewIntegerColumn("col4", true)
var table2ColInt = NewIntegerColumn("colInt", true)
var table2ColFloat = NewFloatColumn("colFloat", true)
var table2ColStr = NewStringColumn("colStr", true)
var table2ColBool = NewBoolColumn("colBool", true)
var table2ColTime = NewTimeColumn("colTime", true)

var table2 = NewTable(
	"db",
	"table2",
	table2Col3,
	table2Col4,
	table2ColInt,
	table2ColFloat,
	table2ColStr,
	table2ColBool,
	table2ColTime)

var table3Col1 = NewIntegerColumn("col1", true)
var table3ColInt = NewIntegerColumn("colInt", true)
var table3StrCol = NewStringColumn("col2", true)
var table3 = NewTable(
	"db",
	"table3",
	table3Col1,
	table3ColInt,
	table3StrCol)

func assertClauseSerialize(t *testing.T, clause clause, query string, args ...interface{}) {
	out := queryData{}
	err := clause.serialize(select_statement, &out)

	assert.NilError(t, err)

	assert.DeepEqual(t, out.buff.String(), query)
	assert.DeepEqual(t, out.args, args)
}

func assertClauseSerializeErr(t *testing.T, clause clause, errString string) {
	out := queryData{}
	err := clause.serialize(select_statement, &out)

	fmt.Println(err)
	assert.Assert(t, err != nil)
	assert.Equal(t, err.Error(), errString)
}

func assertProjectionSerialize(t *testing.T, projection projection, query string, args ...interface{}) {
	out := queryData{}
	err := projection.serializeForProjection(select_statement, &out)

	assert.NilError(t, err)

	assert.DeepEqual(t, out.buff.String(), query)
	assert.DeepEqual(t, out.args, args)
}
