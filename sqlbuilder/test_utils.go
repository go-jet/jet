package sqlbuilder

import (
	"gotest.tools/assert"
	"testing"
)

var table1Col1 = NewIntegerColumn("col1", Nullable)
var table1ColInt = NewIntegerColumn("colInt", Nullable)
var table1ColFloat = NewFloatColumn("colFloat", Nullable)
var table1Col3 = NewIntegerColumn("col3", Nullable)
var table1ColTime = NewTimeColumn("colTime", Nullable)
var table1ColBool = NewBoolColumn("colBool", Nullable)

var table1 = NewTable(
	"db",
	"table1",
	table1Col1,
	table1ColInt,
	table1ColFloat,
	table1Col3,
	table1ColTime,
	table1ColBool)

var table2Col3 = NewIntegerColumn("col3", Nullable)
var table2Col4 = NewIntegerColumn("col4", Nullable)
var table2ColInt = NewIntegerColumn("colInt", Nullable)
var table2ColFloat = NewFloatColumn("colFloat", Nullable)
var table2ColStr = NewStringColumn("colStr", Nullable)
var table2ColBool = NewBoolColumn("colBool", Nullable)
var table2ColTime = NewTimeColumn("colTime", Nullable)

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

var table3Col1 = NewIntegerColumn("col1", Nullable)
var table3StrCol = NewStringColumn("col2", Nullable)
var table3 = NewTable(
	"db",
	"table3",
	table3Col1,
	table3StrCol)

func getTestSerialize(t *testing.T, exp expression) string {
	out := queryData{}
	err := exp.serialize(select_statement, &out)

	assert.NilError(t, err)

	return out.buff.String()
}
