package jet

import (
	"gotest.tools/assert"
	"testing"
)

var table1Col1 = IntegerColumn("col1")
var table1ColInt = IntegerColumn("col_int")
var table1ColFloat = FloatColumn("col_float")
var table1Col3 = IntegerColumn("col3")
var table1ColTime = TimeColumn("col_time")
var table1ColBool = BoolColumn("col_bool")

var table1 = NewTable(
	"db",
	"table1",
	table1Col1,
	table1ColInt,
	table1ColFloat,
	table1Col3,
	table1ColTime,
	table1ColBool)

var table2Col3 = IntegerColumn("col3")
var table2Col4 = IntegerColumn("col4")
var table2ColInt = IntegerColumn("col_int")
var table2ColFloat = FloatColumn("col_float")
var table2ColStr = StringColumn("col_str")
var table2ColBool = BoolColumn("col_bool")
var table2ColTime = TimeColumn("col_time")

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

var table3Col1 = IntegerColumn("col1")
var table3ColInt = IntegerColumn("col_int")
var table3StrCol = StringColumn("col2")
var table3 = NewTable(
	"db",
	"table3",
	table3Col1,
	table3ColInt,
	table3StrCol)

func assertClauseSerialize(t *testing.T, clause clause, query string, args ...interface{}) {
	out := sqlBuilder{}
	err := clause.serialize(select_statement, &out)

	assert.NilError(t, err)

	assert.DeepEqual(t, out.buff.String(), query)
	assert.DeepEqual(t, out.args, args)
}

func assertClauseSerializeErr(t *testing.T, clause clause, errString string) {
	out := sqlBuilder{}
	err := clause.serialize(select_statement, &out)

	//fmt.Println(out.buff.String())
	assert.Assert(t, err != nil)
	assert.Error(t, err, errString)
}

func assertProjectionSerialize(t *testing.T, projection projection, query string, args ...interface{}) {
	out := sqlBuilder{}
	err := projection.serializeForProjection(select_statement, &out)

	assert.NilError(t, err)

	assert.DeepEqual(t, out.buff.String(), query)
	assert.DeepEqual(t, out.args, args)
}

func assertStatement(t *testing.T, query Statement, expectedQuery string, expectedArgs ...interface{}) {
	queryStr, args, err := query.Sql()
	assert.NilError(t, err)

	//fmt.Println(queryStr)
	assert.Equal(t, queryStr, expectedQuery)
	assert.DeepEqual(t, args, expectedArgs)
}

func assertStatementErr(t *testing.T, stmt Statement, errorStr string) {
	_, _, err := stmt.Sql()

	assert.Assert(t, err != nil)
	assert.Error(t, err, errorStr)
}
