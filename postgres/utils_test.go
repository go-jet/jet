package postgres

import (
	"testing"

	"github.com/go-jet/jet/v2/internal/jet"
	"github.com/go-jet/jet/v2/internal/testutils"
)

var table1Col1 = IntegerColumn("col1")
var table1ColInt = IntegerColumn("col_int")
var table1ColFloat = FloatColumn("col_float")
var table1ColTime = TimeColumn("col_time")
var table1ColTimez = TimezColumn("col_timez")
var table1ColTimestamp = TimestampColumn("col_timestamp")
var table1ColTimestampz = TimestampzColumn("col_timestampz")
var table1ColBool = BoolColumn("col_bool")
var table1ColDate = DateColumn("col_date")
var table1ColInterval = IntervalColumn("col_interval")
var table1ColRange = Int8RangeColumn("col_range")

var table1 = NewTable(
	"db",
	"table1",
	"",
	table1Col1,
	table1ColInt,
	table1ColFloat,
	table1ColTime,
	table1ColTimez,
	table1ColBool,
	table1ColDate,
	table1ColTimestamp,
	table1ColTimestampz,
	table1ColInterval,
	table1ColRange,
)

var table2Col3 = IntegerColumn("col3")
var table2Col4 = IntegerColumn("col4")
var table2ColInt = IntegerColumn("col_int")
var table2ColFloat = FloatColumn("col_float")
var table2ColStr = StringColumn("col_str")
var table2ColBool = BoolColumn("col_bool")
var table2ColTime = TimeColumn("col_time")
var table2ColTimez = TimezColumn("col_timez")
var table2ColTimestamp = TimestampColumn("col_timestamp")
var table2ColTimestampz = TimestampzColumn("col_timestampz")
var table2ColDate = DateColumn("col_date")
var table2ColInterval = IntervalColumn("col_interval")
var table2ColRange = Int8RangeColumn("col_range")

var table2 = NewTable("db", "table2", "", table2Col3, table2Col4, table2ColInt, table2ColFloat, table2ColStr, table2ColBool, table2ColTime, table2ColTimez, table2ColDate, table2ColTimestamp, table2ColTimestampz, table2ColInterval, table2ColRange)

var table3Col1 = IntegerColumn("col1")
var table3ColInt = IntegerColumn("col_int")
var table3StrCol = StringColumn("col2")
var table3 = NewTable("db", "table3", "", table3Col1, table3ColInt, table3StrCol)

func assertSerialize(t *testing.T, serializer jet.Serializer, query string, args ...interface{}) {
	testutils.AssertSerialize(t, Dialect, serializer, query, args...)
}

func assertDebugSerialize(t *testing.T, serializer jet.Serializer, query string, args ...interface{}) {
	testutils.AssertDebugSerialize(t, Dialect, serializer, query, args...)
}

func assertClauseSerialize(t *testing.T, clause jet.Clause, query string, args ...interface{}) {
	testutils.AssertClauseSerialize(t, Dialect, clause, query, args...)
}

func assertSerializeErr(t *testing.T, serializer jet.Serializer, errString string) {
	testutils.AssertSerializeErr(t, Dialect, serializer, errString)
}

func assertProjectionSerialize(t *testing.T, projection jet.Projection, query string, args ...interface{}) {
	testutils.AssertProjectionSerialize(t, Dialect, projection, query, args...)
}

var assertStatementSql = testutils.AssertStatementSql
var assertDebugStatementSql = testutils.AssertDebugStatementSql
var assertStatementSqlErr = testutils.AssertStatementSqlErr
var assertPanicErr = testutils.AssertPanicErr
