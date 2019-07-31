package postgres

import (
	"github.com/go-jet/jet"
)

var table1Col1 = IntegerColumn("col1")
var table1ColInt = IntegerColumn("col_int")
var table1ColFloat = FloatColumn("col_float")
var table1Col3 = IntegerColumn("col3")
var table1ColTime = TimeColumn("col_time")
var table1ColTimez = TimezColumn("col_timez")
var table1ColTimestamp = TimestampColumn("col_timestamp")
var table1ColTimestampz = TimestampzColumn("col_timestampz")
var table1ColBool = BoolColumn("col_bool")
var table1ColDate = DateColumn("col_date")

var table1 = jet.NewTable(
	jet.PostgreSQL,
	"db",
	"table1",
	table1Col1,
	table1ColInt,
	table1ColFloat,
	table1Col3,
	table1ColTime,
	table1ColTimez,
	table1ColBool,
	table1ColDate,
	table1ColTimestamp,
	table1ColTimestampz,
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

var table2 = jet.NewTable(
	jet.PostgreSQL,
	"db",
	"table2",
	table2Col3,
	table2Col4,
	table2ColInt,
	table2ColFloat,
	table2ColStr,
	table2ColBool,
	table2ColTime,
	table2ColTimez,
	table2ColDate,
	table2ColTimestamp,
	table2ColTimestampz,
)

var table3Col1 = IntegerColumn("col1")
var table3ColInt = IntegerColumn("col_int")
var table3StrCol = StringColumn("col2")
var table3 = jet.NewTable(
	jet.PostgreSQL,
	"db",
	"table3",
	table3Col1,
	table3ColInt,
	table3StrCol)
