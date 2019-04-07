package sqlbuilder

var table1Col1 = NewIntegerColumn("col1", Nullable)
var table1Col2 = NewIntegerColumn("col2", Nullable)
var table1Col3 = NewIntegerColumn("col3", Nullable)
var table1Col4 = NewTimeColumn("col4", Nullable)
var table1 = NewTable(
	"db",
	"table1",
	table1Col1,
	table1Col2,
	table1Col3,
	table1Col4)

var table2Col3 = NewIntegerColumn("col3", Nullable)
var table2Col4 = NewIntegerColumn("col4", Nullable)
var table2 = NewTable(
	"db",
	"table2",
	table2Col3,
	table2Col4)

var table3Col1 = NewIntegerColumn("col1", Nullable)
var table3Col2 = NewIntegerColumn("col2", Nullable)
var table3 = NewTable(
	"db",
	"table3",
	table3Col1,
	table3Col2)
