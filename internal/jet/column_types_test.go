package jet

import (
	"testing"
)

var subQuery = &selectTableImpl{
	alias: "sub_query",
}

func TestNewArrayColumnString(t *testing.T) {
	stringArrayColumn := ArrayColumn[StringExpression]("colArray").From(subQuery)
	assertClauseSerialize(t, stringArrayColumn, `sub_query."colArray"`)
	assertProjectionSerialize(t, stringArrayColumn, `sub_query."colArray" AS "colArray"`)

	arrayColumn2 := table1ColStringArray.From(subQuery)
	assertClauseSerialize(t, arrayColumn2, `sub_query."table1.col_array_string"`)
	assertProjectionSerialize(t, arrayColumn2, `sub_query."table1.col_array_string" AS "table1.col_array_string"`)
}

func TestNewArrayColumnBool(t *testing.T) {
	boolArrayColumn := ArrayColumn[BoolExpression]("colArrayBool").From(subQuery)
	assertClauseSerialize(t, boolArrayColumn, `sub_query."colArrayBool"`)
	assertProjectionSerialize(t, boolArrayColumn, `sub_query."colArrayBool" AS "colArrayBool"`)

	arrayColumn2 := table1ColBoolArray.From(subQuery)
	assertClauseSerialize(t, arrayColumn2, `sub_query."table1.col_array_bool"`)
	assertProjectionSerialize(t, arrayColumn2, `sub_query."table1.col_array_bool" AS "table1.col_array_bool"`)
}

func TestNewArrayColumnInteger(t *testing.T) {
	intArrayColumn := ArrayColumn[IntegerExpression]("colArrayInt").From(subQuery)
	assertClauseSerialize(t, intArrayColumn, `sub_query."colArrayInt"`)
	assertProjectionSerialize(t, intArrayColumn, `sub_query."colArrayInt" AS "colArrayInt"`)

	arrayColumn2 := table1ColIntArray.From(subQuery)
	assertClauseSerialize(t, arrayColumn2, `sub_query."table1.col_array_int"`)
	assertProjectionSerialize(t, arrayColumn2, `sub_query."table1.col_array_int" AS "table1.col_array_int"`)
}

func TestNewBoolColumn(t *testing.T) {
	boolColumn := BoolColumn("colBool").From(subQuery)
	assertClauseSerialize(t, boolColumn, `sub_query."colBool"`)
	assertClauseSerialize(t, boolColumn.EQ(Bool(true)), `(sub_query."colBool" = $1)`, true)
	assertProjectionSerialize(t, boolColumn, `sub_query."colBool" AS "colBool"`)

	boolColumn2 := table1ColBool.From(subQuery)
	assertClauseSerialize(t, boolColumn2, `sub_query."table1.col_bool"`)
	assertClauseSerialize(t, boolColumn2.EQ(Bool(true)), `(sub_query."table1.col_bool" = $1)`, true)
	assertProjectionSerialize(t, boolColumn2, `sub_query."table1.col_bool" AS "table1.col_bool"`)
}

func TestNewIntColumn(t *testing.T) {
	intColumn := IntegerColumn("col_int").From(subQuery)
	assertClauseSerialize(t, intColumn, `sub_query.col_int`)
	assertClauseSerialize(t, intColumn.EQ(Int(12)), `(sub_query.col_int = $1)`, int64(12))
	assertProjectionSerialize(t, intColumn, `sub_query.col_int AS "col_int"`)

	intColumn2 := table1ColInt.From(subQuery)
	assertClauseSerialize(t, intColumn2, `sub_query."table1.col_int"`)
	assertClauseSerialize(t, intColumn2.EQ(Int(14)), `(sub_query."table1.col_int" = $1)`, int64(14))
	assertProjectionSerialize(t, intColumn2, `sub_query."table1.col_int" AS "table1.col_int"`)

}

func TestNewFloatColumnColumn(t *testing.T) {
	floatColumn := FloatColumn("col_float").From(subQuery)
	assertClauseSerialize(t, floatColumn, `sub_query.col_float`)
	assertClauseSerialize(t, floatColumn.EQ(Float(1.11)), `(sub_query.col_float = $1)`, float64(1.11))
	assertProjectionSerialize(t, floatColumn, `sub_query.col_float AS "col_float"`)

	floatColumn2 := table1ColFloat.From(subQuery)
	assertClauseSerialize(t, floatColumn2, `sub_query."table1.col_float"`)
	assertClauseSerialize(t, floatColumn2.EQ(Float(2.22)), `(sub_query."table1.col_float" = $1)`, float64(2.22))
	assertProjectionSerialize(t, floatColumn2, `sub_query."table1.col_float" AS "table1.col_float"`)
}

func TestNewDateColumnColumn(t *testing.T) {
	dateColumn := DateColumn("col_date").From(subQuery)
	assertClauseSerialize(t, dateColumn, `sub_query.col_date`)
	assertClauseSerialize(t, dateColumn.EQ(Date(2002, 2, 3)),
		`(sub_query.col_date = $1)`, "2002-02-03")
	assertProjectionSerialize(t, dateColumn, `sub_query.col_date AS "col_date"`)

	dateColumn2 := table1ColDate.From(subQuery)
	assertClauseSerialize(t, dateColumn2, `sub_query."table1.col_date"`)
	assertClauseSerialize(t, dateColumn2.EQ(Date(2002, 2, 3)),
		`(sub_query."table1.col_date" = $1)`, "2002-02-03")
	assertProjectionSerialize(t, dateColumn2, `sub_query."table1.col_date" AS "table1.col_date"`)
}

func TestNewTimeColumnColumn(t *testing.T) {
	timeColumn := TimeColumn("col_time").From(subQuery)
	assertClauseSerialize(t, timeColumn, `sub_query.col_time`)
	assertClauseSerialize(t, timeColumn.EQ(Time(1, 1, 1, 1)),
		`(sub_query.col_time = $1)`, "01:01:01.000000001")
	assertProjectionSerialize(t, timeColumn, `sub_query.col_time AS "col_time"`)

	timeColumn2 := table1ColTime.From(subQuery)
	assertClauseSerialize(t, timeColumn2, `sub_query."table1.col_time"`)
	assertClauseSerialize(t, timeColumn2.EQ(Time(2, 2, 2)),
		`(sub_query."table1.col_time" = $1)`, "02:02:02")
	assertProjectionSerialize(t, timeColumn2, `sub_query."table1.col_time" AS "table1.col_time"`)
}

func TestNewTimezColumnColumn(t *testing.T) {
	timezColumn := TimezColumn("col_timez").From(subQuery)
	assertClauseSerialize(t, timezColumn, `sub_query.col_timez`)
	assertClauseSerialize(t, timezColumn.EQ(Timez(1, 1, 1, 1, "UTC")),
		`(sub_query.col_timez = $1)`, "01:01:01.000000001 UTC")
	assertProjectionSerialize(t, timezColumn, `sub_query.col_timez AS "col_timez"`)

	timezColumn2 := table1ColTimez.From(subQuery)
	assertClauseSerialize(t, timezColumn2, `sub_query."table1.col_timez"`)
	assertClauseSerialize(t, timezColumn2.EQ(Timez(2, 2, 2, 0, "UTC")),
		`(sub_query."table1.col_timez" = $1)`, "02:02:02 UTC")
	assertProjectionSerialize(t, timezColumn2, `sub_query."table1.col_timez" AS "table1.col_timez"`)
}

func TestNewTimestampColumnColumn(t *testing.T) {
	timestampColumn := TimestampColumn("col_timestamp").From(subQuery)
	assertClauseSerialize(t, timestampColumn, `sub_query.col_timestamp`)
	assertClauseSerialize(t, timestampColumn.EQ(Timestamp(1, 1, 1, 1, 1, 1)),
		`(sub_query.col_timestamp = $1)`, "0001-01-01 01:01:01")
	assertProjectionSerialize(t, timestampColumn, `sub_query.col_timestamp AS "col_timestamp"`)

	timestampColumn2 := table1ColTimestamp.From(subQuery)
	assertClauseSerialize(t, timestampColumn2, `sub_query."table1.col_timestamp"`)
	assertClauseSerialize(t, timestampColumn2.EQ(Timestamp(2, 2, 2, 2, 2, 2)),
		`(sub_query."table1.col_timestamp" = $1)`, "0002-02-02 02:02:02")
	assertProjectionSerialize(t, timestampColumn2, `sub_query."table1.col_timestamp" AS "table1.col_timestamp"`)
}

func TestNewTimestampzColumnColumn(t *testing.T) {
	timestampzColumn := TimestampzColumn("col_timestampz").From(subQuery)
	assertClauseSerialize(t, timestampzColumn, `sub_query.col_timestampz`)
	assertClauseSerialize(t, timestampzColumn.EQ(Timestampz(1, 1, 1, 1, 1, 1, 0, "UTC")),
		`(sub_query.col_timestampz = $1)`, "0001-01-01 01:01:01 UTC")
	assertProjectionSerialize(t, timestampzColumn, `sub_query.col_timestampz AS "col_timestampz"`)

	timestampzColumn2 := table1ColTimestampz.From(subQuery)
	assertClauseSerialize(t, timestampzColumn2, `sub_query."table1.col_timestampz"`)
	assertClauseSerialize(t, timestampzColumn2.EQ(Timestampz(2, 2, 2, 2, 2, 2, 0, "UTC")),
		`(sub_query."table1.col_timestampz" = $1)`, "0002-02-02 02:02:02 UTC")
	assertProjectionSerialize(t, timestampzColumn2, `sub_query."table1.col_timestampz" AS "table1.col_timestampz"`)
}

func TestIntegerColumn_SET(t *testing.T) {
	assignment := table1ColInt.SET(Int(42))
	assertClauseSerialize(t, assignment, "col_int = $1", int64(42))
}

func TestStringColumn_SET(t *testing.T) {
	strCol := StringColumn("name")
	assignment := strCol.SET(String("test"))
	assertClauseSerialize(t, assignment, "name = $1", "test")
}

func TestBoolColumn_SET(t *testing.T) {
	assignment := table1ColBool.SET(Bool(true))
	assertClauseSerialize(t, assignment, "col_bool = $1", true)
}

func TestFloatColumn_SET(t *testing.T) {
	assignment := table1ColFloat.SET(Float(3.14))
	assertClauseSerialize(t, assignment, "col_float = $1", float64(3.14))
}

func TestIntervalColumn_SET(t *testing.T) {
	intervalCol1 := IntervalColumn("duration")
	intervalCol2 := IntervalColumn("other_duration")
	assignment := intervalCol1.SET(intervalCol2)
	assertClauseSerialize(t, assignment, "duration = other_duration")
}

func TestRangeColumn_SET(t *testing.T) {
	assignment := table1ColRange.SET(Int8Range(Int8(1), Int8(10)))
	assertClauseSerialize(t, assignment, "col_range = int8range($1, $2)", int8(1), int8(10))
}
