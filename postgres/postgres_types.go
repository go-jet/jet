package postgres

import "github.com/go-jet/jet"

type ColumnBool jet.ColumnBool
type BoolExpression jet.BoolExpression

var BoolColumn = jet.BoolColumn
var Bool = jet.Bool

type ColumnString jet.ColumnString
type StringExpression jet.StringExpression

var StringColumn = jet.StringColumn
var String = jet.String

type ColumnInteger jet.ColumnInteger
type IntegerExpression jet.IntegerExpression

var IntegerColumn = jet.IntegerColumn
var Int = jet.Int

type ColumnFloat jet.ColumnFloat
type FloatExpression jet.FloatExpression

var FloatColumn = jet.FloatColumn
var Float = jet.Float
var FloatExp = jet.FloatExp

type ColumnDate jet.ColumnDate
type DateExpression jet.DateExpression

var DateColumn = jet.DateColumn
var Date = func(year, month, day int) DateExpression {
	return CAST(jet.Date(year, month, day)).AS_DATE()
}

type ColumnDateTime jet.ColumnTimestamp
type DateTimeExpression jet.TimestampExpression

var DateTimeColumn = jet.TimestampColumn
var DateTime = func(year, month, day int) DateExpression {
	return CAST(jet.Date(year, month, day)).AS_DATE()
}

type TimeExpression jet.TimeExpression
type ColumnTime jet.ColumnTime

var TimeColumn = jet.TimeColumn
var Time = func(hour, minute, second, milliseconds int) TimeExpression {
	return CAST(jet.Time(hour, minute, second, milliseconds)).AS_TIME()
}
var TimeExp = jet.TimeExp

type TimezExpression jet.TimezExpression
type ColumnTimez jet.ColumnTimez

var TimezColumn = jet.TimezColumn

type ColumnTimestamp jet.ColumnTimestamp
type TimestampExpression jet.TimestampExpression

var TimestampColumn = jet.TimestampColumn
var Timestamp = func(year, month, day, hour, minute, second, milliseconds int) TimestampExpression {
	return CAST(jet.Timestamp(year, month, day, hour, minute, second, milliseconds)).AS_TIMESTAMP()
}
var TimestampExp = jet.TimestampExp

type TimestampzExpression jet.TimestampzExpression
type ColumnTimestampz jet.ColumnTimestampz

var TimestampzColumn = jet.TimestampzColumn

// ---------------- functions ------------------//

var MAXf = jet.MAXf
var SUMf = jet.SUMf
var AVG = jet.AVG
var MINf = jet.MINf
var COUNT = jet.COUNT

var CASE = jet.CASE

// ---------------- statements -----------------//

type SelectStatement jet.SelectStatement

var SELECT = jet.SELECT

var UNION = jet.UNION
var UNION_ALL = jet.UNION_ALL
var INTERSECT = jet.INTERSECT
var INTERSECT_ALL = jet.INTERSECT_ALL

type SelectLock jet.SelectLock

var (
	UPDATE        = jet.NewSelectLock("UPDATE")
	NO_KEY_UPDATE = jet.NewSelectLock("NO KEY UPDATE")
	SHARE         = jet.NewSelectLock("SHARE")
	KEY_SHARE     = jet.NewSelectLock("KEY SHARE")
)

var STAR = jet.STAR
