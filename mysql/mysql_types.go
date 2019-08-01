package mysql

import "github.com/go-jet/jet"

type Expression jet.Expression

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

type ColumnDate jet.ColumnDate
type DateExpression jet.DateExpression

var DateColumn = jet.DateColumn
var Date = jet.Date

type ColumnDateTime jet.ColumnTimestamp
type DateTimeExpression jet.TimestampExpression

var DateTimeColumn = jet.TimestampColumn
var DateTime = jet.Timestamp

type ColumnTimestamp jet.ColumnTimestamp
type TimestampExpression jet.TimestampExpression

var TimestampColumn = jet.TimestampColumn
var Timestamp = jet.Timestamp

type TimeExpression jet.TimeExpression

// ----------------- FUNCTIONS ----------------------//

var ABSf = jet.ABSf
var ABSi = jet.ABSi
var POWER = jet.POWER
var SQRT = jet.SQRT

func CBRT(number jet.NumericExpression) jet.FloatExpression {
	return POWER(number, Float(1.0).DIV(Float(3.0)))
}

var CEIL = jet.CEIL
var FLOOR = jet.FLOOR
var ROUND = jet.ROUND
var SIGN = jet.SIGN
var TRUNC = TRUNCATE

var TRUNCATE = func(floatExpression jet.FloatExpression, precision jet.IntegerExpression) jet.FloatExpression {
	return jet.NewFloatFunc("TRUNCATE", floatExpression, precision)
}

var MINUSi = jet.MINUSi
var MINUSf = jet.MINUSf
var BIT_NOT = jet.BIT_NOT

var SUMf = jet.SUMf
var AVG = jet.AVG
var MAXf = jet.MAXf
var MINf = jet.MINf
var COUNT = jet.COUNT

var SELECT = jet.SELECT

type SelectLock jet.SelectLock

var (
	UPDATE = jet.NewSelectLock("UPDATE")
	SHARE  = jet.NewSelectLock("SHARE")
)

var UNION = jet.UNION

//-----------------literals----------------------//

var STAR = jet.STAR
var NULL = jet.NULL
