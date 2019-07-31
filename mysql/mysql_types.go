package mysql

import "github.com/go-jet/jet"

type ColumnBool jet.ColumnBool

var BoolColumn = jet.BoolColumn
var Bool = jet.Bool

type ColumnString jet.ColumnString

var StringColumn = jet.StringColumn
var String = jet.String

type ColumnInteger jet.ColumnInteger

var IntegerColumn = jet.IntegerColumn
var Int = jet.Int

type ColumnFloat jet.ColumnFloat

var FloatColumn = jet.FloatColumn
var Float = jet.Float

type ColumnDate jet.ColumnDate

var DateColumn = jet.DateColumn
var Date = jet.Date

type ColumnDateTime jet.ColumnTimestamp

var DateTimeColumn = jet.TimestampColumn
var DateTime = jet.Timestamp

type ColumnTimestamp jet.ColumnTimestamp

var TimestampColumn = jet.TimestampColumn
var Timestamp = jet.Timestamp

var CAST = jet.CAST

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
