package mysql

import "github.com/go-jet/jet/internal/jet"

type Expression jet.Expression

type BoolExpression jet.BoolExpression

type StringExpression jet.StringExpression

type IntegerExpression jet.IntegerExpression

type FloatExpression jet.FloatExpression

type TimeExpression jet.TimeExpression

type DateExpression jet.DateExpression

type DateTimeExpression jet.TimestampExpression

type TimestampExpression jet.TimestampExpression

var BoolExp = jet.BoolExp
var StringExp = jet.StringExp
var IntExp = jet.IntExp
var FloatExp = jet.FloatExp
var TimeExp = jet.TimeExp
var DateExp = jet.DateExp
var DateTimeExp = jet.TimestampExp
var TimestampExp = jet.TimestampExp

var RAW = jet.RAW
