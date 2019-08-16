package mysql

import (
	"github.com/go-jet/jet/internal/jet"
	"time"
)

var STAR = jet.STAR
var NULL = jet.NULL
var DEFAULT = jet.DEFAULT

var Bool = jet.Bool
var Int = jet.Int
var Float = jet.Float
var String = jet.String

var Date = func(year int, month time.Month, day int) DateExpression {
	return CAST(jet.Date(year, month, day)).AS_DATE()
}
var DateT = func(t time.Time) DateExpression {
	return CAST(jet.DateT(t)).AS_DATE()
}
var Time = func(hour, minute, second int, nanoseconds ...time.Duration) TimeExpression {
	return CAST(jet.Time(hour, minute, second, nanoseconds...)).AS_TIME()
}
var TimeT = func(t time.Time) TimeExpression {
	return CAST(jet.TimeT(t)).AS_TIME()
}
var DateTime = func(year int, month time.Month, day, hour, minute, second int, nanoseconds ...time.Duration) DateTimeExpression {
	return CAST(jet.Timestamp(year, month, day, hour, minute, second, nanoseconds...)).AS_DATETIME()
}
var DateTimeT = func(t time.Time) DateTimeExpression {
	return CAST(jet.TimestampT(t)).AS_DATETIME()
}
var Timestamp = func(year int, month time.Month, day, hour, minute, second int, nanoseconds ...time.Duration) TimestampExpression {
	return TIMESTAMP(StringExp(jet.Timestamp(year, month, day, hour, minute, second, nanoseconds...)))
}
var TimestampT = func(t time.Time) TimestampExpression {
	return TIMESTAMP(StringExp(jet.TimestampT(t)))
}
