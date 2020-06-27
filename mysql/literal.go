package mysql

import (
	"github.com/go-jet/jet/v2/internal/jet"
	"time"
)

// Keywords
var (
	STAR    = jet.STAR
	NULL    = jet.NULL
	DEFAULT = jet.DEFAULT
)

// Bool creates new bool literal expression
var Bool = jet.Bool

// Int is constructor for integer expressions literals.
var Int = jet.Int

// Float creates new float literal expression
var Float = jet.Float

// String creates new string literal expression
var String = jet.String

// Date creates new date literal
var Date = func(year int, month time.Month, day int) DateExpression {
	return CAST(jet.Date(year, month, day)).AS_DATE()
}

// DateT creates new date literal from time.Time
var DateT = func(t time.Time) DateExpression {
	return CAST(jet.DateT(t)).AS_DATE()
}

// Time creates new time literal
var Time = func(hour, minute, second int, nanoseconds ...time.Duration) TimeExpression {
	return CAST(jet.Time(hour, minute, second, nanoseconds...)).AS_TIME()
}

// TimeT creates new time literal from time.Time
var TimeT = func(t time.Time) TimeExpression {
	return CAST(jet.TimeT(t)).AS_TIME()
}

// DateTime creates new datetime literal
var DateTime = func(year int, month time.Month, day, hour, minute, second int, nanoseconds ...time.Duration) DateTimeExpression {
	return CAST(jet.Timestamp(year, month, day, hour, minute, second, nanoseconds...)).AS_DATETIME()
}

// DateTimeT creates new datetime literal from time.Time
var DateTimeT = func(t time.Time) DateTimeExpression {
	return CAST(jet.TimestampT(t)).AS_DATETIME()
}

// Timestamp creates new timestamp literal
var Timestamp = func(year int, month time.Month, day, hour, minute, second int, nanoseconds ...time.Duration) TimestampExpression {
	return TIMESTAMP(StringExp(jet.Timestamp(year, month, day, hour, minute, second, nanoseconds...)))
}

// TimestampT creates new timestamp literal from time.Time
var TimestampT = func(t time.Time) TimestampExpression {
	return TIMESTAMP(StringExp(jet.TimestampT(t)))
}
