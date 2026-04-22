package cubrid

import (
	"time"

	"github.com/go-jet/jet/v2/internal/jet"
)

// Keywords
var (
	STAR    = jet.STAR
	NULL    = jet.NULL
	DEFAULT = jet.DEFAULT
)

var (
	Bool    = jet.Bool
	Int     = jet.Int
	Int8    = jet.Int8
	Int16   = jet.Int16
	Int32   = jet.Int32
	Int64   = jet.Int
	Uint8   = jet.Uint8
	Uint16  = jet.Uint16
	Uint32  = jet.Uint32
	Uint64  = jet.Uint64
	Float   = jet.Float
	Decimal = jet.Decimal
	String  = jet.String
	UUID    = jet.UUID
)

// Date creates new date literal
func Date(year int, month time.Month, day int) DateExpression {
	return DateExp(jet.Date(year, month, day))
}

// DateT creates new date literal from time.Time
func DateT(t time.Time) DateExpression { return DateExp(jet.DateT(t)) }

// Time creates new time literal
func Time(hour, minute, second int, nanoseconds ...time.Duration) TimeExpression {
	return TimeExp(jet.Time(hour, minute, second, nanoseconds...))
}

// TimeT creates new time literal from time.Time
func TimeT(t time.Time) TimeExpression { return TimeExp(jet.TimeT(t)) }

// DateTime creates new datetime literal
func DateTime(year int, month time.Month, day, hour, minute, second int, nanoseconds ...time.Duration) DateTimeExpression {
	return TimestampExp(jet.Timestamp(year, month, day, hour, minute, second, nanoseconds...))
}

// DateTimeT creates new datetime literal from time.Time
func DateTimeT(t time.Time) DateTimeExpression { return TimestampExp(jet.TimestampT(t)) }

// Timestamp creates new timestamp literal
func Timestamp(year int, month time.Month, day, hour, minute, second int, nanoseconds ...time.Duration) TimestampExpression {
	return TimestampExp(jet.Timestamp(year, month, day, hour, minute, second, nanoseconds...))
}

// TimestampT creates new timestamp literal from time.Time
func TimestampT(t time.Time) TimestampExpression { return TimestampExp(jet.TimestampT(t)) }
