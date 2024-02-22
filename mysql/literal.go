package mysql

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

// Bool creates new bool literal expression
var Bool = jet.Bool

// Int is constructor for 64 bit signed integer expressions literals.
var Int = jet.Int

// Int8 is constructor for 8 bit signed integer expressions literals.
var Int8 = jet.Int8

// Int16 is constructor for 16 bit signed integer expressions literals.
var Int16 = jet.Int16

// Int32 is constructor for 32 bit signed integer expressions literals.
var Int32 = jet.Int32

// Int64 is constructor for 64 bit signed integer expressions literals.
var Int64 = jet.Int

// Uint8 is constructor for 8 bit unsigned integer expressions literals.
var Uint8 = jet.Uint8

// Uint16 is constructor for 16 bit unsigned integer expressions literals.
var Uint16 = jet.Uint16

// Uint32 is constructor for 32 bit unsigned integer expressions literals.
var Uint32 = jet.Uint32

// Uint64 is constructor for 64 bit unsigned integer expressions literals.
var Uint64 = jet.Uint64

// Float creates new float literal expression from float64 value
var Float = jet.Float

// Decimal creates new float literal expression from string value
var Decimal = jet.Decimal

// String creates new string literal expression
var String = jet.String

// UUID is a helper function to create string literal expression from uuid object
// value can be any uuid type with a String method
var UUID = jet.UUID

// Date creates new date literal
func Date(year int, month time.Month, day int) DateExpression {
	return CAST(jet.Date(year, month, day)).AS_DATE()
}

// DateT creates new date literal from time.Time
func DateT(t time.Time) DateExpression {
	return CAST(jet.DateT(t)).AS_DATE()
}

// Time creates new time literal
func Time(hour, minute, second int, nanoseconds ...time.Duration) TimeExpression {
	return CAST(jet.Time(hour, minute, second, nanoseconds...)).AS_TIME()
}

// TimeT creates new time literal from time.Time
func TimeT(t time.Time) TimeExpression {
	return CAST(jet.TimeT(t)).AS_TIME()
}

// DateTime creates new datetime literal
func DateTime(year int, month time.Month, day, hour, minute, second int, nanoseconds ...time.Duration) DateTimeExpression {
	return CAST(jet.Timestamp(year, month, day, hour, minute, second, nanoseconds...)).AS_DATETIME()
}

// DateTimeT creates new datetime literal from time.Time
func DateTimeT(t time.Time) DateTimeExpression {
	return CAST(jet.TimestampT(t)).AS_DATETIME()
}

// Timestamp creates new timestamp literal
func Timestamp(year int, month time.Month, day, hour, minute, second int, nanoseconds ...time.Duration) TimestampExpression {
	return TIMESTAMP(StringExp(jet.Timestamp(year, month, day, hour, minute, second, nanoseconds...)))
}

// TimestampT creates new timestamp literal from time.Time
func TimestampT(t time.Time) TimestampExpression {
	return TIMESTAMP(StringExp(jet.TimestampT(t)))
}
