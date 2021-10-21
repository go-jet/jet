package sqlite

import (
	"github.com/go-jet/jet/v2/internal/jet"
	"time"
)

// Keywords
var (
	STAR = jet.STAR
	NULL = jet.NULL
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

// Date creates new date literal expression
func Date(year int, month time.Month, day int) DateExpression {
	return DATE(jet.Date(year, month, day))
}

// Time creates new time literal expression
func Time(hour, minute, second int, nanoseconds ...time.Duration) TimeExpression {
	return TIME(jet.Time(hour, minute, second, nanoseconds...))
}

// DateTime creates new datetime(timestamp) literal expression
func DateTime(year int, month time.Month, day, hour, minute, second int, nanoseconds ...time.Duration) DateTimeExpression {
	return DATETIME(jet.Timestamp(year, month, day, hour, minute, second, nanoseconds...))
}
