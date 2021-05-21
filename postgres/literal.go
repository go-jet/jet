package postgres

import (
	"time"

	"github.com/go-jet/jet/v2/internal/jet"
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

// Float creates new float literal expression
var Float = jet.Float

// Decimal creates new float literal expression
var Decimal = jet.Decimal

// String creates new string literal expression
var String = jet.String

// UUID is a helper function to create string literal expression from uuid object
// value can be any uuid type with a String method
var UUID = jet.UUID

// Bytea creates new bytea literal expression
var Bytea = func(value interface{}) StringExpression {
	switch value.(type) {
	case string, []byte:
	default:
		panic("Bytea parameter value has to be of the type string or []byte")
	}
	return CAST(jet.Literal(value)).AS_BYTEA()
}

// Date creates new date literal expression
var Date = func(year int, month time.Month, day int) DateExpression {
	return CAST(jet.Date(year, month, day)).AS_DATE()
}

// DateT creates new date literal expression from time.Time object
var DateT = func(t time.Time) DateExpression {
	return CAST(jet.DateT(t)).AS_DATE()
}

// Time creates new time literal expression
var Time = func(hour, minute, second int, nanoseconds ...time.Duration) TimeExpression {
	return CAST(jet.Time(hour, minute, second, nanoseconds...)).AS_TIME()
}

// TimeT creates new time literal expression from time.Time object
var TimeT = func(t time.Time) TimeExpression {
	return CAST(jet.TimeT(t)).AS_TIME()
}

// Timez creates new time with time zone literal expression
var Timez = func(hour, minute, second int, milliseconds time.Duration, timezone string) TimezExpression {
	return CAST(jet.Timez(hour, minute, second, milliseconds, timezone)).AS_TIMEZ()
}

// TimezT creates new time with time zone literal expression from time.Time object
var TimezT = func(t time.Time) TimezExpression {
	return CAST(jet.TimezT(t)).AS_TIMEZ()
}

// Timestamp creates new timestamp literal expression
var Timestamp = func(year int, month time.Month, day, hour, minute, second int, milliseconds ...time.Duration) TimestampExpression {
	return CAST(jet.Timestamp(year, month, day, hour, minute, second, milliseconds...)).AS_TIMESTAMP()
}

// TimestampT creates new timestamp literal expression from time.Time object
var TimestampT = func(t time.Time) TimestampExpression {
	return CAST(jet.TimestampT(t)).AS_TIMESTAMP()
}

// Timestampz creates new timestamp with time zone literal expression
var Timestampz = func(year int, month time.Month, day, hour, minute, second int, milliseconds time.Duration, timezone string) TimestampzExpression {
	return CAST(jet.Timestampz(year, month, day, hour, minute, second, milliseconds, timezone)).AS_TIMESTAMPZ()
}

// TimestampzT creates new timestamp literal expression from time.Time object
var TimestampzT = func(t time.Time) TimestampzExpression {
	return CAST(jet.TimestampzT(t)).AS_TIMESTAMPZ()
}
