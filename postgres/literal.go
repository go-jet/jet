package postgres

import (
	"time"

	"github.com/go-jet/jet/v2/internal/jet"
)

// Bool is boolean literal constructor
func Bool(value bool) BoolExpression {
	return CAST(jet.Bool(value)).AS_BOOL()
}

// Int is constructor for 64 bit signed integer expressions literals.
var Int = jet.Int

// Int8 is constructor for 8 bit signed integer expressions literals.
func Int8(value int8) IntegerExpression {
	return CAST(jet.Int8(value)).AS_SMALLINT()
}

// Int16 is constructor for 16 bit signed integer expressions literals.
func Int16(value int16) IntegerExpression {
	return CAST(jet.Int16(value)).AS_SMALLINT()
}

// Int32 is constructor for 32 bit signed integer expressions literals.
func Int32(value int32) IntegerExpression {
	return CAST(jet.Int32(value)).AS_INTEGER()
}

// Int64 is constructor for 64 bit signed integer expressions literals.
func Int64(value int64) IntegerExpression {
	return CAST(jet.Int(value)).AS_BIGINT()
}

// Uint8 is constructor for 8 bit unsigned integer expressions literals.
func Uint8(value uint8) IntegerExpression {
	return CAST(jet.Uint8(value)).AS_SMALLINT()
}

// Uint16 is constructor for 16 bit unsigned integer expressions literals.
func Uint16(value uint16) IntegerExpression {
	return CAST(jet.Uint16(value)).AS_INTEGER()
}

// Uint32 is constructor for 32 bit unsigned integer expressions literals.
func Uint32(value uint32) IntegerExpression {
	return CAST(jet.Uint32(value)).AS_BIGINT()
}

// Float creates new float literal expression
var Float = jet.Float

// Real is placeholder constructor for 32-bit float literals
func Real(value float32) FloatExpression {
	return CAST(jet.Literal(value)).AS_REAL()
}

// Double is placeholder constructor for 64-bit float literals
func Double(value float64) FloatExpression {
	return CAST(jet.Literal(value)).AS_DOUBLE()
}

// Decimal creates new float literal expression
var Decimal = jet.Decimal

// String is a parameter constructor for the PostgreSQL text type. Using the `Text` constructor is
// generally preferable.
//
// WARNING: String always applies a `text` type cast, which can be problematic if a parameter is compared
// to a `character` column, as this may prevent index usage. In such cases, consider using the Char
// constructor instead. See also other PostgreSQL-specific constructors: Text, Char, and VarChar.
func String(value string) StringExpression {
	return CAST(jet.String(value)).AS_TEXT()
}

// Text is a parameter constructor for the PostgreSQL text type. This constructor also adds an
// explicit placeholder type cast to text in the generated query, such as `$3::text`.
// Example usage:
//
//	Text("English")
func Text(value string) StringExpression {
	return CAST(jet.Literal(value)).AS_TEXT()
}

// Char is a parameter constructor for the PostgreSQL character type. This constructor also adds an
// explicit placeholder type cast to text in the generated query, such as `$3::char(30)`.
// Example usage:
//
//	Char(20)("English")
func Char(length ...int) func(value string) StringExpression {
	return func(value string) StringExpression {
		return CAST(StringExp(jet.Literal(value))).AS_CHAR(length...)
	}
}

// VarChar is a parameter constructor for the PostgreSQL character varying type. This constructor
// also adds an explicit placeholder type cast to text in the generated query, such as `$3::varchar(30)`.
// Example usage:
//
//	VarChar(20)("English")
//	VarChar()("English")
func VarChar(length ...int) func(value string) StringExpression {
	return func(value string) StringExpression {
		return CAST(StringExp(jet.Literal(value))).AS_VARCHAR(length...)
	}
}

// Json creates new json literal expression
func Json(value interface{}) StringExpression {
	switch value.(type) {
	case string, []byte:
	default:
		panic("Bytea parameter value has to be of the type string or []byte")
	}
	return StringExp(CAST(jet.Literal(value)).AS("json"))
}

// UUID is a helper function to create string literal expression from uuid object
// value can be any uuid type with a String method
var UUID = jet.UUID

// Bytea creates new bytea literal expression
func Bytea(value interface{}) StringExpression {
	switch value.(type) {
	case string, []byte:
	default:
		panic("Bytea parameter value has to be of the type string or []byte")
	}
	return CAST(jet.Literal(value)).AS_BYTEA()
}

// Date creates new date literal expression
func Date(year int, month time.Month, day int) DateExpression {
	return CAST(jet.Date(year, month, day)).AS_DATE()
}

// DateT creates new date literal expression from time.Time object
func DateT(t time.Time) DateExpression {
	return CAST(jet.DateT(t)).AS_DATE()
}

// Time creates new time literal expression
func Time(hour, minute, second int, nanoseconds ...time.Duration) TimeExpression {
	return CAST(jet.Time(hour, minute, second, nanoseconds...)).AS_TIME()
}

// TimeT creates new time literal expression from time.Time object
func TimeT(t time.Time) TimeExpression {
	return CAST(jet.TimeT(t)).AS_TIME()
}

// Timez creates new time with time zone literal expression
func Timez(hour, minute, second int, milliseconds time.Duration, timezone string) TimezExpression {
	return CAST(jet.Timez(hour, minute, second, milliseconds, timezone)).AS_TIMEZ()
}

// TimezT creates new time with time zone literal expression from time.Time object
func TimezT(t time.Time) TimezExpression {
	return CAST(jet.TimezT(t)).AS_TIMEZ()
}

// Timestamp creates new timestamp literal expression
func Timestamp(year int, month time.Month, day, hour, minute, second int, milliseconds ...time.Duration) TimestampExpression {
	return CAST(jet.Timestamp(year, month, day, hour, minute, second, milliseconds...)).AS_TIMESTAMP()
}

// TimestampT creates new timestamp literal expression from time.Time object
func TimestampT(t time.Time) TimestampExpression {
	return CAST(jet.TimestampT(t)).AS_TIMESTAMP()
}

// Timestampz creates new timestamp with time zone literal expression
func Timestampz(year int, month time.Month, day, hour, minute, second int, milliseconds time.Duration, timezone string) TimestampzExpression {
	return CAST(jet.Timestampz(year, month, day, hour, minute, second, milliseconds, timezone)).AS_TIMESTAMPZ()
}

// TimestampzT creates new timestamp literal expression from time.Time object
func TimestampzT(t time.Time) TimestampzExpression {
	return CAST(jet.TimestampzT(t)).AS_TIMESTAMPZ()
}
