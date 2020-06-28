package postgres

import (
	"github.com/go-jet/jet/v2/internal/jet"
	"time"
)

// Bool creates new bool literal expression
var Bool = jet.Bool

// Int creates new integer literal expression
var Int = jet.Int

// Float creates new float literal expression
var Float = jet.Float

// String creates new string literal expression
var String = jet.String

// Bytea craates new bytea literal expression
var Bytea = func(value string) StringExpression {
	return CAST(jet.String(value)).AS_BYTEA()
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
