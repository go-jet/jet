package postgres

import (
	"github.com/go-jet/jet/internal/jet"
	"time"
)

var Bool = jet.Bool
var Int = jet.Int
var Float = jet.Float
var String = jet.String

var Bytea = func(value string) StringExpression {
	return CAST(jet.String(value)).AS_BYTEA()
}

var Date = func(year int, month time.Month, day int) DateExpression {
	return CAST(jet.Date(year, month, day)).AS_DATE()
}

var DateT = func(t time.Time) DateExpression {
	return CAST(jet.DateT(t)).AS_DATE()
}

var Time = func(hour, minute, second int, milliseconds ...int) TimeExpression {
	return CAST(jet.Time(hour, minute, second, milliseconds...)).AS_TIME()
}

var TimeT = func(t time.Time) TimeExpression {
	return CAST(jet.TimeT(t)).AS_TIME()
}

var Timez = func(hour, minute, second, milliseconds int, timezone int) TimezExpression {
	return CAST(jet.Timez(hour, minute, second, milliseconds, timezone)).AS_TIMEZ()
}

var TimezT = func(t time.Time) TimezExpression {
	return CAST(jet.TimezT(t)).AS_TIMEZ()
}

var Timestamp = func(year int, month time.Month, day, hour, minute, second, milliseconds int) TimestampExpression {
	return CAST(jet.Timestamp(year, month, day, hour, minute, second, milliseconds)).AS_TIMESTAMP()
}

var TimestampT = func(t time.Time) TimestampExpression {
	return CAST(jet.TimestampzT(t)).AS_TIMESTAMP()
}

var Timestampz = func(year, month, day, hour, minute, second, milliseconds int, timezone int) TimestampzExpression {
	return CAST(jet.Timestampz(year, month, day, hour, minute, second, milliseconds, timezone)).AS_TIMESTAMPZ()
}

var TimestampzT = func(t time.Time) TimestampzExpression {
	return CAST(jet.TimestampzT(t)).AS_TIMESTAMPZ()
}
