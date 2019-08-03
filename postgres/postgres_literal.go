package postgres

import "github.com/go-jet/jet/internal/jet"

var Bool = jet.Bool
var Int = jet.Int
var Float = jet.Float
var String = jet.String
var Date = func(year, month, day int) DateExpression {
	return CAST(jet.Date(year, month, day)).AS_DATE()
}

var Time = func(hour, minute, second, milliseconds int) TimeExpression {
	return CAST(jet.Time(hour, minute, second, milliseconds)).AS_TIME()
}

var Timez = func(hour, minute, second, milliseconds int, timezone int) TimezExpression {
	return CAST(jet.Timez(hour, minute, second, milliseconds, timezone)).AS_TIMEZ()
}

var Timestamp = func(year, month, day, hour, minute, second, milliseconds int) TimestampExpression {
	return CAST(jet.Timestamp(year, month, day, hour, minute, second, milliseconds)).AS_TIMESTAMP()
}

var Timestampz = func(year, month, day, hour, minute, second, milliseconds int, timezone int) TimestampzExpression {
	return CAST(jet.Timestampz(year, month, day, hour, minute, second, milliseconds, timezone)).AS_TIMESTAMPZ()
}
