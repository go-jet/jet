package mysql

import (
	"github.com/go-jet/jet/internal/jet"
	"time"
)

var Bool = jet.Bool
var Int = jet.Int
var Float = jet.Float
var String = jet.String

var Date = jet.Date
var DateT = func(t time.Time) DateExpression {
	return CAST(jet.DateT(t)).AS_DATE()
}
var Time = jet.Time
var TimeT = func(t time.Time) TimeExpression {
	return CAST(jet.TimeT(t)).AS_TIME()
}
var DateTime = jet.Timestamp
var DateTimeT = func(t time.Time) DateTimeExpression {
	return CAST(jet.TimestampT(t)).AS_DATETIME()
}
var Timestamp = jet.Timestamp
var TimestampT = func(t time.Time) TimestampExpression {
	return CAST(jet.TimestampT(t)).AS_TIMESTAMP()
}
