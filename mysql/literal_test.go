package mysql

import (
	"testing"
	"time"
)

func TestBool(t *testing.T) {
	assertClauseSerialize(t, Bool(false), `?`, false)
}

func TestInt(t *testing.T) {
	assertClauseSerialize(t, Int(11), `?`, int64(11))
}

func TestFloat(t *testing.T) {
	assertClauseSerialize(t, Float(12.34), `?`, float64(12.34))
}

func TestString(t *testing.T) {
	assertClauseSerialize(t, String("Some text"), `?`, "Some text")
}

func TestDate(t *testing.T) {
	assertClauseSerialize(t, Date(2014, time.January, 2), `CAST(? AS DATE)`, "2014-01-02")
	assertClauseSerialize(t, DateT(time.Now()), `CAST(? AS DATE)`)
}

func TestTime(t *testing.T) {
	assertClauseSerialize(t, Time(10, 15, 30), `CAST(? AS TIME)`, "10:15:30")
	assertClauseSerialize(t, TimeT(time.Now()), `CAST(? AS TIME)`)
}

func TestDateTime(t *testing.T) {
	assertClauseSerialize(t, DateTime(2010, time.March, 30, 10, 15, 30), `CAST(? AS DATETIME)`, "2010-03-30 10:15:30")
	assertClauseSerialize(t, DateTimeT(time.Now()), `CAST(? AS DATETIME)`)
}

func TestTimestamp(t *testing.T) {
	assertClauseSerialize(t, Timestamp(2010, time.March, 30, 10, 15, 30), `TIMESTAMP(?)`, "2010-03-30 10:15:30")
	assertClauseSerialize(t, TimestampT(time.Now()), `TIMESTAMP(?)`)
}
