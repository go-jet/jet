package postgres

import (
	"testing"
	"time"
)

func TestBool(t *testing.T) {
	assertClauseSerialize(t, Bool(false), `$1`, false)
}

func TestInt(t *testing.T) {
	assertClauseSerialize(t, Int(11), `$1`, int64(11))
}

func TestFloat(t *testing.T) {
	assertClauseSerialize(t, Float(12.34), `$1`, float64(12.34))
}

func TestString(t *testing.T) {
	assertClauseSerialize(t, String("Some text"), `$1`, "Some text")
}

func TestDate(t *testing.T) {
	assertClauseSerialize(t, Date(2014, time.January, 2), `$1::date`, "2014-01-02")
	assertClauseSerialize(t, DateT(time.Now()), `$1::date`)
}

func TestTime(t *testing.T) {
	assertClauseSerialize(t, Time(10, 15, 30), `$1::time without time zone`, "10:15:30")
	assertClauseSerialize(t, TimeT(time.Now()), `$1::time without time zone`)
}

func TestTimez(t *testing.T) {
	assertClauseSerialize(t, Timez(10, 15, 30, 0, "UTC"),
		`$1::time with time zone`, "10:15:30 UTC")
	assertClauseSerialize(t, TimezT(time.Now()), `$1::time with time zone`)
}

func TestTimestamp(t *testing.T) {
	assertClauseSerialize(t, Timestamp(2010, time.March, 30, 10, 15, 30),
		`$1::timestamp without time zone`, "2010-03-30 10:15:30")
	assertClauseSerialize(t, TimestampT(time.Now()), `$1::timestamp without time zone`)
}

func TestTimestampz(t *testing.T) {
	assertClauseSerialize(t, Timestampz(2010, time.March, 30, 10, 15, 30, 0, "UTC"),
		`$1::timestamp with time zone`, "2010-03-30 10:15:30 UTC")
	assertClauseSerialize(t, TimestampzT(time.Now()), `$1::timestamp with time zone`)
}
