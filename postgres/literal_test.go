package postgres

import (
	"math"
	"testing"
	"time"
)

func TestBool(t *testing.T) {
	assertSerialize(t, Bool(false), `$1::boolean`, false)
}

func TestInt(t *testing.T) {
	assertSerialize(t, Int(11), `$1`, int64(11))
}

func TestInt8(t *testing.T) {
	val := int8(math.MinInt8)
	assertSerialize(t, Int8(val), `$1::smallint`, val)
}

func TestInt16(t *testing.T) {
	val := int16(math.MinInt16)
	assertSerialize(t, Int16(val), `$1::smallint`, val)
}

func TestInt32(t *testing.T) {
	val := int32(math.MinInt32)
	assertSerialize(t, Int32(val), `$1::integer`, val)
}

func TestInt64(t *testing.T) {
	val := int64(math.MinInt64)
	assertSerialize(t, Int64(val), `$1::bigint`, val)
}

func TestUint8(t *testing.T) {
	val := uint8(math.MaxUint8)
	assertSerialize(t, Uint8(val), `$1::smallint`, val)
}

func TestUint16(t *testing.T) {
	val := uint16(math.MaxUint16)
	assertSerialize(t, Uint16(val), `$1::integer`, val)
}

func TestUint32(t *testing.T) {
	val := uint32(math.MaxUint32)
	assertSerialize(t, Uint32(val), `$1::bigint`, val)
}

func TestFloat(t *testing.T) {
	assertSerialize(t, Float(12.34), `$1`, float64(12.34))

	assertSerialize(t, Real(12.34), `$1::real`, float32(12.34))
	assertSerialize(t, Double(12.34), `$1::double precision`, float64(12.34))
}

func TestString(t *testing.T) {
	assertSerialize(t, String("Some text"), `$1::text`, "Some text")

	assertSerialize(t, Text("Some text"), `$1::text`, "Some text")
	assertSerialize(t, Char(20)("John Doe"), `$1::char(20)`, "John Doe")
	assertSerialize(t, Char()("John Doe"), `$1::char`, "John Doe")
	assertSerialize(t, VarChar(20)("John Doe"), `$1::varchar(20)`, "John Doe")
	assertSerialize(t, VarChar()("John Doe"), `$1::varchar`, "John Doe")
}

func TestBytea(t *testing.T) {
	assertSerialize(t, Bytea("Some text"), `$1::bytea`, "Some text")
	assertSerialize(t, Bytea([]byte("Some byte array")), `$1::bytea`, []byte("Some byte array"))
}

func TestJson(t *testing.T) {
	assertSerialize(t, Json("{\"key\": \"value\"}"), `$1::json`, "{\"key\": \"value\"}")
	assertSerialize(t, Json([]byte("{\"key\": \"value\"}")), `$1::json`, []byte("{\"key\": \"value\"}"))
}

func TestDate(t *testing.T) {
	assertSerialize(t, Date(2014, time.January, 2), `$1::date`, "2014-01-02")
	assertSerialize(t, DateT(time.Now()), `$1::date`)
}

func TestTime(t *testing.T) {
	assertSerialize(t, Time(10, 15, 30), `$1::time without time zone`, "10:15:30")
	assertSerialize(t, TimeT(time.Now()), `$1::time without time zone`)
}

func TestTimez(t *testing.T) {
	assertSerialize(t, Timez(10, 15, 30, 0, "UTC"),
		`$1::time with time zone`, "10:15:30 UTC")
	assertSerialize(t, TimezT(time.Now()), `$1::time with time zone`)
}

func TestTimestamp(t *testing.T) {
	assertSerialize(t, Timestamp(2010, time.March, 30, 10, 15, 30),
		`$1::timestamp without time zone`, "2010-03-30 10:15:30")
	assertSerialize(t, TimestampT(time.Now()), `$1::timestamp without time zone`)
}

func TestTimestampz(t *testing.T) {
	assertSerialize(t, Timestampz(2010, time.March, 30, 10, 15, 30, 0, "UTC"),
		`$1::timestamp with time zone`, "2010-03-30 10:15:30 UTC")
	assertSerialize(t, TimestampzT(time.Now()), `$1::timestamp with time zone`)
}
