package cubrid

import (
	"math"
	"testing"
	"time"
)

func TestLiteralBool(t *testing.T) {
	assertSerialize(t, Bool(false), `?`, false)
}

func TestLiteralInt(t *testing.T) {
	assertSerialize(t, Int(11), `?`, int64(11))
}

func TestLiteralInt8(t *testing.T) {
	val := int8(math.MinInt8)
	assertSerialize(t, Int8(val), `?`, val)
}

func TestLiteralInt16(t *testing.T) {
	val := int16(math.MinInt16)
	assertSerialize(t, Int16(val), `?`, val)
}

func TestLiteralInt32(t *testing.T) {
	val := int32(math.MinInt32)
	assertSerialize(t, Int32(val), `?`, val)
}

func TestLiteralInt64(t *testing.T) {
	val := int64(math.MinInt64)
	assertSerialize(t, Int64(val), `?`, val)
}

func TestLiteralUint8(t *testing.T) {
	val := uint8(math.MaxUint8)
	assertSerialize(t, Uint8(val), `?`, val)
}

func TestLiteralUint16(t *testing.T) {
	val := uint16(math.MaxUint16)
	assertSerialize(t, Uint16(val), `?`, val)
}

func TestLiteralUint32(t *testing.T) {
	val := uint32(math.MaxUint32)
	assertSerialize(t, Uint32(val), `?`, val)
}

func TestLiteralUint64(t *testing.T) {
	val := uint64(math.MaxUint64)
	assertSerialize(t, Uint64(val), `?`, val)
}

func TestLiteralFloat(t *testing.T) {
	assertSerialize(t, Float(12.34), `?`, float64(12.34))
}

func TestLiteralString(t *testing.T) {
	assertSerialize(t, String("Some text"), `?`, "Some text")
}

func TestLiteralDate(t *testing.T) {
	assertSerialize(t, Date(2014, time.January, 2), `?`, "2014-01-02")
	assertSerialize(t, DateT(time.Now()), `?`)
}

func TestLiteralTime(t *testing.T) {
	assertSerialize(t, Time(10, 15, 30), `?`, "10:15:30")
	assertSerialize(t, TimeT(time.Now()), `?`)
}

func TestLiteralDateTime(t *testing.T) {
	assertSerialize(t, DateTime(2010, time.March, 30, 10, 15, 30), `?`, "2010-03-30 10:15:30")
	assertSerialize(t, DateTimeT(time.Now()), `?`)
}

func TestLiteralTimestamp(t *testing.T) {
	assertSerialize(t, Timestamp(2010, time.March, 30, 10, 15, 30), `?`, "2010-03-30 10:15:30")
	assertSerialize(t, TimestampT(time.Now()), `?`)
}
