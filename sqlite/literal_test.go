package sqlite

import (
	"math"
	"testing"
	"time"
)

func TestBool(t *testing.T) {
	assertSerialize(t, Bool(false), `?`, false)
}

func TestInt(t *testing.T) {
	assertSerialize(t, Int(11), `?`, int64(11))
}

func TestInt8(t *testing.T) {
	val := int8(math.MinInt8)
	assertSerialize(t, Int8(val), `?`, val)
}

func TestInt16(t *testing.T) {
	val := int16(math.MinInt16)
	assertSerialize(t, Int16(val), `?`, val)
}

func TestInt32(t *testing.T) {
	val := int32(math.MinInt32)
	assertSerialize(t, Int32(val), `?`, val)
}

func TestInt64(t *testing.T) {
	val := int64(math.MinInt64)
	assertSerialize(t, Int64(val), `?`, val)
}

func TestUint8(t *testing.T) {
	val := uint8(math.MaxUint8)
	assertSerialize(t, Uint8(val), `?`, val)
}

func TestUint16(t *testing.T) {
	val := uint16(math.MaxUint16)
	assertSerialize(t, Uint16(val), `?`, val)
}

func TestUint32(t *testing.T) {
	val := uint32(math.MaxUint32)
	assertSerialize(t, Uint32(val), `?`, val)
}

func TestUint64(t *testing.T) {
	val := uint64(math.MaxUint64)
	assertSerialize(t, Uint64(val), `?`, val)
}

func TestFloat(t *testing.T) {
	assertSerialize(t, Float(12.34), `?`, float64(12.34))
}

func TestString(t *testing.T) {
	assertSerialize(t, String("Some text"), `?`, "Some text")
}

var testTime = time.Now()

func TestDate(t *testing.T) {
	assertSerialize(t, Date(2014, time.January, 2), "DATE(?)", "2014-01-02")
	assertSerialize(t, DATE(testTime), "DATE(?)", testTime)
}

func TestTime(t *testing.T) {
	assertSerialize(t, Time(10, 15, 30), `TIME(?)`, "10:15:30")
	assertSerialize(t, TIME(testTime), "TIME(?)", testTime)
}

func TestDateTime(t *testing.T) {
	assertSerialize(t, DateTime(2010, time.March, 30, 10, 15, 30), `DATETIME(?)`, "2010-03-30 10:15:30")
	assertSerialize(t, DATETIME(testTime), `DATETIME(?)`, testTime)
}
