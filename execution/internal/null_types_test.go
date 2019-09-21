package internal

import (
	"fmt"
	"gotest.tools/assert"
	"testing"
	"time"
)

func TestNullByteArray(t *testing.T) {
	var array NullByteArray

	assert.NilError(t, array.Scan(nil))
	assert.Equal(t, array.Valid, false)

	assert.NilError(t, array.Scan([]byte("bytea")))
	assert.Equal(t, array.Valid, true)
	assert.Equal(t, string(array.ByteArray), string([]byte("bytea")))

	assert.Error(t, array.Scan(12), "can't scan []byte from 12")
}

func TestNullTime(t *testing.T) {
	var array NullTime

	assert.NilError(t, array.Scan(nil))
	assert.Equal(t, array.Valid, false)

	time := time.Now()
	assert.NilError(t, array.Scan(time))
	assert.Equal(t, array.Valid, true)
	value, _ := array.Value()
	assert.Equal(t, value, time)

	assert.NilError(t, array.Scan([]byte("13:10:11")))
	assert.Equal(t, array.Valid, true)
	value, _ = array.Value()
	assert.Equal(t, fmt.Sprintf("%v", value), "0000-01-01 13:10:11 +0000 UTC")

	assert.NilError(t, array.Scan("13:10:11"))
	assert.Equal(t, array.Valid, true)
	value, _ = array.Value()
	assert.Equal(t, fmt.Sprintf("%v", value), "0000-01-01 13:10:11 +0000 UTC")

	assert.Error(t, array.Scan(12), "can't scan time.Time from 12")
}

func TestNullInt8(t *testing.T) {
	var array NullInt8

	assert.NilError(t, array.Scan(nil))
	assert.Equal(t, array.Valid, false)

	assert.NilError(t, array.Scan(int64(11)))
	assert.Equal(t, array.Valid, true)
	value, _ := array.Value()
	assert.Equal(t, value, int8(11))

	assert.Error(t, array.Scan("text"), "can't scan int8 from text")
}

func TestNullInt16(t *testing.T) {
	var array NullInt16

	assert.NilError(t, array.Scan(nil))
	assert.Equal(t, array.Valid, false)

	assert.NilError(t, array.Scan(int64(11)))
	assert.Equal(t, array.Valid, true)
	value, _ := array.Value()
	assert.Equal(t, value, int16(11))

	assert.NilError(t, array.Scan(int16(20)))
	assert.Equal(t, array.Valid, true)
	value, _ = array.Value()
	assert.Equal(t, value, int16(20))

	assert.NilError(t, array.Scan(int8(30)))
	assert.Equal(t, array.Valid, true)
	value, _ = array.Value()
	assert.Equal(t, value, int16(30))

	assert.NilError(t, array.Scan(uint8(30)))
	assert.Equal(t, array.Valid, true)
	value, _ = array.Value()
	assert.Equal(t, value, int16(30))

	assert.Error(t, array.Scan("text"), "can't scan int16 from text")
}

func TestNullInt32(t *testing.T) {
	var array NullInt32

	assert.NilError(t, array.Scan(nil))
	assert.Equal(t, array.Valid, false)

	assert.NilError(t, array.Scan(int64(11)))
	assert.Equal(t, array.Valid, true)
	value, _ := array.Value()
	assert.Equal(t, value, int32(11))

	assert.NilError(t, array.Scan(int32(32)))
	assert.Equal(t, array.Valid, true)
	value, _ = array.Value()
	assert.Equal(t, value, int32(32))

	assert.NilError(t, array.Scan(int16(20)))
	assert.Equal(t, array.Valid, true)
	value, _ = array.Value()
	assert.Equal(t, value, int32(20))

	assert.NilError(t, array.Scan(uint16(16)))
	assert.Equal(t, array.Valid, true)
	value, _ = array.Value()
	assert.Equal(t, value, int32(16))

	assert.NilError(t, array.Scan(int8(30)))
	assert.Equal(t, array.Valid, true)
	value, _ = array.Value()
	assert.Equal(t, value, int32(30))

	assert.NilError(t, array.Scan(uint8(30)))
	assert.Equal(t, array.Valid, true)
	value, _ = array.Value()
	assert.Equal(t, value, int32(30))

	assert.Error(t, array.Scan("text"), "can't scan int32 from text")
}

func TestNullFloat32(t *testing.T) {
	var array NullFloat32

	assert.NilError(t, array.Scan(nil))
	assert.Equal(t, array.Valid, false)

	assert.NilError(t, array.Scan(float64(64)))
	assert.Equal(t, array.Valid, true)
	value, _ := array.Value()
	assert.Equal(t, value, float32(64))

	assert.NilError(t, array.Scan(float32(32)))
	assert.Equal(t, array.Valid, true)
	value, _ = array.Value()
	assert.Equal(t, value, float32(32))

	assert.Error(t, array.Scan(12), "can't scan float32 from 12")
}
