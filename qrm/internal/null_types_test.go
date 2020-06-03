package internal

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestNullByteArray(t *testing.T) {
	var array NullByteArray

	require.NoError(t, array.Scan(nil))
	require.Equal(t, array.Valid, false)

	require.NoError(t, array.Scan([]byte("bytea")))
	require.Equal(t, array.Valid, true)
	require.Equal(t, string(array.ByteArray), string([]byte("bytea")))

	require.Error(t, array.Scan(12), "can't scan []byte from 12")
}

func TestNullTime(t *testing.T) {
	var array NullTime

	require.NoError(t, array.Scan(nil))
	require.Equal(t, array.Valid, false)

	time := time.Now()
	require.NoError(t, array.Scan(time))
	require.Equal(t, array.Valid, true)
	value, _ := array.Value()
	require.Equal(t, value, time)

	require.NoError(t, array.Scan([]byte("13:10:11")))
	require.Equal(t, array.Valid, true)
	value, _ = array.Value()
	require.Equal(t, fmt.Sprintf("%v", value), "0000-01-01 13:10:11 +0000 UTC")

	require.NoError(t, array.Scan("13:10:11"))
	require.Equal(t, array.Valid, true)
	value, _ = array.Value()
	require.Equal(t, fmt.Sprintf("%v", value), "0000-01-01 13:10:11 +0000 UTC")

	require.Error(t, array.Scan(12), "can't scan time.Time from 12")
}

func TestNullInt8(t *testing.T) {
	var array NullInt8

	require.NoError(t, array.Scan(nil))
	require.Equal(t, array.Valid, false)

	require.NoError(t, array.Scan(int64(11)))
	require.Equal(t, array.Valid, true)
	value, _ := array.Value()
	require.Equal(t, value, int8(11))

	require.Error(t, array.Scan("text"), "can't scan int8 from text")
}

func TestNullInt16(t *testing.T) {
	var array NullInt16

	require.NoError(t, array.Scan(nil))
	require.Equal(t, array.Valid, false)

	require.NoError(t, array.Scan(int64(11)))
	require.Equal(t, array.Valid, true)
	value, _ := array.Value()
	require.Equal(t, value, int16(11))

	require.NoError(t, array.Scan(int16(20)))
	require.Equal(t, array.Valid, true)
	value, _ = array.Value()
	require.Equal(t, value, int16(20))

	require.NoError(t, array.Scan(int8(30)))
	require.Equal(t, array.Valid, true)
	value, _ = array.Value()
	require.Equal(t, value, int16(30))

	require.NoError(t, array.Scan(uint8(30)))
	require.Equal(t, array.Valid, true)
	value, _ = array.Value()
	require.Equal(t, value, int16(30))

	require.Error(t, array.Scan("text"), "can't scan int16 from text")
}

func TestNullInt32(t *testing.T) {
	var array NullInt32

	require.NoError(t, array.Scan(nil))
	require.Equal(t, array.Valid, false)

	require.NoError(t, array.Scan(int64(11)))
	require.Equal(t, array.Valid, true)
	value, _ := array.Value()
	require.Equal(t, value, int32(11))

	require.NoError(t, array.Scan(int32(32)))
	require.Equal(t, array.Valid, true)
	value, _ = array.Value()
	require.Equal(t, value, int32(32))

	require.NoError(t, array.Scan(int16(20)))
	require.Equal(t, array.Valid, true)
	value, _ = array.Value()
	require.Equal(t, value, int32(20))

	require.NoError(t, array.Scan(uint16(16)))
	require.Equal(t, array.Valid, true)
	value, _ = array.Value()
	require.Equal(t, value, int32(16))

	require.NoError(t, array.Scan(int8(30)))
	require.Equal(t, array.Valid, true)
	value, _ = array.Value()
	require.Equal(t, value, int32(30))

	require.NoError(t, array.Scan(uint8(30)))
	require.Equal(t, array.Valid, true)
	value, _ = array.Value()
	require.Equal(t, value, int32(30))

	require.Error(t, array.Scan("text"), "can't scan int32 from text")
}

func TestNullFloat32(t *testing.T) {
	var array NullFloat32

	require.NoError(t, array.Scan(nil))
	require.Equal(t, array.Valid, false)

	require.NoError(t, array.Scan(float64(64)))
	require.Equal(t, array.Valid, true)
	value, _ := array.Value()
	require.Equal(t, value, float32(64))

	require.NoError(t, array.Scan(float32(32)))
	require.Equal(t, array.Valid, true)
	value, _ = array.Value()
	require.Equal(t, value, float32(32))

	require.Error(t, array.Scan(12), "can't scan float32 from 12")
}
