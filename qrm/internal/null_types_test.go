package internal

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestNullBool(t *testing.T) {
	var nullBool NullBool

	require.NoError(t, nullBool.Scan(nil))
	require.Equal(t, nullBool.Valid, false)

	require.NoError(t, nullBool.Scan(int64(1)))
	require.Equal(t, nullBool.Valid, true)
	value, _ := nullBool.Value()
	require.Equal(t, value, true)

	require.NoError(t, nullBool.Scan(uint32(0)))
	require.Equal(t, nullBool.Valid, true)
	value, _ = nullBool.Value()
	require.Equal(t, value, false)

	require.EqualError(t, nullBool.Scan(uint16(22)), "can't assign uint16(22) to bool")
}

func TestNullTime(t *testing.T) {
	var nullTime NullTime

	require.NoError(t, nullTime.Scan(nil))
	require.Equal(t, nullTime.Valid, false)

	time := time.Now()
	require.NoError(t, nullTime.Scan(time))
	require.Equal(t, nullTime.Valid, true)
	value, _ := nullTime.Value()
	require.Equal(t, value, time)

	require.NoError(t, nullTime.Scan([]byte("13:10:11")))
	require.Equal(t, nullTime.Valid, true)
	value, _ = nullTime.Value()
	require.Equal(t, fmt.Sprintf("%v", value), "0000-01-01 13:10:11 +0000 UTC")

	require.NoError(t, nullTime.Scan("13:10:11"))
	require.Equal(t, nullTime.Valid, true)
	value, _ = nullTime.Value()
	require.Equal(t, fmt.Sprintf("%v", value), "0000-01-01 13:10:11 +0000 UTC")

	require.Error(t, nullTime.Scan(12), "can't scan time.Time from 12")
}

func TestNullUInt64(t *testing.T) {
	var nullUInt64 NullUInt64

	require.NoError(t, nullUInt64.Scan(nil))
	require.Equal(t, nullUInt64.Valid, false)

	require.NoError(t, nullUInt64.Scan(int64(11)))
	require.Equal(t, nullUInt64.Valid, true)
	value, _ := nullUInt64.Value()
	require.Equal(t, value, uint64(11))

	require.NoError(t, nullUInt64.Scan(uint64(11)))
	require.Equal(t, nullUInt64.Valid, true)
	value, _ = nullUInt64.Value()
	require.Equal(t, value, uint64(11))

	require.NoError(t, nullUInt64.Scan(int32(32)))
	require.Equal(t, nullUInt64.Valid, true)
	value, _ = nullUInt64.Value()
	require.Equal(t, value, uint64(32))

	require.NoError(t, nullUInt64.Scan(uint32(32)))
	require.Equal(t, nullUInt64.Valid, true)
	value, _ = nullUInt64.Value()
	require.Equal(t, value, uint64(32))

	require.NoError(t, nullUInt64.Scan(int16(20)))
	require.Equal(t, nullUInt64.Valid, true)
	value, _ = nullUInt64.Value()
	require.Equal(t, value, uint64(20))

	require.NoError(t, nullUInt64.Scan(uint16(16)))
	require.Equal(t, nullUInt64.Valid, true)
	value, _ = nullUInt64.Value()
	require.Equal(t, value, uint64(16))

	require.NoError(t, nullUInt64.Scan(int8(30)))
	require.Equal(t, nullUInt64.Valid, true)
	value, _ = nullUInt64.Value()
	require.Equal(t, value, uint64(30))

	require.NoError(t, nullUInt64.Scan(uint8(30)))
	require.Equal(t, nullUInt64.Valid, true)
	value, _ = nullUInt64.Value()
	require.Equal(t, value, uint64(30))

	require.Error(t, nullUInt64.Scan("text"), "can't scan int32 from text")

	//Validate negative use cases
	err := nullUInt64.Scan(int64(-5))
	assert.NotNil(t, err)
	assert.Error(t, err, castOverFlowError)

	//Validate negative use cases
	err = nullUInt64.Scan(-5)
	assert.NotNil(t, err)
	assert.Error(t, err, castOverFlowError)

	//Validate negative use cases
	err = nullUInt64.Scan(int32(-5))
	assert.NotNil(t, err)
	assert.Error(t, err, castOverFlowError)

	//Validate negative use cases
	err = nullUInt64.Scan(int16(-5))
	assert.NotNil(t, err)
	assert.Error(t, err, castOverFlowError)

	//Validate negative use cases
	err = nullUInt64.Scan(int8(-5))
	assert.NotNil(t, err)
	assert.Error(t, err, castOverFlowError)
}
