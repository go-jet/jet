package jet

import (
	"github.com/google/uuid"
	"gotest.tools/assert"
	"testing"
	"time"
)

func TestArgToString(t *testing.T) {
	assert.Equal(t, argToString(true), "TRUE")
	assert.Equal(t, argToString(false), "FALSE")

	assert.Equal(t, argToString(int8(-8)), "-8")
	assert.Equal(t, argToString(int16(-16)), "-16")
	assert.Equal(t, argToString(int(-32)), "-32")
	assert.Equal(t, argToString(int32(-32)), "-32")
	assert.Equal(t, argToString(int64(-64)), "-64")
	assert.Equal(t, argToString(uint8(8)), "8")
	assert.Equal(t, argToString(uint16(16)), "16")
	assert.Equal(t, argToString(uint(32)), "32")
	assert.Equal(t, argToString(uint32(32)), "32")
	assert.Equal(t, argToString(uint64(64)), "64")

	assert.Equal(t, argToString("john"), "'john'")
	assert.Equal(t, argToString([]byte("john")), "'john'")
	assert.Equal(t, argToString(uuid.MustParse("b68dbff4-a87d-11e9-a7f2-98ded00c39c6")), "'b68dbff4-a87d-11e9-a7f2-98ded00c39c6'")

	time, err := time.Parse("Mon Jan 2 15:04:05 -0700 MST 2006", "Mon Jan 2 15:04:05 -0700 MST 2006")
	assert.NilError(t, err)
	assert.Equal(t, argToString(time), "'2006-01-02 15:04:05-07:00'")
	assert.Equal(t, argToString(map[string]bool{}), "[Unsupported type]")
}
