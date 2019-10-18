package qrm

import (
	"github.com/google/uuid"
	"gotest.tools/assert"
	"reflect"
	"testing"
	"time"
)

func TestIsSimpleModelType(t *testing.T) {
	assert.Assert(t, isSimpleModelType(reflect.TypeOf(int8(11))))
	assert.Assert(t, isSimpleModelType(reflect.TypeOf(int16(11))))
	assert.Assert(t, isSimpleModelType(reflect.TypeOf(int32(11))))
	assert.Assert(t, isSimpleModelType(reflect.TypeOf(int64(11))))
	assert.Assert(t, isSimpleModelType(reflect.TypeOf(uint8(11))))
	assert.Assert(t, isSimpleModelType(reflect.TypeOf(uint16(11))))
	assert.Assert(t, isSimpleModelType(reflect.TypeOf(uint32(11))))
	assert.Assert(t, isSimpleModelType(reflect.TypeOf(uint64(11))))

	assert.Assert(t, isSimpleModelType(reflect.TypeOf(float32(123.46))))
	assert.Assert(t, isSimpleModelType(reflect.TypeOf(float64(123.46))))

	assert.Assert(t, isSimpleModelType(reflect.TypeOf([]byte("Text"))))
	assert.Assert(t, isSimpleModelType(reflect.TypeOf(time.Now())))
	assert.Assert(t, isSimpleModelType(reflect.TypeOf(uuid.New())))

	complexModelType := struct {
		Field1 string
		Field2 string
	}{}

	assert.Equal(t, isSimpleModelType(reflect.TypeOf(complexModelType)), false)
	assert.Equal(t, isSimpleModelType(reflect.TypeOf(&complexModelType)), false)
}
