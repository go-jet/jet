package qrm

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

func TestIsSimpleModelType(t *testing.T) {
	assert.True(t, isSimpleModelType(reflect.TypeOf(int8(11))))
	assert.True(t, isSimpleModelType(reflect.TypeOf(int16(11))))
	assert.True(t, isSimpleModelType(reflect.TypeOf(int32(11))))
	assert.True(t, isSimpleModelType(reflect.TypeOf(int64(11))))
	assert.True(t, isSimpleModelType(reflect.TypeOf(uint8(11))))
	assert.True(t, isSimpleModelType(reflect.TypeOf(uint16(11))))
	assert.True(t, isSimpleModelType(reflect.TypeOf(uint32(11))))
	assert.True(t, isSimpleModelType(reflect.TypeOf(uint64(11))))

	assert.True(t, isSimpleModelType(reflect.TypeOf(float32(123.46))))
	assert.True(t, isSimpleModelType(reflect.TypeOf(float64(123.46))))

	assert.True(t, isSimpleModelType(reflect.TypeOf([]byte("Text"))))
	assert.True(t, isSimpleModelType(reflect.TypeOf(time.Now())))
	assert.True(t, isSimpleModelType(reflect.TypeOf(uuid.New())))

	complexModelType := struct {
		Field1 string
		Field2 string
	}{}

	assert.Equal(t, isSimpleModelType(reflect.TypeOf(complexModelType)), false)
	assert.Equal(t, isSimpleModelType(reflect.TypeOf(&complexModelType)), false)
	assert.Equal(t, isSimpleModelType(reflect.TypeOf([]string{"str"})), false)
	assert.Equal(t, isSimpleModelType(reflect.TypeOf([]int{1, 2})), false)
}
