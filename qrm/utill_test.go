package qrm

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
	"time"
)

func TestIsSimpleModelType(t *testing.T) {
	require.True(t, isSimpleModelType(reflect.TypeOf(int8(11))))
	require.True(t, isSimpleModelType(reflect.TypeOf(int16(11))))
	require.True(t, isSimpleModelType(reflect.TypeOf(int32(11))))
	require.True(t, isSimpleModelType(reflect.TypeOf(int64(11))))
	require.True(t, isSimpleModelType(reflect.TypeOf(uint8(11))))
	require.True(t, isSimpleModelType(reflect.TypeOf(uint16(11))))
	require.True(t, isSimpleModelType(reflect.TypeOf(uint32(11))))
	require.True(t, isSimpleModelType(reflect.TypeOf(uint64(11))))

	require.True(t, isSimpleModelType(reflect.TypeOf(float32(123.46))))
	require.True(t, isSimpleModelType(reflect.TypeOf(float64(123.46))))

	require.True(t, isSimpleModelType(reflect.TypeOf([]byte("Text"))))
	require.True(t, isSimpleModelType(reflect.TypeOf(time.Now())))
	require.True(t, isSimpleModelType(reflect.TypeOf(uuid.New())))

	complexModelType := struct {
		Field1 string
		Field2 string
	}{}

	require.Equal(t, isSimpleModelType(reflect.TypeOf(complexModelType)), false)
	require.Equal(t, isSimpleModelType(reflect.TypeOf(&complexModelType)), false)
	require.Equal(t, isSimpleModelType(reflect.TypeOf([]string{"str"})), false)
	require.Equal(t, isSimpleModelType(reflect.TypeOf([]int{1, 2})), false)
}

func TestTryAssign(t *testing.T) {
	convertible := int16(16)
	intBool1 := int32(1)
	intBool0 := int32(0)
	intBool2 := int32(2)
	floatStr := "1.11"
	floatErr := "1.abcd2"
	str := "some string"

	destination := struct {
		Convertible int64
		IntBool1    bool
		IntBool0    bool
		IntBool2    bool
		FloatStr    float64
		FloatErr    float64
		Str         string
	}{}

	testValue := reflect.ValueOf(&destination).Elem()

	// convertible
	require.NoError(t, tryAssign(reflect.ValueOf(convertible), testValue.FieldByName("Convertible")))
	require.Equal(t, int64(16), destination.Convertible)

	// 1/0  to bool
	require.NoError(t, tryAssign(reflect.ValueOf(intBool1), testValue.FieldByName("IntBool1")))
	require.Equal(t, true, destination.IntBool1)
	require.NoError(t, tryAssign(reflect.ValueOf(intBool0), testValue.FieldByName("IntBool0")))
	require.Equal(t, false, destination.IntBool0)

	require.EqualError(t, tryAssign(reflect.ValueOf(intBool2), testValue.FieldByName("IntBool2")), "can't assign int32(2) to bool")

	// string to float
	require.NoError(t, tryAssign(reflect.ValueOf(floatStr), testValue.FieldByName("FloatStr")))
	require.Equal(t, 1.11, destination.FloatStr)
	require.EqualError(t, tryAssign(reflect.ValueOf(floatErr), testValue.FieldByName("FloatErr")), "converting driver.Value type string (\"1.abcd2\") to a float64: invalid syntax")
	require.Equal(t, 0.00, destination.FloatErr)

	// string to string
	require.NoError(t, tryAssign(reflect.ValueOf(str), testValue.FieldByName("Str")))
	require.Equal(t, str, destination.Str)
}
