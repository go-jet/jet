package is

import "reflect"

// Nil check if v is nil
func Nil(v interface{}) bool {
	return v == nil || (reflect.ValueOf(v).Kind() == reflect.Ptr && reflect.ValueOf(v).IsNil())
}
