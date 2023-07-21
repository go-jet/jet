package must

import (
	"github.com/go-jet/jet/v2/internal/utils/is"
	"reflect"
)

// BeTrue panics when condition is false
func BeTrue(condition bool, errorStr string) {
	if !condition {
		panic(errorStr)
	}
}

// BeTypeKind panics with errorStr error, if v interface is not of reflect kind
func BeTypeKind(v interface{}, kind reflect.Kind, errorStr string) {
	if reflect.TypeOf(v).Kind() != kind {
		panic(errorStr)
	}
}

// ValueBeOfTypeKind panics with errorStr error, if v value is not of reflect kind
func ValueBeOfTypeKind(v reflect.Value, kind reflect.Kind, errorStr string) {
	if v.Kind() != kind {
		panic(errorStr)
	}
}

// TypeBeOfKind panics with errorStr error, if v type is not of reflect kind
func TypeBeOfKind(v reflect.Type, kind reflect.Kind, errorStr string) {
	if v.Kind() != kind {
		panic(errorStr)
	}
}

// BeInitializedPtr panics with errorStr if val interface is nil
func BeInitializedPtr(val interface{}, errorStr string) {
	if is.Nil(val) {
		panic(errorStr)
	}
}
