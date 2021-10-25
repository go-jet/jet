package qrm

import (
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/v2/internal/utils"
	"github.com/go-jet/jet/v2/qrm/internal"
	"github.com/google/uuid"
	"reflect"
	"strings"
	"time"
)

var scannerInterfaceType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

func implementsScannerType(fieldType reflect.Type) bool {
	if fieldType.Implements(scannerInterfaceType) {
		return true
	}

	typePtr := reflect.New(fieldType).Type()

	return typePtr.Implements(scannerInterfaceType)
}

func getScanner(value reflect.Value) sql.Scanner {
	if scanner, ok := value.Interface().(sql.Scanner); ok {
		return scanner
	}

	return value.Addr().Interface().(sql.Scanner)
}

func getSliceElemType(slicePtrValue reflect.Value) reflect.Type {
	sliceTypePtr := slicePtrValue.Type()
	elemType := indirectType(sliceTypePtr).Elem()

	return indirectType(elemType)
}

func getSliceElemPtrAt(slicePtrValue reflect.Value, index int) reflect.Value {
	sliceValue := slicePtrValue.Elem()
	elem := sliceValue.Index(index)

	if elem.Kind() == reflect.Ptr {
		return elem
	}

	return elem.Addr()
}

func appendElemToSlice(slicePtrValue reflect.Value, objPtrValue reflect.Value) error {
	utils.MustBeTrue(!slicePtrValue.IsNil(), "jet: internal, slice is nil")

	sliceValue := slicePtrValue.Elem()
	sliceElemType := sliceValue.Type().Elem()

	var newSliceElemValue reflect.Value

	if objPtrValue.Type().AssignableTo(sliceElemType) {
		newSliceElemValue = objPtrValue
	} else if objPtrValue.Elem().Type().AssignableTo(sliceElemType) {
		newSliceElemValue = objPtrValue.Elem()
	} else {
		newSliceElemValue = reflect.New(sliceElemType).Elem()

		var err error

		if newSliceElemValue.Kind() == reflect.Ptr {
			newSliceElemValue.Set(reflect.New(newSliceElemValue.Type().Elem()))
			err = tryAssign(objPtrValue.Elem(), newSliceElemValue.Elem())
		} else {
			err = tryAssign(objPtrValue.Elem(), newSliceElemValue)
		}

		if err != nil {
			return fmt.Errorf("can't append %T to %T slice: %w", objPtrValue.Elem().Interface(), sliceValue.Interface(), err)
		}
	}

	sliceValue.Set(reflect.Append(sliceValue, newSliceElemValue))

	return nil
}

func newElemPtrValueForSlice(slicePtrValue reflect.Value) reflect.Value {
	destinationSliceType := slicePtrValue.Type().Elem()
	elemType := indirectType(destinationSliceType.Elem())

	return reflect.New(elemType)
}

func getTypeName(structType reflect.Type, parentField *reflect.StructField) string {
	if parentField == nil {
		return structType.Name()
	}

	aliasTag := parentField.Tag.Get("alias")

	if aliasTag == "" {
		return structType.Name()
	}

	aliasParts := strings.Split(aliasTag, ".")

	return toCommonIdentifier(aliasParts[0])
}

func getTypeAndFieldName(structType string, field reflect.StructField) (string, string) {
	aliasTag := field.Tag.Get("alias")

	if aliasTag == "" {
		return structType, field.Name
	}

	aliasParts := strings.Split(aliasTag, ".")

	if len(aliasParts) == 1 {
		return structType, toCommonIdentifier(aliasParts[0])
	}

	return toCommonIdentifier(aliasParts[0]), toCommonIdentifier(aliasParts[1])
}

var replacer = strings.NewReplacer(" ", "", "-", "", "_", "")

func toCommonIdentifier(name string) string {
	return strings.ToLower(replacer.Replace(name))
}

func initializeValueIfNilPtr(value reflect.Value) {
	if !value.IsValid() || !value.CanSet() {
		return
	}

	if value.Kind() == reflect.Ptr && value.IsNil() {
		value.Set(reflect.New(value.Type().Elem()))
	}
}

func valueToString(value reflect.Value) string {

	if !value.IsValid() {
		return "nil"
	}

	var valueInterface interface{}
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return "nil"
		}
		valueInterface = value.Elem().Interface()
	} else {
		valueInterface = value.Interface()
	}

	if t, ok := valueInterface.(fmt.Stringer); ok {
		return t.String()
	}

	return fmt.Sprintf("%#v", valueInterface)
}

var timeType = reflect.TypeOf(time.Now())
var uuidType = reflect.TypeOf(uuid.New())
var byteArrayType = reflect.TypeOf([]byte(""))

func isSimpleModelType(objType reflect.Type) bool {
	objType = indirectType(objType)

	switch objType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String,
		reflect.Bool:
		return true
	}

	return objType == timeType || objType == uuidType || objType == byteArrayType
}

func isIntegerType(objType reflect.Type) bool {
	objType = indirectType(objType)

	switch objType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	}

	return false
}

func isFloatType(value reflect.Type) bool {
	switch value.Kind() {
	case reflect.Float32, reflect.Float64:
		return true
	}

	return false
}

func tryAssign(source, destination reflect.Value) error {

	if source.Type() != destination.Type() &&
		!isFloatType(destination.Type()) && // to preserve precision during conversion
		!(isIntegerType(source.Type()) && destination.Kind() == reflect.String) && // default conversion will convert int to 1 rune string
		source.Type().ConvertibleTo(destination.Type()) {

		source = source.Convert(destination.Type())
	}

	if source.Type().AssignableTo(destination.Type()) {
		switch b := source.Interface().(type) {
		case []byte:
			destination.SetBytes(cloneBytes(b))
		default:
			destination.Set(source)
		}
		return nil
	}

	sourceInterface := source.Interface()

	switch destination.Interface().(type) {
	case bool:
		var nullBool internal.NullBool

		err := nullBool.Scan(sourceInterface)

		if err != nil {
			return err
		}

		destination.SetBool(nullBool.Bool)

	case float32, float64:
		var nullFloat sql.NullFloat64

		err := nullFloat.Scan(sourceInterface)
		if err != nil {
			return err
		}

		if nullFloat.Valid {
			destination.SetFloat(nullFloat.Float64)
		}
	case int, int8, int16, int32, int64:
		var integer sql.NullInt64

		err := integer.Scan(sourceInterface)
		if err != nil {
			return err
		}

		if integer.Valid {
			destination.SetInt(integer.Int64)
		}

	case uint, uint8, uint16, uint32, uint64:
		var uInt internal.NullUInt64

		err := uInt.Scan(sourceInterface)

		if err != nil {
			return err
		}

		if uInt.Valid {
			destination.SetUint(uInt.UInt64)
		}

	case string:
		var str sql.NullString

		err := str.Scan(sourceInterface)
		if err != nil {
			return err
		}

		if str.Valid {
			destination.SetString(str.String)
		}

	case time.Time:
		var nullTime internal.NullTime

		err := nullTime.Scan(sourceInterface)
		if err != nil {
			return err
		}

		if nullTime.Valid {
			destination.Set(reflect.ValueOf(nullTime.Time))
		}

	default:
		return fmt.Errorf("can't assign %T to %T", sourceInterface, destination.Interface())
	}

	return nil
}

func setReflectValue(source, destination reflect.Value) error {

	if destination.Kind() == reflect.Ptr {
		if destination.IsNil() {
			initializeValueIfNilPtr(destination)
		}

		if source.Kind() == reflect.Ptr {
			if source.IsNil() {
				return nil // source is nil, destination should keep its zero value
			}
			source = source.Elem()
		}

		if err := tryAssign(source, destination.Elem()); err != nil {
			return err
		}

	} else {
		if source.Kind() == reflect.Ptr {
			if source.IsNil() {
				return nil // source is nil, destination should keep its zero value
			}
			source = source.Elem()
		}

		if err := tryAssign(source, destination); err != nil {
			return err
		}
	}

	return nil
}

func isPrimaryKey(field reflect.StructField, primaryKeyOverwrites []string) bool {
	if len(primaryKeyOverwrites) > 0 {
		return utils.StringSliceContains(primaryKeyOverwrites, field.Name)
	}

	sqlTag := field.Tag.Get("sql")

	return sqlTag == "primary_key"
}

func parentFieldPrimaryKeyOverwrite(parentField *reflect.StructField) []string {
	if parentField == nil {
		return nil
	}

	sqlTag := parentField.Tag.Get("sql")

	if !strings.HasPrefix(sqlTag, "primary_key") {
		return nil
	}

	parts := strings.Split(sqlTag, "=")

	if len(parts) < 2 {
		return nil
	}

	return strings.Split(parts[1], ",")
}

func indirectType(reflectType reflect.Type) reflect.Type {
	if reflectType.Kind() != reflect.Ptr {
		return reflectType
	}
	return reflectType.Elem()
}

func fieldToString(field *reflect.StructField) string {
	if field == nil {
		return ""
	}

	return " at '" + field.Name + " " + field.Type.String() + "'"
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
