package qrm

import (
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/v2/internal/utils"
	"github.com/go-jet/jet/v2/qrm/internal"
	"github.com/google/uuid"
	"reflect"
	"strconv"
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

	newElemValue := objPtrValue

	if sliceElemType.Kind() != reflect.Ptr {
		newElemValue = objPtrValue.Elem()
	}

	if newElemValue.Type().ConvertibleTo(sliceElemType) {
		newElemValue = newElemValue.Convert(sliceElemType)
	}

	if !newElemValue.Type().AssignableTo(sliceElemType) {
		panic("jet: can't append " + newElemValue.Type().String() + " to " + sliceValue.Type().String() + " slice")
	}

	sliceValue.Set(reflect.Append(sliceValue, newElemValue))

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

func isIntegerType(value reflect.Type) bool {
	switch value {
	case int8Type, unit8Type, int16Type, uint16Type,
		int32Type, uint32Type, int64Type, uint64Type:
		return true
	}

	return false
}

func isNumber(valueType reflect.Type) bool {
	return isIntegerType(valueType) || valueType == float64Type || valueType == float32Type
}

func tryAssign(source, destination reflect.Value) bool {

	switch {
	case source.Type().ConvertibleTo(destination.Type()):
		source = source.Convert(destination.Type())
	case isIntegerType(source.Type()) && destination.Type() == boolType:
		intValue := source.Int()

		if intValue == 1 {
			source = reflect.ValueOf(true)
		} else if intValue == 0 {
			source = reflect.ValueOf(false)
		}
	case source.Type() == stringType && isNumber(destination.Type()):
		// if source is string and destination is a number(int8, int32, float32, ...), we first parse string to float64 number
		// and then parsed number is converted into destination type
		f, err := strconv.ParseFloat(source.String(), 64)
		if err != nil {
			return false
		}
		source = reflect.ValueOf(f)

		if source.Type().ConvertibleTo(destination.Type()) {
			source = source.Convert(destination.Type())
		}
	}

	if source.Type().AssignableTo(destination.Type()) {
		destination.Set(source)
		return true
	}

	return false
}

func setReflectValue(source, destination reflect.Value) {

	if tryAssign(source, destination) {
		return
	}

	if destination.Kind() == reflect.Ptr {
		if source.Kind() == reflect.Ptr {
			if !source.IsNil() {
				if destination.IsNil() {
					initializeValueIfNilPtr(destination)
				}

				if tryAssign(source.Elem(), destination.Elem()) {
					return
				}
			} else {
				return
			}
		} else {
			if source.CanAddr() {
				source = source.Addr()
			} else {
				sourceCopy := reflect.New(source.Type())
				sourceCopy.Elem().Set(source)

				source = sourceCopy
			}

			if tryAssign(source, destination) {
				return
			}

			if tryAssign(source.Elem(), destination.Elem()) {
				return
			}
		}
	} else {
		if source.Kind() == reflect.Ptr {
			if source.IsNil() {
				return
			}
			source = source.Elem()
		}

		if tryAssign(source, destination) {
			return
		}
	}

	panic("jet: can't set " + source.Type().String() + " to " + destination.Type().String())
}

func createScanValue(columnTypes []*sql.ColumnType) []interface{} {
	values := make([]interface{}, len(columnTypes))

	for i, sqlColumnType := range columnTypes {
		columnType := newScanType(sqlColumnType)

		columnValue := reflect.New(columnType)

		values[i] = columnValue.Interface()
	}

	return values
}

var boolType = reflect.TypeOf(true)
var int8Type = reflect.TypeOf(int8(1))
var unit8Type = reflect.TypeOf(uint8(1))
var int16Type = reflect.TypeOf(int16(1))
var uint16Type = reflect.TypeOf(uint16(1))
var int32Type = reflect.TypeOf(int32(1))
var uint32Type = reflect.TypeOf(uint32(1))
var int64Type = reflect.TypeOf(int64(1))
var uint64Type = reflect.TypeOf(uint64(1))
var float32Type = reflect.TypeOf(float32(1))
var float64Type = reflect.TypeOf(float64(1))
var stringType = reflect.TypeOf("")

var nullBoolType = reflect.TypeOf(sql.NullBool{})
var nullInt8Type = reflect.TypeOf(internal.NullInt8{})
var nullInt16Type = reflect.TypeOf(internal.NullInt16{})
var nullInt32Type = reflect.TypeOf(internal.NullInt32{})
var nullInt64Type = reflect.TypeOf(sql.NullInt64{})
var nullFloat32Type = reflect.TypeOf(internal.NullFloat32{})
var nullFloat64Type = reflect.TypeOf(sql.NullFloat64{})
var nullStringType = reflect.TypeOf(sql.NullString{})
var nullTimeType = reflect.TypeOf(internal.NullTime{})
var nullByteArrayType = reflect.TypeOf(internal.NullByteArray{})

func newScanType(columnType *sql.ColumnType) reflect.Type {

	switch columnType.DatabaseTypeName() {
	case "TINYINT":
		return nullInt8Type
	case "INT2", "SMALLINT", "YEAR":
		return nullInt16Type
	case "INT4", "MEDIUMINT", "INT":
		return nullInt32Type
	case "INT8", "BIGINT":
		return nullInt64Type
	case "CHAR", "VARCHAR", "TEXT", "", "_TEXT", "TSVECTOR", "BPCHAR", "UUID", "JSON", "JSONB", "INTERVAL", "POINT", "BIT", "VARBIT", "XML":
		return nullStringType
	case "FLOAT4":
		return nullFloat32Type
	case "FLOAT8", "FLOAT", "DOUBLE":
		return nullFloat64Type
	case "BOOL":
		return nullBoolType
	case "BYTEA", "BINARY", "VARBINARY", "BLOB":
		return nullByteArrayType
	case "DATE", "DATETIME", "TIMESTAMP", "TIMESTAMPTZ", "TIME", "TIMETZ":
		return nullTimeType
	default:
		return nullStringType
	}
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
