package execution

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/serenize/snaker"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func Query(db Db, query string, args []interface{}, destinationPtr interface{}) error {

	if destinationPtr == nil {
		return errors.New("Destination is nil. ")
	}

	destinationPtrType := reflect.TypeOf(destinationPtr)
	if destinationPtrType.Kind() != reflect.Ptr {
		return errors.New("Destination has to be a pointer to slice or pointer to struct. ")
	}

	if destinationPtrType.Elem().Kind() == reflect.Slice {
		return queryToSlice(db, query, args, destinationPtr)
	} else if destinationPtrType.Elem().Kind() == reflect.Struct {
		tempSlicePtrValue := reflect.New(reflect.SliceOf(destinationPtrType))
		tempSliceValue := tempSlicePtrValue.Elem()

		err := queryToSlice(db, query, args, tempSlicePtrValue.Interface())

		if err != nil {
			return err
		}

		fmt.Println("TEMP SLICE SIZE: ", tempSliceValue.Len())

		if tempSliceValue.Len() == 0 {
			return nil
		}

		structValue := reflect.ValueOf(destinationPtr).Elem()
		firstTempStruct := tempSliceValue.Index(0).Elem()

		if structValue.Type().AssignableTo(firstTempStruct.Type()) {
			structValue.Set(tempSliceValue.Index(0).Elem())
		}
		return nil
	} else {
		return errors.New("Unsupported destination type. ")
	}
}

func queryToSlice(db Db, query string, args []interface{}, slicePtr interface{}) error {
	if db == nil {
		return errors.New("db is nil")
	}

	if slicePtr == nil {
		return errors.New("Destination is nil. ")
	}

	destinationType := reflect.TypeOf(slicePtr)
	if destinationType.Kind() != reflect.Ptr && destinationType.Elem().Kind() != reflect.Slice {
		return errors.New("Destination has to be a pointer to slice. ")
	}

	rows, err := db.Query(query, args...)

	if err != nil {
		return err
	}
	defer rows.Close()

	scanContext, err := newScanContext(rows)

	if err != nil {
		return err
	}

	if len(scanContext.row) == 0 {
		return nil
	}

	groupTime := time.Duration(0)

	for rows.Next() {
		err := rows.Scan(scanContext.row...)

		if err != nil {
			return err
		}

		scanContext.rowNum++

		begin := time.Now()

		_, err = mapRowToSlice(scanContext, "", reflect.ValueOf(slicePtr), nil)

		if err != nil {
			return err
		}

		groupTime += time.Now().Sub(begin)
	}

	fmt.Println(groupTime.String())

	err = rows.Err()

	if err != nil {
		return err
	}

	err = rows.Close()
	if err != nil {
		return err
	}

	fmt.Println(strconv.Itoa(scanContext.rowNum) + " ROW(S) PROCESSED")

	return nil
}

func mapRowToSlice(scanContext *scanContext, groupKey string, slicePtrValue reflect.Value, structField *reflect.StructField) (updated bool, err error) {

	sliceElemType := getSliceElemType(slicePtrValue)

	if isGoBaseType(sliceElemType) {
		index := 0
		if structField != nil {
			columnName := getRefTableNameFrom(structField)
			index = getIndex(scanContext.columnNames, columnName)

			if index < 0 {
				return
			}
		}
		rowElemPtr := scanContext.rowElemValuePtr(index)

		if !rowElemPtr.IsNil() {
			updated = true
			err = appendElemToSlice(slicePtrValue, rowElemPtr)
			if err != nil {
				return
			}
		}

		return
	}

	if sliceElemType.Kind() != reflect.Struct {
		return false, errors.New("Unsupported dest type: " + structField.Name + " " + structField.Type.String())
	}

	structGroupKey := getGroupKey(scanContext, sliceElemType, structField)

	if structGroupKey == "" {
		structGroupKey = "|ROW: " + strconv.Itoa(scanContext.rowNum) + "|"
	}

	groupKey = groupKey + ":" + structGroupKey

	index, ok := scanContext.uniqueObjectsMap[groupKey]

	if ok {
		structPtrValue := getSliceElemPtrAt(slicePtrValue, index)

		return mapRowToStruct(scanContext, groupKey, structPtrValue, structField)
	} else {
		destinationStructPtr := newElemPtrValueForSlice(slicePtrValue)

		updated, err = mapRowToStruct(scanContext, groupKey, destinationStructPtr, structField)

		if err != nil {
			return
		}

		if updated {
			scanContext.uniqueObjectsMap[groupKey] = slicePtrValue.Elem().Len()
			err = appendElemToSlice(slicePtrValue, destinationStructPtr)

			if err != nil {
				return
			}
		}
	}

	return
}

func getGroupKey(scanContext *scanContext, structType reflect.Type, structField *reflect.StructField) string {
	tableName := getRefTableNameFrom(structField)

	if tableName == "" {
		tableName = snaker.CamelToSnake(structType.Name())
	}

	groupKeys := []string{}

	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)

		if !isGoBaseType(field.Type) {
			var structType reflect.Type
			if field.Type.Kind() == reflect.Struct {
				structType = field.Type
			} else if field.Type.Kind() == reflect.Ptr && field.Type.Elem().Kind() == reflect.Struct {
				structType = field.Type.Elem()
			} else {
				continue
			}

			structGroupKey := getGroupKey(scanContext, structType, &field)

			if structGroupKey != "" {
				groupKeys = append(groupKeys, structGroupKey)
			}
		} else if field.Tag == `sql:"unique"` {
			fieldName := field.Name
			columnName := tableName + "." + snaker.CamelToSnake(fieldName)

			index := getIndex(scanContext.columnNames, columnName)

			if index < 0 {
				continue
			}

			cellValue := scanContext.rowElem(index)
			subKey := valueToString(cellValue)

			if subKey != "" {
				groupKeys = append(groupKeys, subKey)
			}
		}
	}

	if len(groupKeys) == 0 {
		return ""
	}

	groupKey := "{" + structType.Name() + "(" + strings.Join(groupKeys, ",") + ")}"

	return groupKey
}

func getSliceElemType(slicePtrValue reflect.Value) reflect.Type {
	sliceTypePtr := slicePtrValue.Type()

	elemType := sliceTypePtr.Elem().Elem()

	if elemType.Kind() == reflect.Ptr {
		return elemType.Elem()
	}

	return elemType
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
	if slicePtrValue.IsNil() {
		panic("Slice is nil")
	}
	sliceValue := slicePtrValue.Elem()
	sliceElemType := sliceValue.Type().Elem()

	newElemValue := objPtrValue

	if sliceElemType.Kind() != reflect.Ptr {
		newElemValue = objPtrValue.Elem()
	}

	if !newElemValue.Type().AssignableTo(sliceElemType) {
		return fmt.Errorf("Scan: can't append %s to %s slice ", newElemValue.Type().String(), sliceValue.Type().String())
	}

	sliceValue.Set(reflect.Append(sliceValue, newElemValue))

	return nil
}

func newElemPtrValueForSlice(slicePtrValue reflect.Value) reflect.Value {
	destinationSliceType := slicePtrValue.Type().Elem()
	elemType := destinationSliceType.Elem()

	if elemType.Kind() == reflect.Ptr {
		return reflect.New(elemType.Elem())
	}

	return reflect.New(elemType)
}

func mapRowToDestinationPtr(scanContext *scanContext, groupKey string, destPtrValue reflect.Value, structField *reflect.StructField) (updated bool, err error) {

	if destPtrValue.Kind() != reflect.Ptr {
		return false, errors.New("Internal error. ")
	}

	destValueKind := destPtrValue.Elem().Kind()

	if destValueKind == reflect.Struct {
		return mapRowToStruct(scanContext, groupKey, destPtrValue, structField)
	} else if destValueKind == reflect.Slice {
		return mapRowToSlice(scanContext, groupKey, destPtrValue, structField)
	} else {
		return false, errors.New("Unsupported dest type: " + structField.Name + " " + structField.Type.String())
	}
}

func mapRowToDestinationValue(scanContext *scanContext, groupKey string, dest reflect.Value, structField *reflect.StructField) (updated bool, err error) {

	var destPtrValue reflect.Value

	if dest.Kind() != reflect.Ptr {
		destPtrValue = dest.Addr()
	} else if dest.Kind() == reflect.Ptr {
		if dest.IsNil() {
			destPtrValue = reflect.New(dest.Type().Elem())
		} else {
			destPtrValue = dest
		}
	} else {
		return false, errors.New("Internal error. ")
	}

	updated, err = mapRowToDestinationPtr(scanContext, groupKey, destPtrValue, structField)

	if err != nil {
		return
	}

	if dest.Kind() == reflect.Ptr && dest.IsNil() && updated {
		dest.Set(destPtrValue)
	}

	return
}

func getRefTableNameFrom(structField *reflect.StructField) string {
	if structField == nil {
		return ""
	}

	tagOverwriteName := structField.Tag.Get("sqlbuilder")

	if tagOverwriteName != "" {
		return tagOverwriteName
	}

	if !structField.Anonymous {
		return snaker.CamelToSnake(structField.Name)
	}

	var elemType string

	if structField.Type.Kind() == reflect.Ptr {
		elem := structField.Type.Elem()
		if elem.Kind() == reflect.Struct {
			elemType = elem.Name()
		} else if elem.Kind() == reflect.Slice {
			elemType = elem.Elem().Name()
		}
	} else {
		if structField.Type.Kind() == reflect.Struct {
			elemType = structField.Type.Name()
		} else {
			sliceElem := structField.Type.Elem()
			if sliceElem.Kind() == reflect.Ptr {
				elemType = sliceElem.Elem().Name()
			} else {
				elemType = sliceElem.Name()
			}
		}
	}

	return snaker.CamelToSnake(elemType)
}

func mapRowToStruct(scanContext *scanContext, groupKey string, structPtrValue reflect.Value, structField *reflect.StructField) (updated bool, err error) {
	structType := structPtrValue.Type().Elem()
	structValue := structPtrValue.Elem()

	tableName := getRefTableNameFrom(structField)

	if tableName == "" {
		tableName = snaker.CamelToSnake(structType.Name())
	}

	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)

		fieldValue := structValue.Field(i)
		fieldName := field.Name

		if scannerValue, ok := implementsScanner(fieldValue); ok {
			cellValue := getCellValue(scanContext, tableName, fieldName)

			if cellValue == nil {
				continue
			}

			initializeValueIfNil(fieldValue)

			scanner := scannerValue.Interface().(sql.Scanner)

			err = scanner.Scan(cellValue)

			if err != nil {
				err = fmt.Errorf("%s, at struct field: %s %s of type %s. ", err.Error(), field.Name, field.Type.String(), structType.String())
				return
			}
			updated = true
		} else if isGoBaseType(field.Type) {
			cellValue := getCellValue(scanContext, tableName, fieldName)
			//spew.Dump(rowElem)

			//spew.Dump(rowColumnValue, fieldValue)
			if cellValue != nil {
				updated = true
				initializeValueIfNil(fieldValue)
				err = setReflectValue(reflect.ValueOf(cellValue), fieldValue)

				if err != nil {
					err = fmt.Errorf("Scan: %s, at struct field: %s %s of type %s. ", err.Error(), field.Name, field.Type.String(), structType.String())
					return
				}
			}
		} else {
			var changed bool
			changed, err = mapRowToDestinationValue(scanContext, groupKey, fieldValue, &field)

			if err != nil {
				return
			}

			if changed {
				updated = true
			}
		}
	}

	return
}

func initializeValueIfNil(value reflect.Value) {
	if !value.IsValid() || !value.CanSet() {
		return
	}

	if value.Type().Kind() == reflect.Slice && value.IsNil() {
		value.Set(reflect.New(value.Type()).Elem())
	} else if value.Kind() == reflect.Ptr && value.IsNil() {
		value.Set(reflect.New(value.Type().Elem()))
	}
}

func implementsScanner(value reflect.Value) (reflect.Value, bool) {
	if _, ok := value.Interface().(sql.Scanner); ok {
		return value, true
	} else if value.CanAddr() {
		if _, ok := value.Addr().Interface().(sql.Scanner); ok {
			return value.Addr(), true
		}
	}

	return value, false
}

func getCellValue(scanContext *scanContext, tableName, fieldName string) interface{} {
	columnName := ""

	if tableName == "" {
		columnName = snaker.CamelToSnake(fieldName)
	} else {
		columnName = tableName + "." + snaker.CamelToSnake(fieldName)
	}

	index := getIndex(scanContext.columnNames, columnName)

	if index < 0 {
		return nil
	}

	return scanContext.rowElem(index)
}

func valueToString(val interface{}) string {
	if val == nil {
		return ""
	}

	value := reflect.ValueOf(val)

	var valueInterface interface{}
	if value.Kind() == reflect.Ptr {
		valueInterface = value.Elem().Interface()
	} else {
		valueInterface = value.Interface()
	}

	if t, ok := valueInterface.(time.Time); ok {
		return t.String()
	}

	return fmt.Sprintf("%#v", valueInterface)
}

var timeType = reflect.TypeOf(time.Now())
var floatType = reflect.TypeOf(1.0)
var stringType = reflect.TypeOf("str")
var intType = reflect.TypeOf(1)

func isGoBaseType(objType reflect.Type) bool {
	typeStr := objType.String()

	switch typeStr {
	case "string", "int", "int16", "int32", "int64", "float32", "float64", "time.Time", "bool", "[]byte", "[]uint8",
		"*string", "*int", "*int16", "*int32", "*int64", "*float32", "*float64", "*time.Time", "*bool", "*[]byte", "*[]uint8":
		return true
	}

	return false
}

func setReflectValue(source, destination reflect.Value) error {
	var sourceElem reflect.Value
	if destination.Kind() == reflect.Ptr {
		if source.Kind() == reflect.Ptr {
			sourceElem = source
		} else {
			if source.CanAddr() {
				sourceElem = source.Addr()
			} else {
				sourceCopy := reflect.New(source.Type())
				sourceCopy.Elem().Set(source)

				sourceElem = sourceCopy
			}
		}
	} else {
		if source.Kind() == reflect.Ptr {
			sourceElem = source.Elem()
		} else {
			sourceElem = source
		}
	}

	if !sourceElem.Type().AssignableTo(destination.Type()) {
		return errors.New("can't set " + sourceElem.Type().String() + " to " + destination.Type().String())
	}

	destination.Set(sourceElem)

	return nil
}

func getIndex(list []string, text string) int {
	for i, str := range list {
		if str == text {
			return i
		}
	}

	return -1
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

var nullFloatType = reflect.TypeOf(NullFloat32{})
var nullFloat64Type = reflect.TypeOf(sql.NullFloat64{})
var nullInt16Type = reflect.TypeOf(NullInt16{})
var nullInt32Type = reflect.TypeOf(NullInt32{})
var nullInt64Type = reflect.TypeOf(sql.NullInt64{})
var nullStringType = reflect.TypeOf(sql.NullString{})
var nullBoolType = reflect.TypeOf(sql.NullBool{})
var nullTimeType = reflect.TypeOf(NullTime{})
var nullByteArrayType = reflect.TypeOf(NullByteArray{})

func newScanType(columnType *sql.ColumnType) reflect.Type {
	switch columnType.DatabaseTypeName() {
	case "INT2":
		return nullInt16Type
	case "INT4":
		return nullInt32Type
	case "INT8":
		return nullInt64Type
	case "VARCHAR", "TEXT", "", "_TEXT", "TSVECTOR", "BPCHAR", "UUID", "JSON", "JSONB", "INTERVAL", "POINT", "BIT", "VARBIT", "XML":
		return nullStringType
	case "FLOAT4":
		return nullFloatType
	case "FLOAT8", "NUMERIC", "DECIMAL":
		return nullFloat64Type
	case "BOOL":
		return nullBoolType
	case "BYTEA":
		return nullByteArrayType
	case "DATE", "TIMESTAMP", "TIMESTAMPTZ", "TIME", "TIMETZ":
		return nullTimeType
	default:
		fmt.Println("Unknown column database type " + columnType.DatabaseTypeName() + " using string as default.")
		return nullStringType
	}
}

type scanContext struct {
	rowNum      int
	columnNames []string

	row              []interface{}
	uniqueObjectsMap map[string]int
	groupKeyMap      map[string]string
}

func newScanContext(rows *sql.Rows) (*scanContext, error) {
	columnNames, err := rows.Columns()

	if err != nil {
		return nil, err
	}

	columnTypes, err := rows.ColumnTypes()

	if err != nil {
		return nil, err
	}

	return &scanContext{
		row:              createScanValue(columnTypes),
		columnNames:      columnNames,
		uniqueObjectsMap: make(map[string]int),
		groupKeyMap:      make(map[string]string),
	}, nil
}

func (s *scanContext) rowElem(index int) interface{} {

	valuer, ok := s.row[index].(driver.Valuer)

	if !ok {
		panic("Scan value doesn't implement driver.Valuer")
	}

	value, err := valuer.Value()

	if err != nil {
		panic(err)
	}

	return value
}

func (s *scanContext) rowElemValuePtr(index int) reflect.Value {
	rowElem := s.rowElem(index)
	rowElemValue := reflect.ValueOf(rowElem)

	if rowElemValue.Kind() == reflect.Ptr {
		return rowElemValue
	}

	if rowElemValue.CanAddr() {
		return rowElemValue.Addr()
	}

	newElem := reflect.New(rowElemValue.Type())
	newElem.Elem().Set(rowElemValue)
	return newElem
}
