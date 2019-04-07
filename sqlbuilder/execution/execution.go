package execution

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/serenize/snaker"
	"github.com/sub0zero/go-sqlbuilder/types"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func Execute(db types.Db, query string, destinationPtr interface{}) error {
	if db == nil {
		return errors.New("db is nil")
	}

	if destinationPtr == nil {
		return errors.New("Destination is nil ")
	}

	destinationType := reflect.TypeOf(destinationPtr)
	if destinationType.Kind() != reflect.Ptr {
		return errors.New("Destination has to be a pointer to slice or pointer to struct ")
	}

	rows, err := db.Query(query)

	if err != nil {
		return err
	}
	defer rows.Close()

	columnNames, _ := rows.Columns()
	columnTypes, _ := rows.ColumnTypes()

	scanContext := &scanContext{
		row:              createScanValue(columnTypes),
		columnNames:      columnNames,
		uniqueObjectsMap: make(map[string]interface{}),
	}

	//spew.Dump(columnTypes)

	for rows.Next() {
		err := rows.Scan(scanContext.row...)

		if err != nil {
			return err
		}

		scanContext.rowNum++

		if destinationType.Elem().Kind() == reflect.Slice {
			err := mapRowToSlice(scanContext, "", map[string]bool{}, destinationPtr, nil)

			if err != nil {
				return err
			}
		} else if destinationType.Elem().Kind() == reflect.Struct {
			return mapRowToStruct(scanContext, "", map[string]bool{}, destinationPtr, nil)
		}
	}

	err = rows.Err()

	if err != nil {
		return err
	}

	fmt.Println(strconv.Itoa(scanContext.rowNum) + " ROWS PROCESSED")

	return nil
}

type scanContext struct {
	rowNum      int
	columnNames []string

	row              []interface{}
	uniqueObjectsMap map[string]interface{}
}

func getColumnTypeName(columnName string) (string, error) {
	split := strings.Split(columnName, ".")
	if len(split) != 2 {
		return "", errors.New("Invalid column name")
	}

	return split[0], nil
}

func allProcessed(arr []bool) bool {
	for _, b := range arr {
		if !b {
			return false
		}
	}

	return true
}

func getType(reflectType reflect.Type) string {
	var structType reflect.Type
	if reflectType.Kind() == reflect.Struct {
		structType = reflectType
	} else if reflectType.Kind() == reflect.Ptr && reflectType.Elem().Kind() == reflect.Struct {
		structType = reflectType.Elem()
	}

	return structType.Name()
}

func getGroupKey(scanContext *scanContext, typesProcessed map[string]bool, structType reflect.Type, structField *reflect.StructField) string {
	tableName := getTableAlias(structField)

	//fmt.Println("Group: " + tableName)

	if tableName == "" {
		tableName = snaker.CamelToSnake(structType.Name())
	}

	//fmt.Println(tableName)

	if typesProcessed[tableName] {
		return ""
	}

	typesProcessed[tableName] = true

	groupKeys := []string{}

	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)

		////fmt.Println(field.Tag)
		if !isDbBaseType(field.Type) {
			var structType reflect.Type
			if field.Type.Kind() == reflect.Struct {
				structType = field.Type
			} else if field.Type.Kind() == reflect.Ptr && field.Type.Elem().Kind() == reflect.Struct {
				structType = field.Type.Elem()
			} else {
				continue
			}

			//spew.Dump(structType)

			structGroupKey := getGroupKey(scanContext, typesProcessed, structType, &field)

			//groupKey = strings.Join([]string{structGroupKey, groupKey}, ":")

			if structGroupKey != "" {
				groupKeys = append(groupKeys, structGroupKey)
			}
		} else if field.Tag == `sql:"unique"` {
			fieldName := field.Name
			columnName := tableName + "." + snaker.CamelToSnake(fieldName)

			//fmt.Println(fieldName)
			index := getIndex(scanContext.columnNames, columnName)

			if index < 0 {
				continue
			}

			cellValue := cellValue(scanContext.row, index)
			subKey := reflectValueToString(cellValue)

			if subKey != "" {
				groupKeys = append(groupKeys, subKey)
			}
		}
	}

	if len(groupKeys) == 0 {
		return ""
	}

	return "|" + structType.Name() + "(" + strings.Join(groupKeys, ", ") + ")|"
}

func cellValue(row []interface{}, index int) interface{} {
	//spew.Dump(row[index])

	valuer, ok := row[index].(driver.Valuer)

	if !ok {
		//fmt.Println("____________________")
		//spew.Dump(row[index])
		panic("Scan value doesn't implement driver.Valuer")
	}

	//spew.Dump(valuer)

	value, err := valuer.Value()

	if err != nil {
		panic(err)
	}

	//spew.Dump(value)

	return value
}

func getSliceStructType(slicePtr interface{}) reflect.Type {
	sliceTypePtr := reflect.TypeOf(slicePtr)

	elemType := sliceTypePtr.Elem().Elem()

	if elemType.Kind() == reflect.Ptr {
		return elemType.Elem()
	}

	return elemType
}

func cloneProcessedMap(processedMap map[string]bool) map[string]bool {
	newMap := make(map[string]bool, len(processedMap))

	for k, v := range newMap {
		newMap[k] = v
	}

	return newMap
}

func mapRowToSlice(scanContext *scanContext, groupKey string, typesProcessed map[string]bool, destinationPtr interface{}, structField *reflect.StructField) error {
	var err error

	structType := getSliceStructType(destinationPtr)

	structGroupKey := getGroupKey(scanContext, cloneProcessedMap(typesProcessed), structType, structField)

	if structGroupKey == "" {
		structGroupKey = "|ROW: " + strconv.Itoa(scanContext.rowNum) + "|"
	}

	groupKey = groupKey + ":" + structGroupKey

	//fmt.Println(groupKey)

	objPtr, ok := scanContext.uniqueObjectsMap[groupKey]

	if ok {
		err = mapRowToStruct(scanContext, groupKey, typesProcessed, objPtr, structField)
		if err != nil {
			return err
		}
	} else {
		destinationStructPtr := newElemForSlice(destinationPtr)

		err = mapRowToStruct(scanContext, groupKey, typesProcessed, destinationStructPtr, structField)

		if err != nil {
			return err
		}

		elemPtr := appendElemToSlice(destinationPtr, destinationStructPtr)
		scanContext.uniqueObjectsMap[groupKey] = elemPtr
	}

	return err
}

func appendElemToSlice(slice interface{}, objPtr interface{}) interface{} {
	sliceValue := reflect.ValueOf(slice).Elem()
	elemType := sliceValue.Type().Elem()

	if elemType.Kind() == reflect.Ptr {
		sliceValue.Set(reflect.Append(sliceValue, reflect.ValueOf(objPtr)))
		return sliceValue.Index(sliceValue.Len() - 1).Interface()
	}

	sliceValue.Set(reflect.Append(sliceValue, reflect.ValueOf(objPtr).Elem()))

	return sliceValue.Index(sliceValue.Len() - 1).Addr().Interface()
}

func newElemForSlice(destinationSlicePtr interface{}) interface{} {
	destinationSliceType := reflect.TypeOf(destinationSlicePtr).Elem()
	elemType := destinationSliceType.Elem()

	if elemType.Kind() == reflect.Ptr {
		return reflect.New(elemType.Elem()).Interface()
	}

	return reflect.New(elemType).Interface()
}

func mapRowToDestinationValue(scanContext *scanContext, groupKey string, typesProcessed map[string]bool, dest reflect.Value, structField *reflect.StructField) error {
	if dest.Kind() == reflect.Struct {
		err := mapRowToStruct(scanContext, groupKey, typesProcessed, dest.Addr().Interface(), structField)
		if err != nil {
			return err
		}
	} else if dest.Kind() == reflect.Slice {
		err := mapRowToSlice(scanContext, groupKey, typesProcessed, dest.Addr().Interface(), structField)
		if err != nil {
			return err
		}
	} else if dest.Kind() == reflect.Ptr {
		elemType := dest.Type().Elem()

		if elemType.Kind() == reflect.Struct {
			var structValuePtr reflect.Value

			if dest.IsNil() {
				structValuePtr = reflect.New(elemType)
			} else {
				return nil
			}

			err := mapRowToStruct(scanContext, groupKey, typesProcessed, structValuePtr.Interface(), structField)
			if err != nil {
				return err
			}

			if structValuePtr.Elem().Interface() != reflect.New(elemType).Elem().Interface() {
				dest.Set(structValuePtr)
			}

		} else if elemType.Kind() == reflect.Slice {
			var sliceValuePtr reflect.Value

			if dest.IsNil() {
				sliceValuePtr = reflect.New(elemType)
			} else {
				sliceValuePtr = dest
			}

			err := mapRowToSlice(scanContext, groupKey, typesProcessed, sliceValuePtr.Interface(), structField)
			if err != nil {
				return err
			}

			if sliceValuePtr.Elem().Len() > 0 {
				dest.Set(sliceValuePtr)
			}

		} else {
			return errors.New("Unsuported field type: " + dest.Type().Name())
		}
	} else {
		return errors.New("Unsuported field type: " + dest.Type().Name())
	}

	return nil
}

func getTableAlias(structField *reflect.StructField) string {
	if structField == nil {
		return ""
	}

	re := regexp.MustCompile(`sqlbuilder:"(.*?)"`)
	tagMatch := re.FindStringSubmatch(string(structField.Tag))
	if tagMatch != nil && len(tagMatch) == 2 && tagMatch[1] != "" {
		return tagMatch[1]
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

func mapRowToStruct(scanContext *scanContext, groupKey string, typesProcessed map[string]bool, destinationPtr interface{}, structField *reflect.StructField) error {
	structType := reflect.TypeOf(destinationPtr).Elem()
	structValue := reflect.ValueOf(destinationPtr).Elem()

	tableName := getTableAlias(structField)

	if tableName == "" {
		tableName = snaker.CamelToSnake(structType.Name())
	}

	//fmt.Println("map -", tableName)

	if typesProcessed[tableName] {
		//fmt.Println("Already processed")
		return nil
	}

	typesProcessed[tableName] = true

	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)

		fieldValue := structValue.Field(i)
		//fieldTypeName := field.Name
		//fmt.Println("---------------", fieldTypeName,)
		//spew.Dump(field.Type)

		fieldName := field.Name

		if scannerValue, ok := implementsScanner(fieldValue); ok {
			cellValue := getCellValue(scanContext, tableName, fieldName)

			if cellValue == nil {
				continue
			}

			//spew.Dump(scannerValue.Interface())

			if scannerValue.IsNil() {
				initializePtrValue(scannerValue)
			}

			scanner := scannerValue.Interface().(sql.Scanner)

			err := scanner.Scan(cellValue)

			if err != nil {
				return err
			}
		} else if !isDbBaseType(field.Type) {
			//var fieldValueInterface interface{}
			err := mapRowToDestinationValue(scanContext, groupKey, typesProcessed, fieldValue, &field)

			if err != nil {
				return err
			}
		} else {
			cellValue := getCellValue(scanContext, tableName, fieldName)
			//spew.Dump(cellValue)

			//spew.Dump(rowColumnValue, fieldValue)
			if cellValue != nil {
				setReflectValue(reflect.ValueOf(cellValue), fieldValue)
			}
		}
	}

	return nil
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

func initializePtrValue(value reflect.Value) {
	if value.Kind() == reflect.Ptr {
		value.Set(reflect.New(value.Type().Elem()))
	}
}

func getCellValue(scanContext *scanContext, tableName, fieldName string) interface{} {
	columnName := tableName + "." + snaker.CamelToSnake(fieldName)
	//columnName := snaker.CamelToSnake(fieldName)

	////fmt.Println(columnName)
	index := getIndex(scanContext.columnNames, columnName)

	if index < 0 {
		return nil
	}

	return cellValue(scanContext.row, index)
}

func reflectValueToString(val interface{}) string {
	//spew.Dump(val)

	if val == nil {
		return ""
	}

	value := reflect.ValueOf(val)

	//if !value.IsValid()
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

func isDbBaseType(objType reflect.Type) bool {
	//isBaseType := objType == timeType || floatType == objType || stringType == objType || intType == objType
	//isPtrToBaseType := objType.Kind() == reflect.Ptr && (objType.Elem() == timeType || floatType == objType.Elem() ||
	//		stringType == objType.Elem() || intType == objType.Elem())
	typeStr := objType.String()

	switch typeStr {
	case "string", "int32", "int16", "float32", "float64", "time.Time", "bool", "[]byte", "[]uint8",
		"*string", "*int32", "*int16", "*float32", "*float64", "*time.Time", "*bool", "*[]byte", "*[]uint8":
		return true
	}

	//return isBaseType || isPtrToBaseType
	return false
}

func setReflectValue(source, destination reflect.Value) {
	if destination.Kind() == reflect.Ptr {
		if source.Kind() == reflect.Ptr {
			destination.Set(source)
		} else {
			newDestination := reflect.New(destination.Type().Elem())
			newDestination.Elem().Set(source)
			destination.Set(newDestination)
		}
	} else {
		if source.Kind() == reflect.Ptr {
			destination.Set(source.Elem())
		} else {
			destination.Set(source)
		}
	}
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

func newScanType(columnType *sql.ColumnType) reflect.Type {
	//spew.Dump(columnType)
	//fmt.Println(columnType.DatabaseTypeName())
	switch columnType.DatabaseTypeName() {
	case "INT2":
		return nullInt16Type
	case "INT4":
		return nullInt32Type
	case "INT8":
		return nullInt64Type
	case "VARCHAR", "TEXT", "", "_TEXT", "TSVECTOR", "BPCHAR", "BYTEA", "UUID", "JSON", "JSONB":
		return nullStringType
	case "FLOAT4":
		return nullFloatType
	case "FLOAT8", "NUMERIC":
		return nullFloat64Type
	case "BOOL":
		return nullBoolType
	case "DATE", "TIMESTAMP", "TIMESTAMPTZ":
		return nullTimeType
	default:
		panic("Unknown column database type " + columnType.DatabaseTypeName())
	}
}
