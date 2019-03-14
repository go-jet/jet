package execution

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/serenize/snaker"
	"reflect"
	"strings"
	"time"
)

func Execute(db *sql.DB, query string, destinationPtr interface{}) error {
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
	rowData := createScanValue(columnTypes)

	scanContext := &scanContext{
		columnNames:      columnNames,
		uniqueObjectsMap: make(map[string]interface{}),
	}

	for rows.Next() {
		err := rows.Scan(rowData...)

		if err != nil {
			return err
		}

		columnProcessed := make([]bool, len(columnTypes))

		if destinationType.Elem().Kind() == reflect.Slice {
			err := mapRowToSlice(scanContext, "", columnProcessed, rowData, destinationPtr)

			if err != nil {
				return err
			}
		} else if destinationType.Elem().Kind() == reflect.Struct {
			return mapRowToStruct(scanContext, "", columnProcessed, rowData, destinationPtr)
		}
	}

	err = rows.Err()

	if err != nil {
		return err
	}

	return nil
}

type scanContext struct {
	columnNames      []string
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

func getGroupKey(scanContext *scanContext, row []interface{}, structType reflect.Type) string {
	structName := structType.Name()
	groupKey := ""

	for i := 0; i < structType.NumField(); i++ {
		fieldType := structType.Field(i)

		////fmt.Println(fieldType.Tag)

		if fieldType.Tag == `sql:"unique"` {
			fieldName := fieldType.Name
			columnName := snaker.CamelToSnake(structName) + "." + snaker.CamelToSnake(fieldName)

			//fmt.Println(fieldName)
			rowIndex := getIndex(scanContext.columnNames, columnName)

			if rowIndex < 0 {
				continue
			}

			rowValue := reflect.ValueOf(row[rowIndex])

			groupKey = groupKey + reflectValueToString(rowValue)
		} else if !isDbBaseType(fieldType.Type) {
			var structType reflect.Type
			if fieldType.Type.Kind() == reflect.Struct {
				structType = fieldType.Type
			} else if fieldType.Type.Kind() == reflect.Ptr && fieldType.Type.Elem().Kind() == reflect.Struct {
				structType = fieldType.Type.Elem()
			} else {
				continue
			}

			//spew.Dump(structType)

			structGroupKey := getGroupKey(scanContext, row, structType)

			//groupKey = strings.Join([]string{structGroupKey, groupKey}, ":")

			groupKey = groupKey + structGroupKey
		}
	}

	//fmt.Println(groupKey)
	return groupKey
}

func getSliceStructType(slicePtr interface{}) reflect.Type {
	sliceTypePtr := reflect.TypeOf(slicePtr)

	elemType := sliceTypePtr.Elem().Elem()

	if elemType.Kind() == reflect.Ptr {
		return elemType.Elem()
	}

	return elemType
}

func mapRowToSlice(scanContext *scanContext, groupKey string, columnProcessed []bool, row []interface{}, destinationPtr interface{}) error {
	if allProcessed(columnProcessed) {
		return nil
	}

	var err error

	structType := getSliceStructType(destinationPtr)

	groupKey = groupKey + ":" + getGroupKey(scanContext, row, structType)

	objPtr, ok := scanContext.uniqueObjectsMap[groupKey]

	if ok {
		err = mapRowToStruct(scanContext, groupKey, columnProcessed, row, objPtr)
		if err != nil {
			return err
		}
	} else {
		destinationStructPtr := newElemForSlice(destinationPtr)

		err = mapRowToStruct(scanContext, groupKey, columnProcessed, row, destinationStructPtr)

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

func mapRowToDestinationValue(scanContext *scanContext, groupKey string, columnProcessed []bool, row []interface{}, dest reflect.Value) error {
	if dest.Kind() == reflect.Struct {
		err := mapRowToStruct(scanContext, groupKey, columnProcessed, row, dest.Addr().Interface())
		if err != nil {
			return err
		}
	} else if dest.Kind() == reflect.Slice {
		err := mapRowToSlice(scanContext, groupKey, columnProcessed, row, dest.Addr().Interface())
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

			err := mapRowToStruct(scanContext, groupKey, columnProcessed, row, structValuePtr.Interface())
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

			err := mapRowToSlice(scanContext, groupKey, columnProcessed, row, sliceValuePtr.Interface())
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

func mapRowToStruct(scanContext *scanContext, groupKey string, columnProcessed []bool, row []interface{}, destination interface{}) error {
	if allProcessed(columnProcessed) {
		return nil
	}

	structType := reflect.TypeOf(destination).Elem()
	structValue := reflect.ValueOf(destination).Elem()
	structName := structType.Name()

	for i := 0; i < structType.NumField(); i++ {
		fieldType := structType.Field(i)
		//fieldTypeName := fieldType.Name
		fieldValue := structValue.Field(i)
		//fmt.Println("---------------", fieldTypeName)
		////spew.Dump(fieldType.Type)

		fieldName := fieldType.Name

		if !isDbBaseType(fieldType.Type) {
			//var fieldValueInterface interface{}
			err := mapRowToDestinationValue(scanContext, groupKey, columnProcessed, row, fieldValue)

			if err != nil {
				return err
			}
		} else {
			columnName := snaker.CamelToSnake(structName) + "." + snaker.CamelToSnake(fieldName)
			//columnName := snaker.CamelToSnake(fieldName)

			////fmt.Println(columnName)
			rowIndex := getIndex(scanContext.columnNames, columnName)

			if rowIndex < 0 || columnProcessed[rowIndex] {
				continue
			}
			////spew.Dump(row[rowIndex])

			rowColumnValue := reflect.ValueOf(row[rowIndex])

			//spew.Dump(rowColumnValue, fieldValue)
			setReflectValue(rowColumnValue, fieldValue)

			columnProcessed[rowIndex] = true
		}
	}

	return nil
}

func reflectValueToString(value reflect.Value) string {
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
	case "string", "int32", "int16", "float64", "time.Time", "bool",
		"*string", "*int32", "*int16", "*float64", "*time.Time", "*bool":
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
			destination.Set(source.Addr())
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
		columnType := getScanType(sqlColumnType)

		columnValue := reflect.New(columnType)

		values[i] = columnValue.Interface()
	}

	return values
}

func getScanType(columnType *sql.ColumnType) reflect.Type {
	scanType := columnType.ScanType()
	//////fmt.Println(scanType.String())
	if scanType.String() != "interface {}" {
		return scanType
	}

	switch columnType.DatabaseTypeName() {
	case "FLOAT4":
		return floatType
	default:
		return stringType
	}
}
