package execution

import (
	"database/sql"
	"errors"
	"github.com/serenize/snaker"
	"reflect"
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
	values := createScanValue(columnTypes)
	//
	//spew.Dump(columnTypes)
	//spew.Dump(values)

	for rows.Next() {
		err := rows.Scan(values...)

		if err != nil {
			return err
		}

		if destinationType.Elem().Kind() == reflect.Slice {

			destinationStructPtr := newElemForSlice(destinationPtr)

			err = mapValuesToStruct(columnNames, values, destinationStructPtr)

			if err != nil {
				return err
			}

			appendElemToSlice(destinationPtr, destinationStructPtr)
		} else if destinationType.Elem().Kind() == reflect.Struct {
			return mapValuesToStruct(columnNames, values, destinationPtr)
		}
	}

	err = rows.Err()

	if err != nil {
		return err
	}

	return nil
}

func appendElemToSlice(slice interface{}, obj interface{}) {
	//spew.Dump(slice)
	sliceValue := reflect.ValueOf(slice).Elem()

	sliceValue.Set(reflect.Append(sliceValue, reflect.ValueOf(obj).Elem()))
}

func newElemForSlice(destinationSlicePtr interface{}) interface{} {
	destinationSliceType := reflect.TypeOf(destinationSlicePtr).Elem()

	return reflect.New(destinationSliceType.Elem()).Interface()
}

func mapValuesToStruct(columnNames []string, row []interface{}, destination interface{}) error {
	structType := reflect.TypeOf(destination).Elem()
	structValue := reflect.ValueOf(destination).Elem()
	structName := structType.Name()

	for i := 0; i < structType.NumField(); i++ {
		fieldType := structType.Field(i)
		//fieldTypeName := fieldType.Name
		fieldValue := structValue.Field(i)
		//fmt.Println("---------------", fieldTypeName)
		//spew.Dump(fieldType.Type)

		if !isDbBaseType(fieldType.Type) {
			if fieldType.Type.Kind() == reflect.Struct {
				err := mapValuesToStruct(columnNames, row, fieldValue.Addr().Interface())
				if err != nil {
					return err
				}
			} else if fieldType.Type.Kind() == reflect.Ptr {
				newStructValue := reflect.New(fieldType.Type.Elem())
				err := mapValuesToStruct(columnNames, row, newStructValue.Interface())
				if err != nil {
					return err
				}

				if newStructValue.Elem().Interface() != reflect.New(fieldType.Type.Elem()).Elem().Interface() {
					fieldValue.Set(newStructValue)
				}
			}
		} else {
			fieldName := fieldType.Name

			columnName := snaker.CamelToSnake(structName) + "." + snaker.CamelToSnake(fieldName)
			//columnName := snaker.CamelToSnake(fieldName)

			//fmt.Println(columnName)
			rowIndex := getIndex(columnNames, columnName)

			if rowIndex < 0 {
				continue
			}

			//spew.Dump(row[rowIndex])

			rowColumnValue := reflect.ValueOf(row[rowIndex])

			//spew.Dump(rowColumnValue, fieldValue)
			setReflectValue(rowColumnValue, fieldValue)
		}
	}

	return nil
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
	case "string", "int32", "int16", "float64", "time.Time":
		return true
	case "*string", "*int32", "*int16", "*float64", "*time.Time":
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
	//fmt.Println(scanType.String())
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
