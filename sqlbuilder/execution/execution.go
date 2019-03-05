package execution

import (
	"database/sql"
	"errors"
	"github.com/serenize/snaker"
	"reflect"
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

	columnNames, _ := rows.Columns()
	columnTypes, _ := rows.ColumnTypes()
	values := createScanValue(columnTypes)

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

	for i := 0; i < structType.NumField(); i++ {
		fieldType := structType.Field(i)
		fieldValue := structValue.Field(i)

		fieldName := fieldType.Name

		//columnName := structName + "." + fieldName
		columnName := snaker.CamelToSnake(fieldName)

		rowIndex := getIndex(columnNames, columnName)

		if rowIndex < 0 {
			continue
		}

		rowColumnValue := reflect.ValueOf(row[rowIndex])

		setReflectValue(rowColumnValue, fieldValue)
	}

	return nil
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
		columnType := sqlColumnType.ScanType()

		columnValue := reflect.New(columnType)

		values[i] = columnValue.Interface()
	}

	return values
}
