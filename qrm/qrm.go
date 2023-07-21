package qrm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/go-jet/jet/v2/internal/utils/must"
	"reflect"
)

// ErrNoRows is returned by Query when query result set is empty
var ErrNoRows = errors.New("qrm: no rows in result set")

// Query executes Query Result Mapping (QRM) of `query` with list of parametrized arguments `arg` over database connection `db`
// using context `ctx` into destination `destPtr`.
// Destination can be either pointer to struct or pointer to slice of structs.
// If destination is pointer to struct and query result set is empty, method returns qrm.ErrNoRows.
func Query(ctx context.Context, db Queryable, query string, args []interface{}, destPtr interface{}) (rowsProcessed int64, err error) {

	must.BeInitializedPtr(db, "jet: db is nil")
	must.BeInitializedPtr(destPtr, "jet: destination is nil")
	must.BeTypeKind(destPtr, reflect.Ptr, "jet: destination has to be a pointer to slice or pointer to struct")

	destinationPtrType := reflect.TypeOf(destPtr)

	if destinationPtrType.Elem().Kind() == reflect.Slice {
		rowsProcessed, err := queryToSlice(ctx, db, query, args, destPtr)
		if err != nil {
			return rowsProcessed, fmt.Errorf("jet: %w", err)
		}
		return rowsProcessed, nil
	} else if destinationPtrType.Elem().Kind() == reflect.Struct {
		tempSlicePtrValue := reflect.New(reflect.SliceOf(destinationPtrType))
		tempSliceValue := tempSlicePtrValue.Elem()

		rowsProcessed, err := queryToSlice(ctx, db, query, args, tempSlicePtrValue.Interface())

		if err != nil {
			return rowsProcessed, fmt.Errorf("jet: %w", err)
		}

		if rowsProcessed == 0 {
			return 0, ErrNoRows
		}

		// edge case when row result set contains only NULLs.
		if tempSliceValue.Len() == 0 {
			return rowsProcessed, nil
		}

		structValue := reflect.ValueOf(destPtr).Elem()
		firstTempStruct := tempSliceValue.Index(0).Elem()

		if structValue.Type().AssignableTo(firstTempStruct.Type()) {
			structValue.Set(tempSliceValue.Index(0).Elem())
		}
		return rowsProcessed, nil
	} else {
		panic("jet: destination has to be a pointer to slice or pointer to struct")
	}
}

// ScanOneRowToDest will scan one row into struct destination
func ScanOneRowToDest(scanContext *ScanContext, rows *sql.Rows, destPtr interface{}) error {
	must.BeInitializedPtr(destPtr, "jet: destination is nil")
	must.BeTypeKind(destPtr, reflect.Ptr, "jet: destination has to be a pointer to slice or pointer to struct")

	if len(scanContext.row) == 0 {
		return errors.New("empty row slice")
	}

	err := rows.Scan(scanContext.row...)

	if err != nil {
		return fmt.Errorf("jet: rows scan error, %w", err)
	}

	destValuePtr := reflect.ValueOf(destPtr)

	_, err = mapRowToStruct(scanContext, "", destValuePtr, nil)

	if err != nil {
		return fmt.Errorf("jet: failed to scan a row into destination, %w", err)
	}

	return nil
}

func queryToSlice(ctx context.Context, db Queryable, query string, args []interface{}, slicePtr interface{}) (rowsProcessed int64, err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	rows, err := db.QueryContext(ctx, query, args...)

	if err != nil {
		return
	}
	defer rows.Close()

	scanContext, err := NewScanContext(rows)

	if err != nil {
		return
	}

	if len(scanContext.row) == 0 {
		return
	}

	slicePtrValue := reflect.ValueOf(slicePtr)

	for rows.Next() {
		err = rows.Scan(scanContext.row...)

		if err != nil {
			return scanContext.rowNum, err
		}

		scanContext.rowNum++

		_, err = mapRowToSlice(scanContext, "", slicePtrValue, nil)

		if err != nil {
			return scanContext.rowNum, err
		}
	}

	err = rows.Close()
	if err != nil {
		return scanContext.rowNum, err
	}

	return scanContext.rowNum, rows.Err()
}

func mapRowToSlice(
	scanContext *ScanContext,
	groupKey string,
	slicePtrValue reflect.Value,
	field *reflect.StructField) (updated bool, err error) {

	sliceElemType := getSliceElemType(slicePtrValue)

	if isSimpleModelType(sliceElemType) {
		updated, err = mapRowToBaseTypeSlice(scanContext, slicePtrValue, field)
		return
	}

	must.TypeBeOfKind(sliceElemType, reflect.Struct, "jet: unsupported slice element type"+fieldToString(field))

	structGroupKey := scanContext.getGroupKey(sliceElemType, field)

	groupKey = concat(groupKey, ",", structGroupKey)

	index, ok := scanContext.uniqueDestObjectsMap[groupKey]

	if ok {
		structPtrValue := getSliceElemPtrAt(slicePtrValue, index)

		return mapRowToStruct(scanContext, groupKey, structPtrValue, field, true)
	}

	destinationStructPtr := newElemPtrValueForSlice(slicePtrValue)

	updated, err = mapRowToStruct(scanContext, groupKey, destinationStructPtr, field)

	if err != nil {
		return
	}

	if updated {
		scanContext.uniqueDestObjectsMap[groupKey] = slicePtrValue.Elem().Len()
		err = appendElemToSlice(slicePtrValue, destinationStructPtr)

		if err != nil {
			return
		}
	}

	return
}

func mapRowToBaseTypeSlice(scanContext *ScanContext, slicePtrValue reflect.Value, field *reflect.StructField) (updated bool, err error) {
	index := 0
	if field != nil {
		typeName, columnName := getTypeAndFieldName("", *field)
		if index = scanContext.typeToColumnIndex(typeName, columnName); index < 0 {
			return
		}
	}
	rowElemPtr := scanContext.rowElemValueClonePtr(index)

	if rowElemPtr.IsValid() && !rowElemPtr.IsNil() {
		updated = true
		err = appendElemToSlice(slicePtrValue, rowElemPtr)
		if err != nil {
			return
		}
	}

	return
}

func mapRowToStruct(
	scanContext *ScanContext,
	groupKey string,
	structPtrValue reflect.Value,
	parentField *reflect.StructField,
	onlySlices ...bool, // small optimization, not to assign to already assigned struct fields
) (updated bool, err error) {

	mapOnlySlices := len(onlySlices) > 0
	structType := structPtrValue.Type().Elem()

	if scanContext.typesVisited.contains(&structType) {
		return false, nil
	}

	scanContext.typesVisited.push(&structType)
	defer scanContext.typesVisited.pop()

	typeInf := scanContext.getTypeInfo(structType, parentField)

	structValue := structPtrValue.Elem()

	for i := 0; i < structValue.NumField(); i++ {
		field := structType.Field(i)
		fieldValue := structValue.Field(i)

		if !fieldValue.CanSet() { // private field
			continue
		}

		fieldMap := typeInf.fieldMappings[i]

		if fieldMap.complexType {
			var changed bool
			changed, err = mapRowToDestinationValue(scanContext, concat(groupKey, ":", field.Name), fieldValue, &field)

			if err != nil {
				return
			}

			if changed {
				updated = true
			}

		} else {
			if mapOnlySlices || fieldMap.rowIndex == -1 {
				continue
			}

			scannedValue := scanContext.rowElemValue(fieldMap.rowIndex)

			if !scannedValue.IsValid() {
				setZeroValue(fieldValue) // scannedValue is nil, destination should be set to zero value
				continue
			}

			updated = true

			if fieldMap.implementsScanner {
				initializeValueIfNilPtr(fieldValue)
				fieldScanner := getScanner(fieldValue)

				value := scannedValue.Interface()

				err := fieldScanner.Scan(value)

				if err != nil {
					return updated, fmt.Errorf(`can't scan %T(%q) to '%s %s': %w`, value, value, field.Name, field.Type.String(), err)
				}
			} else {
				err := assign(scannedValue, fieldValue)

				if err != nil {
					return updated, fmt.Errorf(`can't assign %T(%q) to '%s %s': %w`, scannedValue.Interface(), scannedValue.Interface(),
						field.Name, field.Type.String(), err)
				}
			}
		}
	}

	return
}

func mapRowToDestinationValue(
	scanContext *ScanContext,
	groupKey string,
	dest reflect.Value,
	structField *reflect.StructField) (updated bool, err error) {

	var destPtrValue reflect.Value

	if dest.Kind() != reflect.Ptr {
		destPtrValue = dest.Addr()
	} else {
		if dest.IsNil() {
			destPtrValue = reflect.New(dest.Type().Elem())
		} else {
			destPtrValue = dest
		}
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

func mapRowToDestinationPtr(
	scanContext *ScanContext,
	groupKey string,
	destPtrValue reflect.Value,
	structField *reflect.StructField) (updated bool, err error) {

	must.ValueBeOfTypeKind(destPtrValue, reflect.Ptr, "jet: internal error. Destination is not pointer.")

	destValueKind := destPtrValue.Elem().Kind()

	if destValueKind == reflect.Struct {
		return mapRowToStruct(scanContext, groupKey, destPtrValue, structField)
	} else if destValueKind == reflect.Slice {
		return mapRowToSlice(scanContext, groupKey, destPtrValue, structField)
	} else {
		panic("jet: unsupported dest type: " + structField.Name + " " + structField.Type.String())
	}
}
