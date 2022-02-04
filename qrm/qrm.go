package qrm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/go-jet/jet/v2/internal/utils"
)

// ErrNoRows is returned by Query when query result set is empty
var ErrNoRows = errors.New("qrm: no rows in result set")

// Query executes Query Result Mapping (QRM) of `query` with list of parametrized arguments `arg` over database connection `db`
// using context `ctx` into destination `destPtr`.
// Destination can be either pointer to struct or pointer to slice of structs.
// If destination is pointer to struct and query result set is empty, method returns qrm.ErrNoRows.
func Query(ctx context.Context, db DB, query string, args []interface{}, destPtr interface{}) (rowsProcessed int64, err error) {

	utils.MustBeInitializedPtr(db, "jet: db is nil")
	utils.MustBeInitializedPtr(destPtr, "jet: destination is nil")
	utils.MustBe(destPtr, reflect.Ptr, "jet: destination has to be a pointer to slice or pointer to struct")

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
	utils.MustBeInitializedPtr(destPtr, "jet: destination is nil")
	utils.MustBe(destPtr, reflect.Ptr, "jet: destination has to be a pointer to slice or pointer to struct")

	if len(scanContext.row) == 0 {
		return errors.New("empty row slice")
	}

	err := rows.Scan(scanContext.row...)

	if err != nil {
		return fmt.Errorf("rows scan error, %w", err)
	}

	destValue := reflect.ValueOf(destPtr)

	_, err = mapRowToStruct(scanContext, "", newTypeStack(), destValue, nil)

	if err != nil {
		return fmt.Errorf("failed to map a row, %w", err)
	}

	return nil
}

func queryToSlice(ctx context.Context, db DB, query string, args []interface{}, slicePtr interface{}) (rowsProcessed int64, err error) {
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

		_, err = mapRowToSlice(scanContext, "", newTypeStack(), slicePtrValue, nil)

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
	typesVisited *typeStack,
	slicePtrValue reflect.Value,
	field *reflect.StructField) (updated bool, err error) {

	sliceElemType := getSliceElemType(slicePtrValue)

	if isSimpleModelType(sliceElemType) {
		updated, err = mapRowToBaseTypeSlice(scanContext, slicePtrValue, field)
		return
	}

	utils.TypeMustBe(sliceElemType, reflect.Struct, "jet: unsupported slice element type"+fieldToString(field))

	structGroupKey := scanContext.getGroupKey(sliceElemType, field)

	groupKey = groupKey + "," + structGroupKey

	index, ok := scanContext.uniqueDestObjectsMap[groupKey]

	if ok {
		structPtrValue := getSliceElemPtrAt(slicePtrValue, index)

		return mapRowToStruct(scanContext, groupKey, typesVisited, structPtrValue, field, true)
	}

	destinationStructPtr := newElemPtrValueForSlice(slicePtrValue)

	updated, err = mapRowToStruct(scanContext, groupKey, typesVisited, destinationStructPtr, field)

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
	rowElemPtr := scanContext.rowElemValuePtr(index)

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
	typesVisited *typeStack, // to prevent circular dependency scan
	structPtrValue reflect.Value,
	parentField *reflect.StructField,
	onlySlices ...bool, // small optimization, not to assign to already assigned struct fields
) (updated bool, err error) {

	mapOnlySlices := len(onlySlices) > 0
	structType := structPtrValue.Type().Elem()

	if typesVisited.contains(&structType) {
		return false, nil
	}

	typesVisited.push(&structType)
	defer typesVisited.pop()

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
			changed, err = mapRowToDestinationValue(scanContext, groupKey, typesVisited, fieldValue, &field)

			if err != nil {
				return
			}

			if changed {
				updated = true
			}

		} else {
			if mapOnlySlices || fieldMap.columnIndex == -1 {
				continue
			}

			cellValue := scanContext.rowElem(fieldMap.columnIndex)

			if cellValue == nil {
				continue
			}

			initializeValueIfNilPtr(fieldValue)
			updated = true

			if fieldMap.implementsScanner {
				scanner := getScanner(fieldValue)

				err = scanner.Scan(cellValue)

				if err != nil {
					err = fmt.Errorf(`can't scan %T(%q) to '%s %s': %w`, cellValue, cellValue, field.Name, field.Type.String(), err)
					return
				}
			} else {
				err = setReflectValue(reflect.ValueOf(cellValue), fieldValue)

				if err != nil {
					err = fmt.Errorf(`can't assign %T(%q) to '%s %s': %w`, cellValue, cellValue, field.Name, field.Type.String(), err)
					return
				}
			}
		}
	}

	return
}

func mapRowToDestinationValue(
	scanContext *ScanContext,
	groupKey string,
	typesVisited *typeStack,
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

	updated, err = mapRowToDestinationPtr(scanContext, groupKey, typesVisited, destPtrValue, structField)

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
	typesVisited *typeStack,
	destPtrValue reflect.Value,
	structField *reflect.StructField) (updated bool, err error) {

	utils.ValueMustBe(destPtrValue, reflect.Ptr, "jet: internal error. Destination is not pointer.")

	destValueKind := destPtrValue.Elem().Kind()

	if destValueKind == reflect.Struct {
		return mapRowToStruct(scanContext, groupKey, typesVisited, destPtrValue, structField)
	} else if destValueKind == reflect.Slice {
		return mapRowToSlice(scanContext, groupKey, typesVisited, destPtrValue, structField)
	} else {
		panic("jet: unsupported dest type: " + structField.Name + " " + structField.Type.String())
	}
}
