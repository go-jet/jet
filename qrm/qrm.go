package qrm

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/go-jet/jet/v2/internal/utils/must"
)

// Config holds the configuration settings for QRM scanning behavior.
type Config struct {
	// StrictScan, when true, causes the scanning function to panic if it encounters any
	// unused columns in the SQL query result. This ensures that every column is mapped
	// to a field in the destination struct.
	// Does not apply to statements build with SELECT_JSON_OBJ or SELECT_JSON_ARR
	StrictScan bool

	// StrictFieldMapping, when true, causes the scanning function to panic if it encounters any
	// destination struct fields that do not have matching columns in the SQL query result.
	// This check applies only to fields that are mapped from a single column (simple/scanner/json_column).
	// Complex fields (struct/slice) are excluded because they are populated recursively and can be optional.
	// Does not apply to statements build with SELECT_JSON_OBJ or SELECT_JSON_ARR
	StrictFieldMapping bool

	// JsonUnmarshalFunc is called by the Query method to unmarshal JSON query results created by
	// SELECT_JSON_OBJ and SELECT_JSON_ARR statements.
	// It can be replaced with any implementation that matches the standard "encoding/json" `Unmarshal` function signature.
	// By default, it uses the `Unmarshal` function from Go's standard `encoding/json` package.
	JsonUnmarshalFunc func(data []byte, v any) error
}

// GlobalConfig is the package-wide configuration for SQL scanning.
// This variable is not thread safe, and it should be modified only once, for instance, during application initialization.
var GlobalConfig = Config{
	StrictScan:         false,
	StrictFieldMapping: false,
	JsonUnmarshalFunc:  json.Unmarshal,
}

// ErrNoRows is returned by Query when query result set is empty
var ErrNoRows = errors.New("qrm: no rows in result set")

// QueryJsonObj executes a SQL query that returns a JSON object, unmarshals the result into the provided destination,
// and returns the number of rows processed.
//
// The query must return exactly one row with a single column; otherwise, an error is returned.
//
// Parameters:
//
//	ctx      - The context for managing query execution (timeouts, cancellations).
//	db       - The database connection or transaction that implements the Queryable interface.
//	query    - The SQL query string to be executed.
//	args     - A slice of arguments to be used with the query.
//	destPtr  - A pointer to the variable where the unmarshaled JSON result will be stored.
//	          The destination should be a pointer to a struct or map[string]any.
//
// Returns:
//
//	rowsProcessed - The number of rows processed by the query execution.
//	err           - An error if query execution or unmarshaling fails.
func QueryJsonObj(ctx context.Context, db Queryable, query string, args []interface{}, destPtr interface{}) (rowsProcessed int64, err error) {
	must.BeInitializedPtr(destPtr, "jet: destination is nil")
	must.BeTypeKind(destPtr, reflect.Ptr, jsonDestObjErr)
	destType := reflect.TypeOf(destPtr).Elem()
	must.BeTrue(destType.Kind() == reflect.Struct || destType.Kind() == reflect.Map, jsonDestObjErr)

	return queryJson(ctx, db, query, args, destPtr)
}

// QueryJsonArr executes a SQL query that returns a JSON array, unmarshals the result into the provided destination,
// and returns the number of rows processed.
//
// The query must return exactly one row with a single column; otherwise, an error is returned.
//
// Parameters:
//
//	ctx      - The context for managing query execution (timeouts, cancellations).
//	db       - The database connection or transaction that implements the Queryable interface.
//	query    - The SQL query string to be executed.
//	args     - A slice of arguments to be used with the query.
//	destPtr  - A pointer to the variable where the unmarshaled JSON array will be stored.
//	          The destination should be a pointer to a slice of structs or []map[string]any.
//
// Returns:
//
//	rowsProcessed - The number of rows processed by the query execution.
//	err           - An error if query execution or unmarshaling fails.
func QueryJsonArr(ctx context.Context, db Queryable, query string, args []interface{}, destPtr interface{}) (rowsProcessed int64, err error) {
	must.BeInitializedPtr(destPtr, "jet: destination is nil")
	must.BeTypeKind(destPtr, reflect.Ptr, jsonDestArrErr)
	destType := reflect.TypeOf(destPtr).Elem()
	must.BeTrue(destType.Kind() == reflect.Slice, jsonDestArrErr)

	return queryJson(ctx, db, query, args, destPtr)
}

var jsonDestObjErr = "jet: SELECT_JSON_OBJ destination has to be a pointer to struct or pointer to map[string]any"
var jsonDestArrErr = "jet: SELECT_JSON_ARR destination has to be a pointer to slice of struct or pointer to []map[string]any"

func queryJson(ctx context.Context, db Queryable, query string, args []interface{}, destPtr interface{}) (rowsProcessed int64, err error) {
	must.BeInitializedPtr(db, "jet: db is nil")

	var rows *sql.Rows
	rows, err = db.QueryContext(ctx, query, args...)

	if err != nil {
		return 0, err
	}

	defer rows.Close()

	if !rows.Next() {
		err = rows.Err()
		if err != nil {
			return 0, err
		}
		return 0, ErrNoRows
	}

	var jsonData []byte
	err = rows.Scan(&jsonData)

	if err != nil {
		return 1, err
	}

	if jsonData == nil {
		return 1, nil
	}

	err = GlobalConfig.JsonUnmarshalFunc(jsonData, &destPtr)

	if err != nil {
		return 1, fmt.Errorf("jet: invalid json, %w", err)
	}

	if rows.Next() {
		return 1, fmt.Errorf("jet: query returned more then one row")
	}

	err = rows.Close()
	if err != nil {
		return 1, err
	}

	return 1, nil
}

// Query executes a Query Result Mapping (QRM) of the provided SQL `query` with a list of parameterized arguments `args`
// over the database connection `db` using the provided context `ctx` and stores the result in the destination `destPtr`.
//
// The destination must be a pointer to either a struct or a slice of structs
// If the destination is a pointer to a struct and no rows are returned, the method returns qrm.ErrNoRows.
//
// Parameters:
//
//	ctx      - The context for managing query execution (timeouts, cancellations).
//	db       - The database connection or transaction implementing the Queryable interface.
//	query    - The SQL query string to be executed.
//	args     - A slice of arguments to be used with the query.
//	destPtr  - A pointer to the variable where the query result will be stored. This can be a pointer to a struct or a slice of structs.
//
// Returns:
//
//	rowsProcessed - The number of rows processed by the query execution.
//	err           - An error if query execution or result mapping fails, or if no rows are found when a struct is expected.
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

	scanContext.rowNum++

	_, err = mapRowToStruct(scanContext, "", destValuePtr, nil)

	if err != nil {
		return fmt.Errorf("jet: failed to scan a row into destination, %w", err)
	}

	scanContext.EnsureEveryColumnRead() // can panic
	if GlobalConfig.StrictFieldMapping {
		scanContext.EnsureEveryFieldMapped() // can panic
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

		if scanContext.rowNum == 1 && GlobalConfig.StrictScan {
			scanContext.EnsureEveryColumnRead()
		}
		if scanContext.rowNum == 1 && GlobalConfig.StrictFieldMapping {
			scanContext.EnsureEveryFieldMapped()
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
		typeName, columnName, _ := getTypeAndFieldName("", *field)
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

		fieldMappingInfo := typeInf.fieldMappings[i]

		switch fieldMappingInfo.Type {

		case complexType:
			var changed bool
			changed, err = mapRowToDestinationValue(scanContext, concat(groupKey, ":", field.Name), fieldValue, &field)

			if err != nil {
				return
			}

			if changed {
				updated = true
			}
		default:
			if mapOnlySlices || fieldMappingInfo.rowIndex == -1 {
				continue
			}

			scannedValue := scanContext.rowElemValue(fieldMappingInfo.rowIndex)

			if !scannedValue.IsValid() {
				setZeroValue(fieldValue) // scannedValue is nil, destination should be set to zero value
				continue
			}

			updated = true

			switch fieldMappingInfo.Type {
			case implementsScanner:
				initializeValueIfNilPtr(fieldValue)
				fieldScanner := getScanner(fieldValue)

				value := scannedValue.Interface()

				err := fieldScanner.Scan(value)

				if err != nil {
					return updated, qrmAssignError(scannedValue, field, err)
				}
			case jsonUnmarshal:
				value, ok := scannedValue.Interface().([]byte)

				if !ok {
					return updated, qrmAssignError(scannedValue, field, fmt.Errorf("value not convertable to []byte"))
				}

				fieldInterface := fieldValue.Addr().Interface()

				err := json.Unmarshal(value, fieldInterface)

				if err != nil {
					return updated, qrmAssignError(scannedValue, field, fmt.Errorf("invalid json, %w", err))
				}
			default: // simple type
				err := assign(scannedValue, fieldValue)

				if err != nil {
					return updated, qrmAssignError(scannedValue, field, err)
				}
			}
		}
	}

	return
}

func qrmAssignError(scannedValue reflect.Value, field reflect.StructField, err error) error {
	return fmt.Errorf(`can't assign %T(%q) to '%s %s': %w`, scannedValue.Interface(), scannedValue.Interface(),
		field.Name, field.Type.String(), err)
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
