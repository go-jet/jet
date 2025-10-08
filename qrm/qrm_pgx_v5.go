package qrm

import (
	"context"
	"fmt"
	"github.com/go-jet/jet/v2/internal/utils/must"
	"github.com/jackc/pgx/v5"
	"reflect"
)

// QueryablePgxV5 interface for pgx Query method
type QueryablePgxV5 interface {
	Query(ctx context.Context, query string, args ...any) (pgx.Rows, error)
}

// QueryJsonObjPgxV5 executes a SQL query that returns a JSON object, unmarshals the result into the provided destination,
// and returns the number of rows processed.
//
// The query must return exactly one row with a single column; otherwise, an error is returned.
//
// Parameters:
//
//	ctx      - The context for managing query execution (timeouts, cancellations).
//	db       - The database connection or transaction that implements the QueryablePGX interface.
//	query    - The SQL query string to be executed.
//	args     - A slice of arguments to be used with the query.
//	destPtr  - A pointer to the variable where the unmarshaled JSON result will be stored.
//	          The destination should be a pointer to a struct or map[string]any.
//
// Returns:
//
//	rowsProcessed - The number of rows processed by the query execution.
//	err           - An error if query execution or unmarshaling fails.
func QueryJsonObjPgxV5(ctx context.Context, db QueryablePgxV5, query string, args []interface{}, destPtr interface{}) (rowsProcessed int64, err error) {
	must.BeInitializedPtr(destPtr, "jet: destination is nil")
	must.BeTypeKind(destPtr, reflect.Ptr, jsonDestObjErr)
	destType := reflect.TypeOf(destPtr).Elem()
	must.BeTrue(destType.Kind() == reflect.Struct || destType.Kind() == reflect.Map, jsonDestObjErr)

	return queryJsonPgxV5(ctx, db, query, args, destPtr)
}

// QueryJsonArrPgxV5 executes a SQL query that returns a JSON array, unmarshals the result into the provided destination,
// and returns the number of rows processed.
//
// The query must return exactly one row with a single column; otherwise, an error is returned.
//
// Parameters:
//
//	ctx      - The context for managing query execution (timeouts, cancellations).
//	db       - The database connection or transaction that implements the QueryablePGX interface.
//	query    - The SQL query string to be executed.
//	args     - A slice of arguments to be used with the query.
//	destPtr  - A pointer to the variable where the unmarshaled JSON array will be stored.
//	          The destination should be a pointer to a slice of structs or []map[string]any.
//
// Returns:
//
//	rowsProcessed - The number of rows processed by the query execution.
//	err           - An error if query execution or unmarshaling fails.
func QueryJsonArrPgxV5(ctx context.Context, db QueryablePgxV5, query string, args []interface{}, destPtr interface{}) (rowsProcessed int64, err error) {
	must.BeInitializedPtr(destPtr, "jet: destination is nil")
	must.BeTypeKind(destPtr, reflect.Ptr, jsonDestArrErr)
	destType := reflect.TypeOf(destPtr).Elem()
	must.BeTrue(destType.Kind() == reflect.Slice, jsonDestArrErr)

	return queryJsonPgxV5(ctx, db, query, args, destPtr)
}

// QueryPgxV5 executes Query Result Mapping (QRM) of `query` with list of parametrized arguments `arg` over database connection `db`
// using context `ctx` into destination `destPtr`.
// Destination can be either pointer to struct or pointer to slice of structs.
// If destination is pointer to struct and query result set is empty, method returns qrm.ErrNoRows.
func QueryPgxV5(ctx context.Context, db QueryablePgxV5, query string, args []interface{}, destPtr interface{}) (rowsProcessed int64, err error) {

	must.BeInitializedPtr(db, "jet: db is nil")
	must.BeInitializedPtr(destPtr, "jet: destination is nil")
	must.BeTypeKind(destPtr, reflect.Ptr, "jet: destination has to be a pointer to slice or pointer to struct")

	destinationPtrType := reflect.TypeOf(destPtr)

	if destinationPtrType.Elem().Kind() == reflect.Slice {
		rowsProcessed, err := queryToSlicePgxV5(ctx, db, query, args, destPtr)
		if err != nil {
			return rowsProcessed, fmt.Errorf("jet: %w", err)
		}
		return rowsProcessed, nil
	} else if destinationPtrType.Elem().Kind() == reflect.Struct {
		tempSlicePtrValue := reflect.New(reflect.SliceOf(destinationPtrType))
		tempSliceValue := tempSlicePtrValue.Elem()

		rowsProcessed, err := queryToSlicePgxV5(ctx, db, query, args, tempSlicePtrValue.Interface())

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

func queryToSlicePgxV5(ctx context.Context, db QueryablePgxV5, query string, args []interface{}, slicePtr interface{}) (rowsProcessed int64, err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	rows, err := db.Query(ctx, query, args...)

	if err != nil {
		return
	}
	defer rows.Close()

	scanContext, err := NewScanContextPGXv5(rows)

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

	rows.Close()

	return scanContext.rowNum, rows.Err()
}

func queryJsonPgxV5(ctx context.Context, db QueryablePgxV5, query string, args []interface{}, destPtr interface{}) (rowsProcessed int64, err error) {
	must.BeInitializedPtr(db, "jet: db is nil")

	var rows pgx.Rows
	rows, err = db.Query(ctx, query, args...)

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

	rows.Close()

	return 1, nil
}
