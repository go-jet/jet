package qrm

import (
	"context"
	"fmt"
	"github.com/go-jet/jet/v2/internal/utils/must"
	"github.com/jackc/pgx/v5"
	"reflect"
)

// QueryablePGX interface for pgx Query method
type QueryablePGX interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// QueryPGX executes Query Result Mapping (QRM) of `query` with list of parametrized arguments `arg` over database connection `db`
// using context `ctx` into destination `destPtr`.
// Destination can be either pointer to struct or pointer to slice of structs.
// If destination is pointer to struct and query result set is empty, method returns qrm.ErrNoRows.
func QueryPGX(ctx context.Context, db QueryablePGX, query string, args []interface{}, destPtr interface{}) (rowsProcessed int64, err error) {

	must.BeInitializedPtr(db, "jet: db is nil")
	must.BeInitializedPtr(destPtr, "jet: destination is nil")
	must.BeTypeKind(destPtr, reflect.Ptr, "jet: destination has to be a pointer to slice or pointer to struct")

	destinationPtrType := reflect.TypeOf(destPtr)

	if destinationPtrType.Elem().Kind() == reflect.Slice {
		rowsProcessed, err := queryToSlicePGX(ctx, db, query, args, destPtr)
		if err != nil {
			return rowsProcessed, fmt.Errorf("jet: %w", err)
		}
		return rowsProcessed, nil
	} else if destinationPtrType.Elem().Kind() == reflect.Struct {
		tempSlicePtrValue := reflect.New(reflect.SliceOf(destinationPtrType))
		tempSliceValue := tempSlicePtrValue.Elem()

		rowsProcessed, err := queryToSlicePGX(ctx, db, query, args, tempSlicePtrValue.Interface())

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

func queryToSlicePGX(ctx context.Context, db QueryablePGX, query string, args []interface{}, slicePtr interface{}) (rowsProcessed int64, err error) {
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
