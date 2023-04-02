package testutils

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-jet/jet/v2/internal/jet"
	"github.com/go-jet/jet/v2/internal/utils/throw"
	"github.com/go-jet/jet/v2/qrm"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// UnixTimeComparer will compare time equality while ignoring time zone
var UnixTimeComparer = cmp.Comparer(func(t1, t2 time.Time) bool {
	return t1.Unix() == t2.Unix()
})

// AssertExecAndRollback will execute and rollback statement in sql transaction
func AssertExecAndRollback(t *testing.T, stmt jet.Statement, db *sql.DB, rowsAffected ...int64) {
	tx, err := db.Begin()
	require.NoError(t, err)
	defer func() {
		err := tx.Rollback()
		require.NoError(t, err)
	}()

	AssertExec(t, stmt, tx, rowsAffected...)
}

// AssertExec assert statement execution for successful execution and number of rows affected
func AssertExec(t *testing.T, stmt jet.Statement, db qrm.DB, rowsAffected ...int64) {
	res, err := stmt.Exec(db)

	require.NoError(t, err)
	rows, err := res.RowsAffected()
	require.NoError(t, err)

	if len(rowsAffected) > 0 {
		require.Equal(t, rowsAffected[0], rows)
	}
}

// ExecuteInTxAndRollback will execute function in sql transaction and then rollback transaction
func ExecuteInTxAndRollback(t *testing.T, db *sql.DB, f func(tx *sql.Tx)) {
	tx, err := db.Begin()
	require.NoError(t, err)
	defer func() {
		err := tx.Rollback()
		require.NoError(t, err)
	}()

	f(tx)
}

// AssertExecErr assert statement execution for failed execution with error string errorStr
func AssertExecErr(t *testing.T, stmt jet.Statement, db qrm.DB, errorStr string) {
	_, err := stmt.Exec(db)

	require.Error(t, err, errorStr)
}

// AssertExecContextErr assert statement execution for failed execution with error string errorStr
func AssertExecContextErr(ctx context.Context, t *testing.T, stmt jet.Statement, db qrm.DB, errorStr string) {
	_, err := stmt.ExecContext(ctx, db)

	require.Error(t, err, errorStr)
}

func getFullPath(relativePath string) string {
	path, _ := os.Getwd()
	return filepath.Join(path, "../", relativePath)
}

// PrintJson print v as json
func PrintJson(v interface{}) {
	jsonText, _ := json.MarshalIndent(v, "", "\t")
	fmt.Println(string(jsonText))
}

// ToJSON converts v into json string
func ToJSON(v interface{}) string {
	jsonText, _ := json.MarshalIndent(v, "", "\t")
	return string(jsonText)
}

// AssertJSON check if data json output is the same as expectedJSON
func AssertJSON(t *testing.T, data interface{}, expectedJSON string) {
	jsonData, err := json.MarshalIndent(data, "", "\t")
	require.NoError(t, err)

	dataJson := "\n" + string(jsonData) + "\n"
	require.Equal(t, dataJson, expectedJSON)
}

// SaveJSONFile saves v as json at testRelativePath
func SaveJSONFile(v interface{}, testRelativePath string) {
	jsonText, _ := json.MarshalIndent(v, "", "\t")

	filePath := getFullPath(testRelativePath)
	err := ioutil.WriteFile(filePath, jsonText, 0644)

	throw.OnError(err)
}

// AssertJSONFile check if data json representation is the same as json at testRelativePath
func AssertJSONFile(t *testing.T, data interface{}, testRelativePath string) {

	filePath := getFullPath(testRelativePath)
	fileJSONData, err := ioutil.ReadFile(filePath)
	require.NoError(t, err)

	if runtime.GOOS == "windows" {
		fileJSONData = bytes.Replace(fileJSONData, []byte("\r\n"), []byte("\n"), -1)
	}

	jsonData, err := json.MarshalIndent(data, "", "\t")
	require.NoError(t, err)

	require.True(t, string(fileJSONData) == string(jsonData))
	//AssertDeepEqual(t, string(fileJSONData), string(jsonData))
}

// AssertStatementSql check if statement Sql() is the same as expectedQuery and expectedArgs
func AssertStatementSql(t *testing.T, query jet.Statement, expectedQuery string, expectedArgs ...interface{}) {
	queryStr, args := query.Sql()
	assertQueryString(t, queryStr, expectedQuery)

	if len(expectedArgs) == 0 {
		return
	}
	AssertDeepEqual(t, args, expectedArgs)
}

// AssertStatementSqlErr checks if statement Sql() panics with errorStr
func AssertStatementSqlErr(t *testing.T, stmt jet.Statement, errorStr string) {
	defer func() {
		r := recover()
		require.Equal(t, r, errorStr)
	}()

	stmt.Sql()
}

// AssertDebugStatementSql check if statement Sql() is the same as expectedQuery
func AssertDebugStatementSql(t *testing.T, query jet.Statement, expectedQuery string, expectedArgs ...interface{}) {
	_, args := query.Sql()

	if len(expectedArgs) > 0 {
		AssertDeepEqual(t, args, expectedArgs)
	}

	debugSql := query.DebugSql()
	assertQueryString(t, debugSql, expectedQuery)
}

// AssertSerialize checks if clause serialize produces expected query and args
func AssertSerialize(t *testing.T, dialect jet.Dialect, serializer jet.Serializer, query string, args ...interface{}) {
	out := jet.SQLBuilder{Dialect: dialect}
	jet.Serialize(serializer, jet.SelectStatementType, &out)

	//fmt.Println(out.Buff.String())

	AssertDeepEqual(t, out.Buff.String(), query)

	if len(args) > 0 {
		AssertDeepEqual(t, out.Args, args)
	}
}

// AssertDebugSerialize checks if clause serialize produces expected debug query and args
func AssertDebugSerialize(t *testing.T, dialect jet.Dialect, clause jet.Serializer, query string, args ...interface{}) {
	out := jet.SQLBuilder{Dialect: dialect, Debug: true}
	jet.Serialize(clause, jet.SelectStatementType, &out)

	AssertDeepEqual(t, out.Buff.String(), query)

	if len(args) > 0 {
		AssertDeepEqual(t, out.Args, args)
	}
}

// AssertClauseSerialize checks if clause serialize produces expected query and args
func AssertClauseSerialize(t *testing.T, dialect jet.Dialect, clause jet.Clause, query string, args ...interface{}) {
	out := jet.SQLBuilder{Dialect: dialect}
	clause.Serialize(jet.SelectStatementType, &out)

	require.Equal(t, out.Buff.String(), query)

	if len(args) > 0 {
		AssertDeepEqual(t, out.Args, args)
	}
}

// AssertPanicErr checks if running a function fun produces a panic with errorStr string
func AssertPanicErr(t *testing.T, fun func(), errorStr string) {
	defer func() {
		r := recover()
		require.Equal(t, r, errorStr)
	}()

	fun()
}

// AssertSerializeErr check if clause serialize panics with errString
func AssertSerializeErr(t *testing.T, dialect jet.Dialect, clause jet.Serializer, errString string) {
	defer func() {
		r := recover()
		require.Equal(t, r, errString)
	}()

	out := jet.SQLBuilder{Dialect: dialect}
	jet.Serialize(clause, jet.SelectStatementType, &out)
}

// AssertProjectionSerialize check if projection serialize produces expected query and args
func AssertProjectionSerialize(t *testing.T, dialect jet.Dialect, projection jet.Projection, query string, args ...interface{}) {
	out := jet.SQLBuilder{Dialect: dialect}
	jet.SerializeForProjection(projection, jet.SelectStatementType, &out)

	AssertDeepEqual(t, out.Buff.String(), query)
	AssertDeepEqual(t, out.Args, args)
}

// AssertQueryPanicErr check if statement Query execution panics with error errString
func AssertQueryPanicErr(t *testing.T, stmt jet.Statement, db qrm.DB, dest interface{}, errString string) {
	defer func() {
		r := recover()
		require.Equal(t, r, errString)
	}()

	_ = stmt.Query(db, dest)
}

// AssertFileContent check if file content at filePath contains expectedContent text.
func AssertFileContent(t *testing.T, filePath string, expectedContent string) {
	enumFileData, err := ioutil.ReadFile(filePath)

	require.NoError(t, err)

	require.Equal(t, "\n"+string(enumFileData), expectedContent)
}

// AssertFileNamesEqual check if all filesInfos are contained in fileNames
func AssertFileNamesEqual(t *testing.T, dirPath string, fileNames ...string) {
	files, err := ioutil.ReadDir(dirPath)
	require.NoError(t, err)

	require.Equal(t, len(files), len(fileNames))

	fileNamesMap := map[string]bool{}

	for _, fileInfo := range files {
		fileNamesMap[fileInfo.Name()] = true
	}

	for _, fileName := range fileNames {
		require.True(t, fileNamesMap[fileName], fileName+" does not exist.")
	}
}

// AssertDeepEqual checks if actual and expected objects are deeply equal.
func AssertDeepEqual(t *testing.T, actual, expected interface{}, option ...cmp.Option) {
	if !assert.True(t, cmp.Equal(actual, expected, option...)) {
		printDiff(actual, expected, option...)
		t.FailNow()
	}
}

func assertQueryString(t *testing.T, actual, expected string) {
	if !assert.Equal(t, actual, expected) {
		printDiff(actual, expected)
		t.FailNow()
	}
}

func printDiff(actual, expected interface{}, options ...cmp.Option) {
	fmt.Println(cmp.Diff(actual, expected, options...))
	fmt.Println("Actual: ")
	fmt.Println(actual)
	fmt.Println("Expected: ")
	fmt.Println(expected)
}

// BoolPtr returns address of bool parameter
func BoolPtr(b bool) *bool {
	return &b
}

// Int8Ptr returns address of int8 parameter
func Int8Ptr(i int8) *int8 {
	return &i
}

// UInt8Ptr returns address of uint8 parameter
func UInt8Ptr(i uint8) *uint8 {
	return &i
}

// Int16Ptr returns address of int16 parameter
func Int16Ptr(i int16) *int16 {
	return &i
}

// UInt16Ptr returns address of uint16 parameter
func UInt16Ptr(i uint16) *uint16 {
	return &i
}

// Int32Ptr returns address of int32 parameter
func Int32Ptr(i int32) *int32 {
	return &i
}

// UInt32Ptr returns address of uint32 parameter
func UInt32Ptr(i uint32) *uint32 {
	return &i
}

// Int64Ptr returns address of int64 parameter
func Int64Ptr(i int64) *int64 {
	return &i
}

// UInt64Ptr returns address of uint64 parameter
func UInt64Ptr(i uint64) *uint64 {
	return &i
}

// StringPtr returns address of string parameter
func StringPtr(s string) *string {
	return &s
}

// TimePtr returns address of time.Time parameter
func TimePtr(t time.Time) *time.Time {
	return &t
}

// ByteArrayPtr returns address of []byte parameter
func ByteArrayPtr(arr []byte) *[]byte {
	return &arr
}

// Float32Ptr returns address of float32 parameter
func Float32Ptr(f float32) *float32 {
	return &f
}

// Float64Ptr returns address of float64 parameter
func Float64Ptr(f float64) *float64 {
	return &f
}

// UUIDPtr returns address of uuid.UUID
func UUIDPtr(u string) *uuid.UUID {
	newUUID := uuid.MustParse(u)

	return &newUUID
}
