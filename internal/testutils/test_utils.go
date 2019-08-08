package testutils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/go-jet/jet/execution"
	"github.com/go-jet/jet/internal/jet"
	"gotest.tools/assert"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func AssertExec(t *testing.T, stmt jet.Statement, db execution.DB, rowsAffected ...int64) {
	res, err := stmt.Exec(db)

	assert.NilError(t, err)
	rows, err := res.RowsAffected()
	assert.NilError(t, err)

	if len(rowsAffected) > 0 {
		assert.Equal(t, rows, rowsAffected[0])
	}
}

func AssertExecErr(t *testing.T, stmt jet.Statement, db execution.DB, errorStr string) {
	_, err := stmt.Exec(db)

	assert.Error(t, err, errorStr)
}

func getFullPath(relativePath string) string {
	goPath := os.Getenv("GOPATH")
	return filepath.Join(goPath, "src/github.com/go-jet/jet/tests", relativePath)
}

func PrintJson(v interface{}) {
	jsonText, _ := json.MarshalIndent(v, "", "\t")
	fmt.Println(string(jsonText))
}

func AssertJSON(t *testing.T, data interface{}, expectedJSON string) {
	jsonData, err := json.MarshalIndent(data, "", "\t")
	assert.NilError(t, err)

	assert.Equal(t, "\n"+string(jsonData)+"\n", expectedJSON)
}

func SaveJsonFile(v interface{}, testRelativePath string) {
	jsonText, _ := json.MarshalIndent(v, "", "\t")

	filePath := getFullPath(testRelativePath)
	err := ioutil.WriteFile(filePath, jsonText, 0644)

	if err != nil {
		panic(err)
	}
}

func AssertJSONFile(t *testing.T, data interface{}, testRelativePath string) {

	filePath := getFullPath(testRelativePath)
	fileJSONData, err := ioutil.ReadFile(filePath)
	assert.NilError(t, err)

	if runtime.GOOS == "windows" {
		fileJSONData = bytes.Replace(fileJSONData, []byte("\r\n"), []byte("\n"), -1)
	}

	jsonData, err := json.MarshalIndent(data, "", "\t")
	assert.NilError(t, err)

	assert.Assert(t, string(fileJSONData) == string(jsonData))
	//assert.DeepEqual(t, string(fileJSONData), string(jsonData))
}

func AssertStatementSql(t *testing.T, query jet.Statement, expectedQuery string, expectedArgs ...interface{}) {
	queryStr, args, err := query.Sql()
	assert.NilError(t, err)
	assert.Equal(t, queryStr, expectedQuery)

	if len(expectedArgs) == 0 {
		return
	}
	assert.DeepEqual(t, args, expectedArgs)
}

func AssertDebugStatementSql(t *testing.T, query jet.Statement, expectedQuery string, expectedArgs ...interface{}) {
	_, args, err := query.Sql()
	assert.NilError(t, err)
	//assert.Equal(t, queryStr, expectedQuery)
	if len(expectedArgs) > 0 {
		assert.DeepEqual(t, args, expectedArgs)
	}

	debuqSql, err := query.DebugSql()

	assert.NilError(t, err)

	assert.Equal(t, debuqSql, expectedQuery)
}

func Date(t string) *time.Time {
	newTime, err := time.Parse("2006-01-02", t)

	if err != nil {
		panic(err)
	}

	return &newTime
}

func TimestampWithoutTimeZone(t string, precision int) *time.Time {

	precisionStr := ""

	if precision > 0 {
		precisionStr = "." + strings.Repeat("9", precision)
	}

	newTime, err := time.Parse("2006-01-02 15:04:05"+precisionStr+" +0000", t+" +0000")

	if err != nil {
		panic(err)
	}

	return &newTime
}

func TimeWithoutTimeZone(t string) *time.Time {
	newTime, err := time.Parse("15:04:05", t)

	if err != nil {
		panic(err)
	}

	return &newTime
}

func TimeWithTimeZone(t string) *time.Time {
	newTimez, err := time.Parse("15:04:05 -0700", t)

	if err != nil {
		panic(err)
	}

	return &newTimez
}

func TimestampWithTimeZone(t string, precision int) *time.Time {

	precisionStr := ""

	if precision > 0 {
		precisionStr = "." + strings.Repeat("9", precision)
	}

	newTime, err := time.Parse("2006-01-02 15:04:05"+precisionStr+" -0700 MST", t)

	if err != nil {
		panic(err)
	}

	return &newTime
}
