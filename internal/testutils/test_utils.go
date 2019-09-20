package testutils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/go-jet/jet/execution"
	"github.com/go-jet/jet/internal/jet"
	"github.com/go-jet/jet/internal/utils"
	"gotest.tools/assert"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// AssertExec assert statement execution for successful execution and number of rows affected
func AssertExec(t *testing.T, stmt jet.Statement, db execution.DB, rowsAffected ...int64) {
	res, err := stmt.Exec(db)

	assert.NilError(t, err)
	rows, err := res.RowsAffected()
	assert.NilError(t, err)

	if len(rowsAffected) > 0 {
		assert.Equal(t, rows, rowsAffected[0])
	}
}

// AssertExecErr assert statement execution for failed execution with error string errorStr
func AssertExecErr(t *testing.T, stmt jet.Statement, db execution.DB, errorStr string) {
	_, err := stmt.Exec(db)

	assert.Error(t, err, errorStr)
}

func getFullPath(relativePath string) string {
	goPath := os.Getenv("GOPATH")
	return filepath.Join(goPath, "src/github.com/go-jet/jet/tests", relativePath)
}

// PrintJson print v as json
func PrintJson(v interface{}) {
	jsonText, _ := json.MarshalIndent(v, "", "\t")
	fmt.Println(string(jsonText))
}

// AssertJSON check if data json output is the same as expectedJSON
func AssertJSON(t *testing.T, data interface{}, expectedJSON string) {
	jsonData, err := json.MarshalIndent(data, "", "\t")
	assert.NilError(t, err)

	assert.Equal(t, "\n"+string(jsonData)+"\n", expectedJSON)
}

// SaveJSONFile saves v as json at testRelativePath
func SaveJSONFile(v interface{}, testRelativePath string) {
	jsonText, _ := json.MarshalIndent(v, "", "\t")

	filePath := getFullPath(testRelativePath)
	err := ioutil.WriteFile(filePath, jsonText, 0644)

	utils.PanicOnError(err)
}

// AssertJSONFile check if data json representation is the same as json at testRelativePath
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

// AssertStatementSql check if statement Sql() is the same as expectedQuery and expectedArgs
func AssertStatementSql(t *testing.T, query jet.Statement, expectedQuery string, expectedArgs ...interface{}) {
	queryStr, args := query.Sql()
	assert.Equal(t, queryStr, expectedQuery)

	if len(expectedArgs) == 0 {
		return
	}
	assert.DeepEqual(t, args, expectedArgs)
}

// AssertStatementSqlErr checks if statement Sql() panics with errorStr
func AssertStatementSqlErr(t *testing.T, stmt jet.Statement, errorStr string) {
	defer func() {
		r := recover()
		assert.Equal(t, r, errorStr)
	}()

	stmt.Sql()
}

// AssertDebugStatementSql check if statement Sql() is the same as expectedQuery
func AssertDebugStatementSql(t *testing.T, query jet.Statement, expectedQuery string, expectedArgs ...interface{}) {
	_, args := query.Sql()

	if len(expectedArgs) > 0 {
		assert.DeepEqual(t, args, expectedArgs)
	}

	debuqSql := query.DebugSql()
	assert.Equal(t, debuqSql, expectedQuery)
}

// AssertClauseSerialize checks if clause serialize produces expected query and args
func AssertClauseSerialize(t *testing.T, dialect jet.Dialect, clause jet.Serializer, query string, args ...interface{}) {
	out := jet.SQLBuilder{Dialect: dialect}
	jet.Serialize(clause, jet.SelectStatementType, &out)

	//fmt.Println(out.Buff.String())

	assert.DeepEqual(t, out.Buff.String(), query)

	if len(args) > 0 {
		assert.DeepEqual(t, out.Args, args)
	}
}

// AssertClauseSerializeErr check if clause serialize panics with errString
func AssertClauseSerializeErr(t *testing.T, dialect jet.Dialect, clause jet.Serializer, errString string) {
	defer func() {
		r := recover()
		assert.Equal(t, r, errString)
	}()

	out := jet.SQLBuilder{Dialect: dialect}
	jet.Serialize(clause, jet.SelectStatementType, &out)
}

// AssertProjectionSerialize check if projection serialize produces expected query and args
func AssertProjectionSerialize(t *testing.T, dialect jet.Dialect, projection jet.Projection, query string, args ...interface{}) {
	out := jet.SQLBuilder{Dialect: dialect}
	jet.SerializeForProjection(projection, jet.SelectStatementType, &out)

	assert.DeepEqual(t, out.Buff.String(), query)
	assert.DeepEqual(t, out.Args, args)
}

// AssertQueryPanicErr check if statement Query execution panics with error errString
func AssertQueryPanicErr(t *testing.T, stmt jet.Statement, db execution.DB, dest interface{}, errString string) {
	defer func() {
		r := recover()
		assert.Equal(t, r, errString)
	}()

	stmt.Query(db, dest)
}

// AssertFileContent check if file content at filePath contains expectedContent text.
func AssertFileContent(t *testing.T, filePath string, contentBegin string, expectedContent string) {
	enumFileData, err := ioutil.ReadFile(filePath)

	assert.NilError(t, err)

	beginIndex := bytes.Index(enumFileData, []byte(contentBegin))

	//fmt.Println("-"+string(enumFileData[beginIndex:])+"-")

	assert.DeepEqual(t, string(enumFileData[beginIndex:]), expectedContent)
}

// AssertFileNamesEqual check if all filesInfos are contained in fileNames
func AssertFileNamesEqual(t *testing.T, fileInfos []os.FileInfo, fileNames ...string) {
	assert.Equal(t, len(fileInfos), len(fileNames))

	fileNamesMap := map[string]bool{}

	for _, fileInfo := range fileInfos {
		fileNamesMap[fileInfo.Name()] = true
	}

	for _, fileName := range fileNames {
		assert.Assert(t, fileNamesMap[fileName], fileName+" does not exist.")
	}
}
