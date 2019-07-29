package testutils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/go-jet/jet"
	"gotest.tools/assert"
	"io/ioutil"
	"runtime"
	"strings"
	"testing"
	"time"
)

func JsonPrint(v interface{}) {
	jsonText, _ := json.MarshalIndent(v, "", "\t")
	fmt.Println(string(jsonText))
}

func JsonSave(path string, v interface{}) {
	jsonText, _ := json.MarshalIndent(v, "", "\t")

	err := ioutil.WriteFile(path, jsonText, 0644)

	if err != nil {
		panic(err)
	}
}

func AssertJSON(t *testing.T, data interface{}, expectedJSON string) {
	jsonData, err := json.MarshalIndent(data, "", "\t")
	assert.NilError(t, err)

	assert.Equal(t, "\n"+string(jsonData)+"\n", expectedJSON)
}

func AssertJSONFile(t *testing.T, jsonFilePath string, data interface{}) {
	fileJSONData, err := ioutil.ReadFile(jsonFilePath)
	assert.NilError(t, err)

	if runtime.GOOS == "windows" {
		fileJSONData = bytes.Replace(fileJSONData, []byte("\r\n"), []byte("\n"), -1)
	}

	jsonData, err := json.MarshalIndent(data, "", "\t")
	assert.NilError(t, err)

	assert.Assert(t, string(fileJSONData) == string(jsonData))
	//assert.Equal(t, string(fileJSONData), string(jsonData))
}

func AssertStatementSql(t *testing.T, query jet.Statement, expectedQuery string, expectedArgs ...interface{}) {
	_, args, err := query.Sql()
	assert.NilError(t, err)
	//assert.Equal(t, queryStr, expectedQuery)
	assert.DeepEqual(t, args, expectedArgs)

	debuqSql, err := query.DebugSql()

	assert.NilError(t, err)
	assert.Equal(t, debuqSql, expectedQuery)
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
