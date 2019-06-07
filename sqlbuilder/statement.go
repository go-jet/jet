package sqlbuilder

import (
	"database/sql"
	"github.com/go-jet/jet/sqlbuilder/execution"
	"strconv"
	"strings"
)

type Statement interface {
	// String returns generated SQL as string.
	Sql() (query string, args []interface{}, err error)

	DebugSql() (query string, err error)

	Query(db execution.Db, destination interface{}) error
	Execute(db execution.Db) (sql.Result, error)
}

func DebugSql(statement Statement) (string, error) {
	sql, args, err := statement.Sql()

	if err != nil {
		return "", err
	}

	debugSql := sql

	for i, arg := range args {
		argPlaceholder := "$" + strconv.Itoa(i+1)
		debugSql = strings.Replace(debugSql, argPlaceholder, ArgToString(arg), 1)
	}

	return debugSql, nil
}
