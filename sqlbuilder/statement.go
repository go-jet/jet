package sqlbuilder

import (
	"database/sql"
	"github.com/sub0zero/go-sqlbuilder/types"
	"strconv"
	"strings"
)

type Statement interface {
	// String returns generated SQL as string.
	Sql() (query string, args []interface{}, err error)

	DebugSql() (query string, err error)

	Query(db types.Db, destination interface{}) error
	Execute(db types.Db) (sql.Result, error)
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
