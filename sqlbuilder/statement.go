package sqlbuilder

import (
	"context"
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
	QueryContext(db execution.Db, context context.Context, destination interface{}) error

	Exec(db execution.Db) (sql.Result, error)
	ExecContext(db execution.Db, context context.Context) (sql.Result, error)
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

func Query(statement Statement, db execution.Db, destination interface{}) error {
	query, args, err := statement.Sql()

	if err != nil {
		return err
	}

	return execution.Query(db, context.Background(), query, args, destination)
}

func QueryContext(statement Statement, db execution.Db, context context.Context, destination interface{}) error {
	query, args, err := statement.Sql()

	if err != nil {
		return err
	}

	return execution.Query(db, context, query, args, destination)
}

func Exec(statement Statement, db execution.Db) (res sql.Result, err error) {
	query, args, err := statement.Sql()

	if err != nil {
		return
	}

	return db.Exec(query, args...)
}

func ExecContext(statement Statement, db execution.Db, context context.Context) (res sql.Result, err error) {
	query, args, err := statement.Sql()

	if err != nil {
		return
	}

	return db.ExecContext(context, query, args...)
}
