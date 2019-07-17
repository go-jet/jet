package jet

import (
	"context"
	"database/sql"
	"github.com/go-jet/jet/execution"
	"strconv"
	"strings"
)

type Statement interface {
	// Sql returns parametrized sql query with list of arguments.
	// err is returned if statement is not composed correctly
	Sql() (query string, args []interface{}, err error)
	// DebugSql returns debug query where every parametrized placeholder is replaced with its argument.
	// Do not use it in production. Use it only for debug purposes.
	// err is returned if statement is not composed correctly
	DebugSql() (query string, err error)

	// Query executes statement over database connection db and stores row result in destination.
	// Destination can be arbitrary structure
	Query(db execution.DB, destination interface{}) error
	// QueryContext executes statement with a context over database connection db and stores row result in destination.
	// Destination can be of arbitrary structure
	QueryContext(db execution.DB, context context.Context, destination interface{}) error

	//Exec executes statement over db connection without returning any rows.
	Exec(db execution.DB) (sql.Result, error)
	//Exec executes statement with context over db connection without returning any rows.
	ExecContext(db execution.DB, context context.Context) (sql.Result, error)
}

func debugSql(statement Statement) (string, error) {
	sqlQuery, args, err := statement.Sql()

	if err != nil {
		return "", err
	}

	debugSqlQuery := sqlQuery

	for i, arg := range args {
		argPlaceholder := "$" + strconv.Itoa(i+1)
		debugSqlQuery = strings.Replace(debugSqlQuery, argPlaceholder, ArgToString(arg), 1)
	}

	return debugSqlQuery, nil
}

func query(statement Statement, db execution.DB, destination interface{}) error {
	query, args, err := statement.Sql()

	if err != nil {
		return err
	}

	return execution.Query(db, context.Background(), query, args, destination)
}

func queryContext(statement Statement, db execution.DB, context context.Context, destination interface{}) error {
	query, args, err := statement.Sql()

	if err != nil {
		return err
	}

	return execution.Query(db, context, query, args, destination)
}

func exec(statement Statement, db execution.DB) (res sql.Result, err error) {
	query, args, err := statement.Sql()

	if err != nil {
		return
	}

	return db.Exec(query, args...)
}

func execContext(statement Statement, db execution.DB, context context.Context) (res sql.Result, err error) {
	query, args, err := statement.Sql()

	if err != nil {
		return
	}

	return db.ExecContext(context, query, args...)
}
