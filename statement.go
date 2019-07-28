package jet

import (
	"context"
	"database/sql"
	"github.com/go-jet/jet/execution"
	"strings"
)

//Statement is common interface for all statements(SELECT, INSERT, UPDATE, DELETE, LOCK)
type Statement interface {
	acceptsVisitor
	// Sql returns parametrized sql query with list of arguments.
	// err is returned if statement is not composed correctly
	Sql(dialect ...Dialect) (query string, args []interface{}, err error)
	// DebugSql returns debug query where every parametrized placeholder is replaced with its argument.
	// Do not use it in production. Use it only for debug purposes.
	// err is returned if statement is not composed correctly
	DebugSql(dialect ...Dialect) (query string, err error)

	// Query executes statement over database connection db and stores row result in destination.
	// Destination can be arbitrary structure
	Query(db execution.DB, destination interface{}) error
	// QueryContext executes statement with a context over database connection db and stores row result in destination.
	// Destination can be of arbitrary structure
	QueryContext(context context.Context, db execution.DB, destination interface{}) error

	//Exec executes statement over db connection without returning any rows.
	Exec(db execution.DB) (sql.Result, error)
	//Exec executes statement with context over db connection without returning any rows.
	ExecContext(context context.Context, db execution.DB) (sql.Result, error)
}

func debugSql(statement Statement, overrideDialect ...Dialect) (string, error) {
	dialect := detectDialect(statement, overrideDialect...)
	sqlQuery, args, err := statement.Sql()

	if err != nil {
		return "", err
	}

	debugSQLQuery := sqlQuery

	for i, arg := range args {
		argPlaceholder := dialect.ArgumentPlaceholder(i + 1)
		debugSQLQuery = strings.Replace(debugSQLQuery, argPlaceholder, argToString(arg), 1)
	}

	return debugSQLQuery, nil
}

func query(statement Statement, db execution.DB, destination interface{}) error {
	query, args, err := statement.Sql()

	if err != nil {
		return err
	}

	return execution.Query(context.Background(), db, query, args, destination)
}

func queryContext(context context.Context, statement Statement, db execution.DB, destination interface{}) error {
	query, args, err := statement.Sql()

	if err != nil {
		return err
	}

	return execution.Query(context, db, query, args, destination)
}

func exec(statement Statement, db execution.DB) (res sql.Result, err error) {
	query, args, err := statement.Sql()

	if err != nil {
		return
	}

	return db.Exec(query, args...)
}

func execContext(context context.Context, statement Statement, db execution.DB) (res sql.Result, err error) {
	query, args, err := statement.Sql()

	if err != nil {
		return
	}

	return db.ExecContext(context, query, args...)
}
