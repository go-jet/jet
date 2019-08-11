package jet

import (
	"context"
	"database/sql"
	"errors"
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

type SerializerStatement interface {
	Serializer
	Statement
}

type StatementWithProjections interface {
	Statement
	HasProjections
	Serializer
}

type HasProjections interface {
	projections() []Projection
}

type SerializerStatementInterfaceImpl struct {
	noOpVisitorImpl
	Parent        SerializerStatement
	Dialect       Dialect
	StatementType StatementType
}

func (s *SerializerStatementInterfaceImpl) Sql(dialect ...Dialect) (query string, args []interface{}, err error) {

	queryData := &SqlBuilder{Dialect: s.Dialect}

	err = s.Parent.serialize(s.StatementType, queryData, noWrap)

	if err != nil {
		return "", nil, err
	}

	query, args = queryData.finalize()

	return
}

func (s *SerializerStatementInterfaceImpl) DebugSql(dialect ...Dialect) (query string, err error) {
	return debugSql(s.Parent, s.Dialect)
}

func (s *SerializerStatementInterfaceImpl) Query(db execution.DB, destination interface{}) error {
	return query(s.Parent, db, destination)
}

func (s *SerializerStatementInterfaceImpl) QueryContext(context context.Context, db execution.DB, destination interface{}) error {
	return queryContext(context, s.Parent, db, destination)
}

func (s *SerializerStatementInterfaceImpl) Exec(db execution.DB) (res sql.Result, err error) {
	return exec(s.Parent, db)
}

func (s *SerializerStatementInterfaceImpl) ExecContext(context context.Context, db execution.DB) (res sql.Result, err error) {
	return execContext(context, s.Parent, db)
}

func debugSql(statement Statement, overrideDialect ...Dialect) (string, error) {
	dialect := detectDialect(statement, overrideDialect...)
	sqlQuery, args, err := statement.Sql(dialect)

	if err != nil {
		return "", err
	}

	//debugSQLQuery := sqlQuery
	//
	//for i, arg := range args {
	//	argPlaceholder := dialect.ArgumentPlaceholder()(i + 1)
	//	debugSQLQuery = strings.Replace(debugSQLQuery, argPlaceholder, argToString(arg), 1)
	//}
	//
	//return debugSQLQuery, nil
	return queryStringToDebugString(sqlQuery, args, dialect), nil
}

func queryStringToDebugString(sqlQuery string, args []interface{}, dialect Dialect) string {
	debugSQLQuery := sqlQuery

	for i, arg := range args {
		argPlaceholder := dialect.ArgumentPlaceholder()(i + 1)
		debugSQLQuery = strings.Replace(debugSQLQuery, argPlaceholder, argToString(arg), 1)
	}

	return debugSQLQuery
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

type ExpressionStatementImpl struct {
	ExpressionInterfaceImpl
	StatementImpl
}

func (s *ExpressionStatementImpl) serializeForProjection(statement StatementType, out *SqlBuilder) error {
	return s.serialize(statement, out)
}

func NewStatementImpl(Dialect Dialect, statementType StatementType, parent SerializerStatement, clauses ...Clause) StatementImpl {
	return StatementImpl{
		SerializerStatementInterfaceImpl: SerializerStatementInterfaceImpl{
			Parent:        parent,
			Dialect:       Dialect,
			StatementType: statementType,
		},
		Clauses: clauses,
	}
}

type StatementImpl struct {
	SerializerStatementInterfaceImpl
	acceptsVisitor

	Clauses []Clause
}

func (s *StatementImpl) projections() []Projection {
	for _, clause := range s.Clauses {
		if selectClause, ok := clause.(ClauseWithProjections); ok {
			return selectClause.projections()
		}
	}

	return nil
}

func (s *StatementImpl) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) error {
	if s == nil {
		return errors.New("jet: Select expression is nil. ")
	}

	if !contains(options, noWrap) {
		out.WriteString("(")

		out.increaseIdent()
	}

	for _, clause := range s.Clauses {
		err := clause.Serialize(statement, out)

		if err != nil {
			return err
		}
	}

	if !contains(options, noWrap) {
		out.decreaseIdent()
		out.NewLine()
		out.WriteString(")")
	}

	return nil
}
