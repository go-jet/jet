package jet

import (
	"context"
	"database/sql"
	"github.com/go-jet/jet/execution"
)

//Statement is common interface for all statements(SELECT, INSERT, UPDATE, DELETE, LOCK)
type Statement interface {
	// Sql returns parametrized sql query with list of arguments.
	Sql() (query string, args []interface{})
	// DebugSql returns debug query where every parametrized placeholder is replaced with its argument.
	// Do not use it in production. Use it only for debug purposes.
	DebugSql() (query string)

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
	projections() ProjectionList
}

type SerializerStatementInterfaceImpl struct {
	dialect       Dialect
	statementType StatementType
	parent        SerializerStatement
}

func (s *SerializerStatementInterfaceImpl) Sql() (query string, args []interface{}) {

	queryData := &SqlBuilder{Dialect: s.dialect}

	s.parent.serialize(s.statementType, queryData, noWrap)

	query, args = queryData.finalize()
	return
}

func (s *SerializerStatementInterfaceImpl) DebugSql() (query string) {
	sqlBuilder := &SqlBuilder{Dialect: s.dialect, debug: true}

	s.parent.serialize(s.statementType, sqlBuilder, noWrap)

	query, _ = sqlBuilder.finalize()
	return
}

func (s *SerializerStatementInterfaceImpl) Query(db execution.DB, destination interface{}) error {
	query, args := s.Sql()

	return execution.Query(context.Background(), db, query, args, destination)
}

func (s *SerializerStatementInterfaceImpl) QueryContext(context context.Context, db execution.DB, destination interface{}) error {
	query, args := s.Sql()

	return execution.Query(context, db, query, args, destination)
}

func (s *SerializerStatementInterfaceImpl) Exec(db execution.DB) (res sql.Result, err error) {
	query, args := s.Sql()
	return db.Exec(query, args...)
}

func (s *SerializerStatementInterfaceImpl) ExecContext(context context.Context, db execution.DB) (res sql.Result, err error) {
	query, args := s.Sql()

	return db.ExecContext(context, query, args...)
}

type ExpressionStatementImpl struct {
	ExpressionInterfaceImpl
	StatementImpl
}

func (s *ExpressionStatementImpl) serializeForProjection(statement StatementType, out *SqlBuilder) {
	s.serialize(statement, out)
}

func NewStatementImpl(Dialect Dialect, statementType StatementType, parent SerializerStatement, clauses ...Clause) StatementImpl {
	return StatementImpl{
		SerializerStatementInterfaceImpl: SerializerStatementInterfaceImpl{
			parent:        parent,
			dialect:       Dialect,
			statementType: statementType,
		},
		Clauses: clauses,
	}
}

type StatementImpl struct {
	SerializerStatementInterfaceImpl

	Clauses []Clause
}

func (s *StatementImpl) projections() ProjectionList {
	for _, clause := range s.Clauses {
		if selectClause, ok := clause.(ClauseWithProjections); ok {
			return selectClause.projections()
		}
	}

	return nil
}

func (s *StatementImpl) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) {

	if !contains(options, noWrap) {
		out.WriteString("(")
		out.IncreaseIdent()
	}

	for _, clause := range s.Clauses {
		clause.Serialize(statement, out)
	}

	if !contains(options, noWrap) {
		out.DecreaseIdent()
		out.NewLine()
		out.WriteString(")")
	}
}
