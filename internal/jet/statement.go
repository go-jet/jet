package jet

import (
	"context"
	"database/sql"
	"errors"
	"github.com/go-jet/jet/execution"
)

//Statement is common interface for all statements(SELECT, INSERT, UPDATE, DELETE, LOCK)
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
	dialect       Dialect
	statementType StatementType
	parent        SerializerStatement
}

func (s *SerializerStatementInterfaceImpl) Sql() (query string, args []interface{}, err error) {

	queryData := &SqlBuilder{Dialect: s.dialect}

	err = s.parent.serialize(s.statementType, queryData, noWrap)

	if err != nil {
		return "", nil, err
	}

	query, args = queryData.finalize()

	return
}

func (s *SerializerStatementInterfaceImpl) DebugSql() (query string, err error) {
	sqlBuilder := &SqlBuilder{Dialect: s.dialect, debug: true}

	err = s.parent.serialize(s.statementType, sqlBuilder, noWrap)

	if err != nil {
		return "", err
	}

	query, _ = sqlBuilder.finalize()

	return
}

func (s *SerializerStatementInterfaceImpl) Query(db execution.DB, destination interface{}) error {
	query, args, err := s.Sql()

	if err != nil {
		return err
	}

	return execution.Query(context.Background(), db, query, args, destination)
}

func (s *SerializerStatementInterfaceImpl) QueryContext(context context.Context, db execution.DB, destination interface{}) error {
	query, args, err := s.Sql()

	if err != nil {
		return err
	}

	return execution.Query(context, db, query, args, destination)
}

func (s *SerializerStatementInterfaceImpl) Exec(db execution.DB) (res sql.Result, err error) {
	query, args, err := s.Sql()

	if err != nil {
		return
	}

	return db.Exec(query, args...)
}

func (s *SerializerStatementInterfaceImpl) ExecContext(context context.Context, db execution.DB) (res sql.Result, err error) {
	query, args, err := s.Sql()

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

		out.IncreaseIdent()
	}

	for _, clause := range s.Clauses {
		err := clause.Serialize(statement, out)

		if err != nil {
			return err
		}
	}

	if !contains(options, noWrap) {
		out.DecreaseIdent()
		out.NewLine()
		out.WriteString(")")
	}

	return nil
}
