package jet

import (
	"context"
	"database/sql"
	"github.com/go-jet/jet/v2/qrm"
)

//Statement is common interface for all statements(SELECT, INSERT, UPDATE, DELETE, LOCK)
type Statement interface {
	// Sql returns parametrized sql query with list of arguments.
	Sql() (query string, args []interface{})
	// DebugSql returns debug query where every parametrized placeholder is replaced with its argument.
	// Do not use it in production. Use it only for debug purposes.
	DebugSql() (query string)
	// Query executes statement over database connection/transaction db and stores row result in destination.
	// Destination can be either pointer to struct or pointer to a slice.
	// If destination is pointer to struct and query result set is empty, method returns qrm.ErrNoRows.
	Query(db qrm.DB, destination interface{}) error
	// QueryContext executes statement with a context over database connection/transaction db and stores row result in destination.
	// Destination can be either pointer to struct or pointer to a slice.
	// If destination is pointer to struct and query result set is empty, method returns qrm.ErrNoRows.
	QueryContext(ctx context.Context, db qrm.DB, destination interface{}) error
	//Exec executes statement over db connection/transaction without returning any rows.
	Exec(db qrm.DB) (sql.Result, error)
	//Exec executes statement with context over db connection/transaction without returning any rows.
	ExecContext(ctx context.Context, db qrm.DB) (sql.Result, error)
	// Rows executes statements over db connection/transaction and returns rows
	Rows(ctx context.Context, db qrm.DB) (*Rows, error)
}

// Rows wraps sql.Rows type to add query result mapping for Scan method
type Rows struct {
	*sql.Rows
}

// Scan will map the Row values into struct destination
func (r *Rows) Scan(destination interface{}) error {
	return qrm.ScanOneRowToDest(r.Rows, destination)
}

// SerializerStatement interface
type SerializerStatement interface {
	Serializer
	Statement
	HasProjections
}

// HasProjections interface
type HasProjections interface {
	projections() ProjectionList
}

// serializerStatementInterfaceImpl struct
type serializerStatementInterfaceImpl struct {
	dialect       Dialect
	statementType StatementType
	parent        SerializerStatement
}

func (s *serializerStatementInterfaceImpl) Sql() (query string, args []interface{}) {

	queryData := &SQLBuilder{Dialect: s.dialect}

	s.parent.serialize(s.statementType, queryData, NoWrap)

	query, args = queryData.finalize()
	return
}

func (s *serializerStatementInterfaceImpl) DebugSql() (query string) {
	sqlBuilder := &SQLBuilder{Dialect: s.dialect, Debug: true}

	s.parent.serialize(s.statementType, sqlBuilder, NoWrap)

	query, _ = sqlBuilder.finalize()
	return
}

func (s *serializerStatementInterfaceImpl) Query(db qrm.DB, destination interface{}) error {
	query, args := s.Sql()
	ctx := context.Background()

	callLogger(ctx, s)

	return qrm.Query(ctx, db, query, args, destination)
}

func (s *serializerStatementInterfaceImpl) QueryContext(ctx context.Context, db qrm.DB, destination interface{}) error {
	query, args := s.Sql()

	callLogger(ctx, s)

	return qrm.Query(ctx, db, query, args, destination)
}

func (s *serializerStatementInterfaceImpl) Exec(db qrm.DB) (res sql.Result, err error) {
	query, args := s.Sql()

	callLogger(context.Background(), s)

	return db.Exec(query, args...)
}

func (s *serializerStatementInterfaceImpl) ExecContext(ctx context.Context, db qrm.DB) (res sql.Result, err error) {
	query, args := s.Sql()

	callLogger(ctx, s)

	return db.ExecContext(ctx, query, args...)
}

func (s *serializerStatementInterfaceImpl) Rows(ctx context.Context, db qrm.DB) (*Rows, error) {
	query, args := s.Sql()

	callLogger(ctx, s)

	rows, err := db.QueryContext(ctx, query, args...)

	if err != nil {
		return nil, err
	}

	return &Rows{rows}, nil
}

func callLogger(ctx context.Context, statement Statement) {
	if logger != nil {
		logger(ctx, statement)
	}
}

// ExpressionStatement interfacess
type ExpressionStatement interface {
	Expression
	Statement
	HasProjections
}

// NewExpressionStatementImpl creates new expression statement
func NewExpressionStatementImpl(Dialect Dialect, statementType StatementType, parent ExpressionStatement, clauses ...Clause) ExpressionStatement {
	return &expressionStatementImpl{
		ExpressionInterfaceImpl{Parent: parent},
		statementImpl{
			serializerStatementInterfaceImpl: serializerStatementInterfaceImpl{
				parent:        parent,
				dialect:       Dialect,
				statementType: statementType,
			},
			Clauses: clauses,
		},
	}
}

type expressionStatementImpl struct {
	ExpressionInterfaceImpl
	statementImpl
}

func (s *expressionStatementImpl) serializeForProjection(statement StatementType, out *SQLBuilder) {
	s.serialize(statement, out)
}

// NewStatementImpl creates new statementImpl
func NewStatementImpl(Dialect Dialect, statementType StatementType, parent SerializerStatement, clauses ...Clause) SerializerStatement {
	return &statementImpl{
		serializerStatementInterfaceImpl: serializerStatementInterfaceImpl{
			parent:        parent,
			dialect:       Dialect,
			statementType: statementType,
		},
		Clauses: clauses,
	}
}

type statementImpl struct {
	serializerStatementInterfaceImpl

	Clauses []Clause
}

func (s *statementImpl) projections() ProjectionList {
	for _, clause := range s.Clauses {
		if selectClause, ok := clause.(ClauseWithProjections); ok {
			return selectClause.Projections()
		}
	}

	return nil
}

func (s *statementImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	if !contains(options, NoWrap) {
		out.WriteString("(")
		out.IncreaseIdent()
	}

	for _, clause := range s.Clauses {
		clause.Serialize(statement, out, FallTrough(options)...)
	}

	if !contains(options, NoWrap) {
		out.DecreaseIdent()
		out.NewLine()
		out.WriteString(")")
	}
}
