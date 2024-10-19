package jet

import (
	"context"
	"database/sql"
	"github.com/go-jet/jet/v2/qrm"
	"time"
)

// Statement is common interface for all statements(SELECT, INSERT, UPDATE, DELETE, LOCK)
type Statement interface {
	// Sql returns parametrized sql query with list of arguments.
	Sql() (query string, args []interface{})
	// DebugSql returns debug query where every parametrized placeholder is replaced with its argument string representation.
	// Do not use it in production. Use it only for debug purposes.
	DebugSql() (query string)
	// Query executes statement over database connection/transaction db and stores row results in destination.
	// Destination can be either pointer to struct or pointer to a slice.
	// If destination is pointer to struct and query result set is empty, method returns qrm.ErrNoRows.
	Query(db qrm.Queryable, destination interface{}) error
	// QueryContext executes statement with a context over database connection/transaction db and stores row result in destination.
	// Destination can be either pointer to struct or pointer to a slice.
	// If destination is pointer to struct and query result set is empty, method returns qrm.ErrNoRows.
	QueryContext(ctx context.Context, db qrm.Queryable, destination interface{}) error
	// Exec executes statement over db connection/transaction without returning any rows.
	Exec(db qrm.Executable) (sql.Result, error)
	// ExecContext executes statement with context over db connection/transaction without returning any rows.
	ExecContext(ctx context.Context, db qrm.Executable) (sql.Result, error)
	// Rows executes statements over db connection/transaction and returns rows
	Rows(ctx context.Context, db qrm.Queryable) (*Rows, error)
}

// Rows wraps sql.Rows type with a support for query result mapping
type Rows struct {
	*sql.Rows

	scanContext *qrm.ScanContext
}

// Scan will map the Row values into struct destination
func (r *Rows) Scan(destination interface{}) error {
	return qrm.ScanOneRowToDest(r.scanContext, r.Rows, destination)
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

// SerializerHasProjections interface is combination of Serializer and HasProjections interface
type SerializerHasProjections interface {
	Serializer
	HasProjections
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

func (s *serializerStatementInterfaceImpl) Query(db qrm.Queryable, destination interface{}) error {
	return s.QueryContext(context.Background(), db, destination)
}

func (s *serializerStatementInterfaceImpl) QueryContext(ctx context.Context, db qrm.Queryable, destination interface{}) error {
	query, args := s.Sql()

	callLogger(ctx, s)

	var rowsProcessed int64
	var err error

	duration := duration(func() {
		rowsProcessed, err = qrm.Query(ctx, db, query, args, destination)
	})

	callQueryLoggerFunc(ctx, QueryInfo{
		Statement:     s,
		RowsProcessed: rowsProcessed,
		Duration:      duration,
		Err:           err,
	})

	return err
}

func (s *serializerStatementInterfaceImpl) Exec(db qrm.Executable) (res sql.Result, err error) {
	return s.ExecContext(context.Background(), db)
}

func (s *serializerStatementInterfaceImpl) ExecContext(ctx context.Context, db qrm.Executable) (res sql.Result, err error) {
	query, args := s.Sql()

	callLogger(ctx, s)

	duration := duration(func() {
		res, err = db.ExecContext(ctx, query, args...)
	})

	var rowsAffected int64

	if err == nil {
		rowsAffected, _ = res.RowsAffected()
	}

	callQueryLoggerFunc(ctx, QueryInfo{
		Statement:     s,
		RowsProcessed: rowsAffected,
		Duration:      duration,
		Err:           err,
	})

	return res, err
}

func (s *serializerStatementInterfaceImpl) Rows(ctx context.Context, db qrm.Queryable) (*Rows, error) {
	query, args := s.Sql()

	callLogger(ctx, s)

	var rows *sql.Rows
	var err error

	duration := duration(func() {
		rows, err = db.QueryContext(ctx, query, args...)
	})

	callQueryLoggerFunc(ctx, QueryInfo{
		Statement: s,
		Duration:  duration,
		Err:       err,
	})

	if err != nil {
		return nil, err
	}

	scanContext, err := qrm.NewScanContext(rows)

	if err != nil {
		return nil, err
	}

	return &Rows{
		Rows:        rows,
		scanContext: scanContext,
	}, nil
}

func duration(f func()) time.Duration {
	start := time.Now()

	f()

	return time.Since(start)
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

	if contains(options, Ident) {
		out.IncreaseIdent()
	}

	for _, clause := range s.Clauses {
		clause.Serialize(s.statementType, out, FallTrough(options)...)
	}

	if contains(options, Ident) {
		out.DecreaseIdent()
		out.NewLine()
	}

	if !contains(options, NoWrap) {
		out.DecreaseIdent()
		out.NewLine()
		out.WriteString(")")
	}
}
