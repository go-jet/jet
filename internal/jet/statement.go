package jet

import (
	"context"
	"database/sql"
	"github.com/go-jet/jet/v2/qrm"
	"time"
)

// Statement is a common interface for all SQL statements, including SELECT, SELECT_JSON_ARR, SELECT_JSON_OBJ, INSERT,
// UPDATE, DELETE, and LOCK.
type Statement interface {
	// Sql returns a parameterized SQL query along with its list of arguments.
	Sql() (query string, args []interface{})

	// DebugSql returns a debug-friendly SQL query where all parameterized placeholders
	// are replaced with their respective argument string representations.
	//
	// Warning: This method should only be used for debugging purposes.
	//   Do not use it in production, as it may lead to security risks such as SQL injection.
	DebugSql() (query string)

	// Query delegates call to QueryContext using context.Background() as parameter.
	Query(db qrm.Queryable, destination interface{}) error

	// QueryContext executes the statement with the provided context over a database connection or transaction (`db`),
	// and stores the retrieved row results in the given destination.
	//
	// For statements of type SELECT, INSERT, UPDATE, or DELETE, the destination must be a pointer to either a struct or a slice.
	// For SELECT_JSON_ARR statements, the destination must be a pointer to a slice of structs or a pointer to []map[string]any.
	// For SELECT_JSON_OBJ statements, the destination must be a pointer to a struct or a pointer to map[string]any.
	//
	// If the destination is a pointer to a struct and the query returns no rows, QueryContext returns qrm.ErrNoRows.
	QueryContext(ctx context.Context, db qrm.Queryable, destination interface{}) error

	// Exec delegates call to ExecContext using context.Background() as parameter.
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

// statementInterfaceImpl struct
type statementInterfaceImpl struct {
	dialect       Dialect
	statementType StatementType
	root          SerializerStatement
}

func (s *statementInterfaceImpl) Sql() (query string, args []interface{}) {

	queryData := &SQLBuilder{Dialect: s.dialect}

	s.root.serialize(s.statementType, queryData, NoWrap)

	query, args = queryData.finalize()
	return
}

func (s *statementInterfaceImpl) DebugSql() (query string) {
	sqlBuilder := &SQLBuilder{Dialect: s.dialect, Debug: true}

	s.root.serialize(s.statementType, sqlBuilder, NoWrap)

	query, _ = sqlBuilder.finalize()
	return
}

func (s *statementInterfaceImpl) Query(db qrm.Queryable, destination interface{}) error {
	return s.QueryContext(context.Background(), db, destination)
}

func (s *statementInterfaceImpl) QueryContext(ctx context.Context, db qrm.Queryable, destination interface{}) error {
	return s.query(ctx, func(query string, args []interface{}) (int64, error) {
		switch s.statementType {
		case SelectJsonObjStatementType:
			return qrm.QueryJsonObj(ctx, db, query, args, destination)
		case SelectJsonArrStatementType:
			return qrm.QueryJsonArr(ctx, db, query, args, destination)
		default:
			return qrm.Query(ctx, db, query, args, destination)
		}
	})
}

func (s *statementInterfaceImpl) query(
	ctx context.Context,
	queryFunc func(query string, args []interface{}) (int64, error),
) error {
	query, args := s.Sql()

	callLogger(ctx, s)

	var rowsProcessed int64
	var err error

	duration := duration(func() {
		rowsProcessed, err = queryFunc(query, args)
	})

	callQueryLoggerFunc(ctx, QueryInfo{
		Statement:     s,
		RowsProcessed: rowsProcessed,
		Duration:      duration,
		Err:           err,
	})

	return err
}

func (s *statementInterfaceImpl) Exec(db qrm.Executable) (res sql.Result, err error) {
	return s.ExecContext(context.Background(), db)
}

func (s *statementInterfaceImpl) ExecContext(ctx context.Context, db qrm.Executable) (res sql.Result, err error) {
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

func (s *statementInterfaceImpl) Rows(ctx context.Context, db qrm.Queryable) (*Rows, error) {
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
func NewExpressionStatementImpl(Dialect Dialect,
	statementType StatementType,
	root ExpressionStatement,
	clauses ...Clause) ExpressionStatement {

	return &expressionStatementImpl{
		ExpressionInterfaceImpl{Root: root},
		statementImpl{
			statementInterfaceImpl: statementInterfaceImpl{
				root:          root,
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

func (e *expressionStatementImpl) serializeForRowToJsonProjection(statement StatementType, out *SQLBuilder) {
	panic("jet: SELECT JSON statements need to be aliased when used as a projection.")
}

// NewStatementImpl creates new statementImpl
func NewStatementImpl(Dialect Dialect, statementType StatementType, root SerializerStatement, clauses ...Clause) SerializerStatement {
	return &statementImpl{
		statementInterfaceImpl: statementInterfaceImpl{
			root:          root,
			dialect:       Dialect,
			statementType: statementType,
		},
		Clauses: clauses,
	}
}

type statementImpl struct {
	statementInterfaceImpl

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
