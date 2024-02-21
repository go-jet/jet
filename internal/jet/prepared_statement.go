package jet

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/v2/qrm"
)

// PreparedStatement is jet wrapper over sql prepared statement
type PreparedStatement struct {
	stmt      *sql.Stmt
	statement Statement

	// cached values
	query string
	args  []interface{}
}

// Stmt returns a transaction-specific prepared statement from an existing statement.
func (s *PreparedStatement) Stmt(tx *sql.Tx) *PreparedStatement {
	return &PreparedStatement{
		stmt:      tx.Stmt(s.stmt),
		statement: s.statement,
		query:     s.query,
		args:      s.args,
	}
}

// DBPreparer interface
type DBPreparer interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

// Prepare on first call will create a new sql prepared statement from jet statement. On subsequent calls only
// jet statement arguments are extracted.
func (s *PreparedStatement) Prepare(ctx context.Context, db DBPreparer, stmt Statement) error {
	if s.stmt != nil {
		args := stmt.Args()

		if len(s.args) != len(args) {
			panic("previously prepared statement arguments does not match new statement arguments")
		}

		s.statement = stmt
		s.args = args
		// s.query is already set
		return nil
	}

	query, args := stmt.Sql()

	sqlStmt, err := db.PrepareContext(ctx, query)

	if err != nil {
		return fmt.Errorf("failed to create prepared statement: %w", err)
	}

	s.stmt = sqlStmt
	s.statement = stmt
	s.query = query
	s.args = args

	return nil
}

// Sql returns parametrized sql query with list of arguments.
func (s PreparedStatement) Sql() (query string, args []interface{}) {
	return s.query, s.args
}

// DebugSql returns debug query where every parametrized placeholder is replaced with its argument string representation.
// Do not use it in production. Use it only for debug purposes.
func (s PreparedStatement) DebugSql() string {
	return s.statement.DebugSql()
}

// Query executes a statement with a context over database connection/transaction db and store row result in destination.
// Destination can be either pointer to struct or pointer to a slice.
// If destination is pointer to struct a query result set is empty, method returns qrm.ErrNoRows.
func (s *PreparedStatement) Query(ctx context.Context, destination interface{}) error {
	callLogger(ctx, s.statement)

	var rowsProcessed int64
	var err error

	duration := duration(func() {
		rowsProcessed, err = qrm.QueryPreparedStatement(ctx, s.stmt, s.args, destination)
	})

	callQueryLoggerFunc(ctx, QueryInfo{
		Statement:     s.statement,
		RowsProcessed: rowsProcessed,
		Duration:      duration,
		Err:           err,
	})

	return err
}

// Exec executes statement with context over db connection/transaction without returning any rows.
func (s *PreparedStatement) Exec(ctx context.Context) (res sql.Result, err error) {
	callLogger(ctx, s.statement)

	duration := duration(func() {
		res, err = s.stmt.ExecContext(ctx, s.args...)
	})

	var rowsAffected int64

	if err == nil {
		rowsAffected, _ = res.RowsAffected()
	}

	callQueryLoggerFunc(ctx, QueryInfo{
		Statement:     s.statement,
		RowsProcessed: rowsAffected,
		Duration:      duration,
		Err:           err,
	})

	return res, err
}

// Rows executes statements over db connection/transaction and returns rows
func (s *PreparedStatement) Rows(ctx context.Context) (*Rows, error) {
	callLogger(ctx, s.statement)

	var rows *sql.Rows
	var err error

	duration := duration(func() {
		rows, err = s.stmt.QueryContext(ctx, s.args...)
	})

	callQueryLoggerFunc(ctx, QueryInfo{
		Statement: s.statement,
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

// Close will close sql prepared statement if a statement is set
func (s *PreparedStatement) Close() error {
	if s.stmt != nil {
		return s.stmt.Close()
	}

	return nil
}
