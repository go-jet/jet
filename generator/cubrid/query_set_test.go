package cubrid

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/go-jet/jet/v2/generator/metadata"
)

// errDriver always fails on Prepare (makes all SQL queries fail).
type errDriver struct{}

func (d *errDriver) Open(_ string) (driver.Conn, error) { return &errConn{}, nil }

type errConn struct{}

func (c *errConn) Prepare(_ string) (driver.Stmt, error) {
	return nil, errors.New("mock db error")
}
func (c *errConn) Close() error          { return nil }
func (c *errConn) Begin() (driver.Tx, error) { return nil, errors.New("mock db error") }

// partialDriver returns one table name from ListTables but fails on ListColumns.
type partialDriver struct{}

func (d *partialDriver) Open(_ string) (driver.Conn, error) { return &partialConn{}, nil }

type partialConn struct{}

func (c *partialConn) Prepare(query string) (driver.Stmt, error) {
	if strings.Contains(query, "class_type = 'CLASS'") {
		return &oneRowStmt{row: []driver.Value{"mock_table"}}, nil
	}
	return nil, errors.New("mock column list error")
}
func (c *partialConn) Close() error          { return nil }
func (c *partialConn) Begin() (driver.Tx, error) { return nil, errors.New("mock tx error") }

type oneRowStmt struct {
	row     []driver.Value
	fetched bool
}

func (s *oneRowStmt) Close() error { return nil }
func (s *oneRowStmt) NumInput() int { return 0 }
func (s *oneRowStmt) Exec(_ []driver.Value) (driver.Result, error) {
	return nil, errors.New("mock exec error")
}
func (s *oneRowStmt) Query(_ []driver.Value) (driver.Rows, error) {
	return &oneRowRows{row: s.row}, nil
}

type oneRowRows struct {
	row     []driver.Value
	fetched bool
}

func (r *oneRowRows) Columns() []string { return []string{"class_name"} }
func (r *oneRowRows) Close() error      { return nil }
func (r *oneRowRows) Next(dest []driver.Value) error {
	if r.fetched {
		return io.EOF
	}
	r.fetched = true
	dest[0] = r.row[0]
	return nil
}

func init() {
	sql.Register("cubrid-mock-error", &errDriver{})
	sql.Register("cubrid-mock-partial", &partialDriver{})
}

func newErrDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("cubrid-mock-error", "test")
	if err != nil {
		t.Fatalf("failed to open mock db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func newPartialDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("cubrid-mock-partial", "test")
	if err != nil {
		t.Fatalf("failed to open partial mock db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestGetTablesMetaData_ListTablesError(t *testing.T) {
	db := newErrDB(t)
	qs := &cubridQuerySet{}

	_, err := qs.GetTablesMetaData(db, "testdb", metadata.BaseTable)
	if err == nil {
		t.Fatal("expected error from GetTablesMetaData with mock DB, got nil")
	}
}

func TestGetTablesMetaData_ListViewsError(t *testing.T) {
	db := newErrDB(t)
	qs := &cubridQuerySet{}

	_, err := qs.GetTablesMetaData(db, "testdb", metadata.ViewTable)
	if err == nil {
		t.Fatal("expected error from GetTablesMetaData(View) with mock DB, got nil")
	}
}

func TestGetEnumsMetaData_ReturnsNil(t *testing.T) {
	db := newErrDB(t)
	qs := &cubridQuerySet{}

	enums, err := qs.GetEnumsMetaData(db, "testdb")
	if err != nil {
		t.Fatalf("GetEnumsMetaData returned unexpected error: %v", err)
	}
	if enums != nil {
		t.Fatalf("GetEnumsMetaData should return nil, got %v", enums)
	}
}

func TestGetColumnMetaData_Error(t *testing.T) {
	db := newErrDB(t)

	_, err := getColumnMetaData(context.Background(), db, "some_table")
	if err == nil {
		t.Fatal("expected error from getColumnMetaData with mock DB, got nil")
	}
}

// TestGetTablesMetaData_ColumnError covers the error path when getColumnMetaData fails
// after ListTables succeeds (mock returns one table, then fails on ListColumns).
func TestGetTablesMetaData_ColumnError(t *testing.T) {
	db := newPartialDB(t)
	qs := &cubridQuerySet{}

	_, err := qs.GetTablesMetaData(db, "testdb", metadata.BaseTable)
	if err == nil {
		t.Fatal("expected error when getColumnMetaData fails, got nil")
	}
}
