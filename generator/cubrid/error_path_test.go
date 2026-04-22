package cubrid

import (
	"database/sql"
	"database/sql/driver"
	"io"
	"strings"
	"testing"

	cubriddriver "github.com/search5/cubrid-go"
)

// TestGenerateDSN_MissingDBName covers the extractDBName error path in GenerateDSN.
func TestGenerateDSN_MissingDBName(t *testing.T) {
	err := GenerateDSN("cubrid://dba:@localhost:33000", t.TempDir())
	if err == nil {
		t.Fatal("expected error for DSN without database name, got nil")
	}
}

// TestGenerateDSN_ConnectionRefused covers the openConnection error path in GenerateDSN
// (valid DSN but unreachable host).
func TestGenerateDSN_ConnectionRefused(t *testing.T) {
	err := GenerateDSN("cubrid://dba:@127.0.0.1:1/testdb", t.TempDir())
	if err == nil {
		t.Fatal("expected error for unreachable host in GenerateDSN, got nil")
	}
}

// TestGeneratePool_EmptyDSN covers the NewPool error path in GeneratePool.
func TestGeneratePool_EmptyDSN(t *testing.T) {
	err := GeneratePool(cubriddriver.PoolConfig{DSN: ""}, "testdb", t.TempDir())
	if err == nil {
		t.Fatal("expected error for empty pool DSN, got nil")
	}
}

// TestGenerateHA_InvalidDSN covers the NewHACluster error path in GenerateHA.
func TestGenerateHA_InvalidDSN(t *testing.T) {
	err := GenerateHA(cubriddriver.HAConfig{DSN: ""}, "testdb", t.TempDir(), false)
	if err == nil {
		t.Fatal("expected error for empty HA DSN, got nil")
	}
}

// TestGenerate_ConnectionRefused covers the openConnection Ping error path.
func TestGenerate_ConnectionRefused(t *testing.T) {
	err := Generate(t.TempDir(), DBConnection{
		Host: "127.0.0.1", Port: 1,
		User: "dba", Password: "", DBName: "testdb",
	})
	if err == nil {
		t.Fatal("expected error for unreachable CUBRID host, got nil")
	}
}

// TestGenerateDB_SchemaError covers the metadata.GetSchema error path in GenerateDB.
// Uses the errDriver mock (registered in query_set_test.go) to make DB queries fail.
func TestGenerateDB_SchemaError(t *testing.T) {
	db, err := sql.Open("cubrid-mock-error", "test")
	if err != nil {
		t.Fatalf("failed to open mock db: %v", err)
	}
	defer db.Close()

	err = GenerateDB(db, "testdb", t.TempDir())
	if err == nil {
		t.Fatal("expected error when schema retrieval fails, got nil")
	}
}

// TestGenerateDB_ProcessSchemaError covers the template.ProcessSchema error path in GenerateDB.
// Uses schemaDriver which returns one table, then passes /dev/null as destDir so
// directory creation inside ProcessSchema fails.
func TestGenerateDB_ProcessSchemaError(t *testing.T) {
	db, err := sql.Open("cubrid-mock-schema", "test")
	if err != nil {
		t.Fatalf("failed to open schema mock db: %v", err)
	}
	defer db.Close()

	// /dev/null is a device file; creating subdirectories inside it fails.
	err = GenerateDB(db, "testdb", "/dev/null")
	if err == nil {
		t.Fatal("expected error when ProcessSchema fails due to invalid destDir, got nil")
	}
}

// emptyDriver returns empty result sets (simulates a DB with no tables).
type emptyDriver struct{}

func (d *emptyDriver) Open(_ string) (driver.Conn, error) { return &emptyConn{}, nil }

type emptyConn struct{}

func (c *emptyConn) Prepare(_ string) (driver.Stmt, error) { return &emptyStmt{}, nil }
func (c *emptyConn) Close() error                          { return nil }
func (c *emptyConn) Begin() (driver.Tx, error)             { return nil, nil }

type emptyStmt struct{}

func (s *emptyStmt) Close() error                                   { return nil }
func (s *emptyStmt) NumInput() int                                  { return 0 }
func (s *emptyStmt) Exec(_ []driver.Value) (driver.Result, error)  { return nil, nil }
func (s *emptyStmt) Query(_ []driver.Value) (driver.Rows, error)   { return &emptyRows{}, nil }

type emptyRows struct{}

func (r *emptyRows) Columns() []string             { return []string{"class_name"} }
func (r *emptyRows) Close() error                  { return nil }
func (r *emptyRows) Next(_ []driver.Value) error   { return io.EOF }

// schemaDriver returns one table ("mock_table") with one column ("id" integer),
// enabling non-empty schema metadata for ProcessSchema testing.
type schemaDriver struct{}

func (d *schemaDriver) Open(_ string) (driver.Conn, error) { return &schemaConn{}, nil }

type schemaConn struct{}

func (c *schemaConn) Prepare(query string) (driver.Stmt, error) {
	switch {
	case strings.Contains(query, "class_type = 'CLASS'"):
		return &schemaStmt{kind: "tables"}, nil
	case strings.Contains(query, "class_type = 'VCLASS'"):
		return &schemaStmt{kind: "views"}, nil
	case strings.Contains(query, "db_attribute"):
		return &schemaStmt{kind: "columns"}, nil
	default:
		return &schemaStmt{kind: "empty"}, nil
	}
}
func (c *schemaConn) Close() error          { return nil }
func (c *schemaConn) Begin() (driver.Tx, error) { return nil, nil }

type schemaStmt struct{ kind string }

func (s *schemaStmt) Close() error { return nil }
func (s *schemaStmt) NumInput() int { return -1 } // variadic
func (s *schemaStmt) Exec(_ []driver.Value) (driver.Result, error) { return nil, nil }
func (s *schemaStmt) Query(_ []driver.Value) (driver.Rows, error) {
	return &schemaRows{kind: s.kind}, nil
}

type schemaRows struct {
	kind    string
	fetched bool
}

func (r *schemaRows) Columns() []string {
	switch r.kind {
	case "tables", "views":
		return []string{"class_name"}
	case "columns":
		return []string{"attr_name", "data_type", "prec", "scale", "is_nullable", "default_value"}
	default:
		return []string{"col"}
	}
}
func (r *schemaRows) Close() error { return nil }
func (r *schemaRows) Next(dest []driver.Value) error {
	if r.fetched || r.kind == "views" || r.kind == "empty" {
		return io.EOF
	}
	r.fetched = true
	switch r.kind {
	case "tables":
		dest[0] = "mock_table"
	case "columns":
		dest[0] = "id"
		dest[1] = "integer"
		dest[2] = int64(10)
		dest[3] = int64(0)
		dest[4] = "NO"
		dest[5] = nil
	}
	return nil
}

func init() {
	sql.Register("cubrid-mock-empty", &emptyDriver{})
	sql.Register("cubrid-mock-schema", &schemaDriver{})
}
