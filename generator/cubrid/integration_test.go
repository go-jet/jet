//go:build integration

package cubrid

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/search5/cubrid-go"
)

const defaultDSN = "cubrid://dba:@localhost:33000/cubdb"

func getTestDSN() string {
	if dsn := os.Getenv("CUBRID_DSN"); dsn != "" {
		return dsn
	}
	return defaultDSN
}

func getTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("cubrid", getTestDSN())
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("CUBRID not available: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func setupTestTable(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()

	db.ExecContext(ctx, `DROP TABLE IF EXISTS jet_test_orders`)
	db.ExecContext(ctx, `DROP TABLE IF EXISTS jet_test_users`)

	_, err := db.ExecContext(ctx, `
		CREATE TABLE jet_test_users (
			id       INT AUTO_INCREMENT PRIMARY KEY,
			name     VARCHAR(100) NOT NULL,
			email    VARCHAR(200),
			age      INT DEFAULT 0,
			balance  NUMERIC(10,2),
			is_admin BIT DEFAULT B'0',
			created  DATETIME DEFAULT SYSDATETIME
		)`)
	if err != nil {
		t.Fatalf("failed to create jet_test_users: %v", err)
	}

	_, err = db.ExecContext(ctx, `
		CREATE TABLE jet_test_orders (
			id        INT AUTO_INCREMENT PRIMARY KEY,
			user_id   INT NOT NULL,
			amount    DOUBLE NOT NULL,
			status    VARCHAR(20) DEFAULT 'pending',
			order_dt  TIMESTAMP DEFAULT SYSTIMESTAMP,
			FOREIGN KEY (user_id) REFERENCES jet_test_users(id)
		)`)
	if err != nil {
		t.Fatalf("failed to create jet_test_orders: %v", err)
	}

	t.Cleanup(func() {
		db.ExecContext(ctx, `DROP TABLE IF EXISTS jet_test_orders`)
		db.ExecContext(ctx, `DROP TABLE IF EXISTS jet_test_users`)
	})
}

func TestIntegrationQuerySet(t *testing.T) {
	db := getTestDB(t)
	setupTestTable(t, db)

	qs := &cubridQuerySet{}

	tables, err := qs.GetTablesMetaData(db, "cubdb", "BASE TABLE")
	if err != nil {
		t.Fatalf("GetTablesMetaData: %v", err)
	}

	found := false
	for _, tbl := range tables {
		if tbl.Name == "jet_test_users" {
			found = true
			if len(tbl.Columns) < 7 {
				t.Errorf("expected at least 7 columns, got %d", len(tbl.Columns))
			}

			// Verify column types are correctly mapped.
			for _, col := range tbl.Columns {
				if col.Name == "id" && !col.IsPrimaryKey {
					t.Error("id should be primary key")
				}
				if col.Name == "name" && col.IsNullable {
					t.Error("name should not be nullable")
				}
				if col.Name == "email" && !col.IsNullable {
					t.Error("email should be nullable")
				}
			}
			break
		}
	}
	if !found {
		t.Error("jet_test_users not found")
	}
}

func TestIntegrationViews(t *testing.T) {
	db := getTestDB(t)

	qs := &cubridQuerySet{}
	views, err := qs.GetTablesMetaData(db, "cubdb", "VIEW")
	if err != nil {
		t.Fatalf("GetTablesMetaData(VIEW): %v", err)
	}
	// Views may be empty, just verify no error.
	_ = views
}

func TestIntegrationGenerate(t *testing.T) {
	db := getTestDB(t)
	setupTestTable(t, db)

	destDir := t.TempDir()
	dbName, _ := extractDBName(getTestDSN())

	err := GenerateDB(db, dbName, destDir)
	if err != nil {
		t.Fatalf("GenerateDB: %v", err)
	}

	// Verify generated structure.
	modelDir := filepath.Join(destDir, dbName, "model")
	tableDir := filepath.Join(destDir, dbName, "table")

	if _, err := os.Stat(modelDir); os.IsNotExist(err) {
		t.Error("model directory was not created")
	}
	if _, err := os.Stat(tableDir); os.IsNotExist(err) {
		t.Error("table directory was not created")
	}

	// Check model files.
	entries, err := os.ReadDir(modelDir)
	if err != nil {
		t.Fatalf("failed to read model dir: %v", err)
	}
	fileNames := make(map[string]bool)
	for _, e := range entries {
		fileNames[e.Name()] = true
	}
	if !fileNames["jet_test_users.go"] {
		t.Errorf("jet_test_users.go not found, files: %v", fileNames)
	}
	if !fileNames["jet_test_orders.go"] {
		t.Errorf("jet_test_orders.go not found, files: %v", fileNames)
	}

	// Verify generated SQL builder files import cubrid, not mysql.
	tableEntries, err := os.ReadDir(tableDir)
	if err != nil {
		t.Fatalf("failed to read table dir: %v", err)
	}
	if len(tableEntries) == 0 {
		t.Error("no table SQL builder files generated")
	}

	// Read a generated file and verify it imports cubrid dialect.
	for _, e := range tableEntries {
		if e.Name() == "jet_test_users.go" {
			content, err := os.ReadFile(filepath.Join(tableDir, e.Name()))
			if err != nil {
				t.Fatalf("failed to read generated file: %v", err)
			}
			s := string(content)
			if !contains(s, "jet/v2/cubrid") {
				t.Errorf("generated file should import cubrid dialect, got:\n%s", s[:min(len(s), 300)])
			}
			if contains(s, "jet/v2/mysql") {
				t.Error("generated file should NOT import mysql dialect")
			}
			break
		}
	}
}

func TestIntegrationGenerateDSN(t *testing.T) {
	db := getTestDB(t)
	setupTestTable(t, db)
	db.Close()

	destDir := t.TempDir()
	err := GenerateDSN(getTestDSN(), destDir)
	if err != nil {
		t.Fatalf("GenerateDSN: %v", err)
	}

	dbName, _ := extractDBName(getTestDSN())
	modelDir := filepath.Join(destDir, dbName, "model")
	if _, err := os.Stat(modelDir); os.IsNotExist(err) {
		t.Error("model directory was not created via GenerateDSN")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
