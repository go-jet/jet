//go:build integration

package cubrid

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestIntegrationCubridTypes tests code generation with CUBRID-specific column types.
func TestIntegrationCubridTypes(t *testing.T) {
	db := getTestDB(t)
	ctx := context.Background()

	db.ExecContext(ctx, `DROP TABLE IF EXISTS jet_type_test`)
	_, err := db.ExecContext(ctx, `
		CREATE TABLE jet_type_test (
			id          INT AUTO_INCREMENT PRIMARY KEY,
			col_short   SHORT,
			col_int     INT NOT NULL,
			col_bigint  BIGINT,
			col_float   FLOAT,
			col_double  DOUBLE,
			col_numeric NUMERIC(12,4),
			col_monetary MONETARY,
			col_char    CHAR(10),
			col_varchar VARCHAR(200),
			col_string  STRING,
			col_nchar   NCHAR(20),
			col_bit     BIT(8),
			col_varbit  BIT VARYING(100),
			col_date    DATE,
			col_time    TIME,
			col_datetime DATETIME,
			col_timestamp TIMESTAMP,
			col_blob    BLOB,
			col_clob    CLOB,
			col_set     SET_OF(VARCHAR(50)),
			col_multiset MULTISET_OF(INTEGER),
			col_sequence SEQUENCE_OF(VARCHAR(30))
		)`)
	if err != nil {
		t.Fatalf("failed to create jet_type_test: %v", err)
	}
	t.Cleanup(func() {
		db.ExecContext(ctx, `DROP TABLE IF EXISTS jet_type_test`)
	})

	destDir := t.TempDir()
	dbName, _ := extractDBName(getTestDSN())

	err = GenerateDB(db, dbName, destDir)
	if err != nil {
		t.Fatalf("GenerateDB: %v", err)
	}

	// Read generated model file
	modelFile := filepath.Join(destDir, dbName, "model", "jet_type_test.go")
	modelBytes, err := os.ReadFile(modelFile)
	if err != nil {
		t.Fatalf("failed to read model file: %v", err)
	}
	model := string(modelBytes)

	// Read generated table builder file
	tableFile := filepath.Join(destDir, dbName, "table", "jet_type_test.go")
	tableBytes, err := os.ReadFile(tableFile)
	if err != nil {
		t.Fatalf("failed to read table file: %v", err)
	}
	table := string(tableBytes)

	// Verify model types
	typeChecks := []struct {
		field    string
		goType  string
		inModel string
	}{
		{"ColShort", "*int16", model},
		{"ColInt", "int32", model},
		{"ColBigint", "*int64", model},
		{"ColFloat", "*float32", model},
		{"ColDouble", "*float64", model},
		{"ColNumeric", "*float64", model},
		{"ColMonetary", "*float64", model},
		{"ColChar", "*string", model},
		{"ColVarchar", "*string", model},
		{"ColString", "*string", model},
		{"ColNchar", "*string", model},
		{"ColDate", "*time.Time", model},
		{"ColTime", "*time.Time", model},
		{"ColDatetime", "*time.Time", model},
		{"ColTimestamp", "*time.Time", model},
		{"ColClob", "*string", model},
		{"ColSet", "*string", model},
		{"ColMultiset", "*string", model},
		{"ColSequence", "*string", model},
	}

	for _, tc := range typeChecks {
		t.Run("model/"+tc.field, func(t *testing.T) {
			if !strings.Contains(tc.inModel, tc.field) {
				t.Errorf("field %s not found in model", tc.field)
			}
			if !strings.Contains(tc.inModel, tc.goType) {
				t.Errorf("type %s for field %s not found in model", tc.goType, tc.field)
			}
		})
	}

	// Verify table builder column types
	builderChecks := []struct {
		col      string
		colType  string
		inTable  string
	}{
		{"ColShort", "ColumnInteger", table},
		{"ColInt", "ColumnInteger", table},
		{"ColBigint", "ColumnInteger", table},
		{"ColFloat", "ColumnFloat", table},
		{"ColDouble", "ColumnFloat", table},
		{"ColNumeric", "ColumnFloat", table},
		{"ColMonetary", "ColumnFloat", table},
		{"ColChar", "ColumnString", table},
		{"ColVarchar", "ColumnString", table},
		{"ColString", "ColumnString", table},
		{"ColNchar", "ColumnString", table},
		{"ColDate", "ColumnDate", table},
		{"ColTime", "ColumnTime", table},
		{"ColDatetime", "ColumnTimestamp", table},
		{"ColTimestamp", "ColumnTimestamp", table},
		{"ColClob", "ColumnString", table},
		{"ColSet", "ColumnString", table},
		{"ColMultiset", "ColumnString", table},
		{"ColSequence", "ColumnString", table},
	}

	for _, tc := range builderChecks {
		t.Run("builder/"+tc.col, func(t *testing.T) {
			if !strings.Contains(tc.inTable, tc.col) {
				t.Errorf("column %s not found in table builder", tc.col)
			}
			if !strings.Contains(tc.inTable, tc.colType) {
				t.Errorf("column type %s not found in table builder for %s", tc.colType, tc.col)
			}
		})
	}

	// Verify import is cubrid, not mysql
	if !strings.Contains(table, "jet/v2/cubrid") {
		t.Error("table builder should import cubrid dialect")
	}
	if strings.Contains(table, "jet/v2/mysql") {
		t.Error("table builder should NOT import mysql dialect")
	}
}

// TestIntegrationGenerateWithView tests that views are handled correctly.
func TestIntegrationGenerateWithView(t *testing.T) {
	db := getTestDB(t)
	ctx := context.Background()

	db.ExecContext(ctx, `DROP VIEW IF EXISTS jet_view_test`)
	db.ExecContext(ctx, `DROP TABLE IF EXISTS jet_view_base`)

	_, err := db.ExecContext(ctx, `
		CREATE TABLE jet_view_base (
			id   INT PRIMARY KEY,
			name VARCHAR(100),
			val  DOUBLE
		)`)
	if err != nil {
		t.Fatalf("create base table: %v", err)
	}

	_, err = db.ExecContext(ctx, `
		CREATE VIEW jet_view_test AS
		SELECT id, name, val FROM jet_view_base WHERE val > 0`)
	if err != nil {
		t.Fatalf("create view: %v", err)
	}

	t.Cleanup(func() {
		db.ExecContext(ctx, `DROP VIEW IF EXISTS jet_view_test`)
		db.ExecContext(ctx, `DROP TABLE IF EXISTS jet_view_base`)
	})

	destDir := t.TempDir()
	dbName, _ := extractDBName(getTestDSN())

	err = GenerateDB(db, dbName, destDir)
	if err != nil {
		t.Fatalf("GenerateDB: %v", err)
	}

	// Check view directory exists and has the view file
	viewDir := filepath.Join(destDir, dbName, "view")
	if _, err := os.Stat(viewDir); os.IsNotExist(err) {
		t.Fatal("view directory was not created")
	}

	entries, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("read view dir: %v", err)
	}

	found := false
	for _, e := range entries {
		if e.Name() == "jet_view_test.go" {
			found = true
			break
		}
	}
	if !found {
		names := []string{}
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("jet_view_test.go not found in view dir, files: %v", names)
	}
}

// TestIntegrationPrimaryKeyComposite tests composite primary key detection.
func TestIntegrationPrimaryKeyComposite(t *testing.T) {
	db := getTestDB(t)
	ctx := context.Background()

	db.ExecContext(ctx, `DROP TABLE IF EXISTS jet_pk_test`)
	_, err := db.ExecContext(ctx, `
		CREATE TABLE jet_pk_test (
			tenant_id INT NOT NULL,
			user_id   INT NOT NULL,
			name      VARCHAR(100),
			PRIMARY KEY (tenant_id, user_id)
		)`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() { db.ExecContext(ctx, `DROP TABLE IF EXISTS jet_pk_test`) })

	qs := &cubridQuerySet{}
	tables, err := qs.GetTablesMetaData(db, "cubdb", "BASE TABLE")
	if err != nil {
		t.Fatal(err)
	}

	for _, tbl := range tables {
		if tbl.Name == "jet_pk_test" {
			pkCount := 0
			for _, col := range tbl.Columns {
				if col.IsPrimaryKey {
					pkCount++
				}
			}
			if pkCount != 2 {
				t.Errorf("expected 2 primary key columns, got %d", pkCount)
			}
			return
		}
	}
	t.Error("jet_pk_test not found")
}

// TestIntegrationNullableDetection tests nullable column detection.
func TestIntegrationNullableDetection(t *testing.T) {
	db := getTestDB(t)
	ctx := context.Background()

	db.ExecContext(ctx, `DROP TABLE IF EXISTS jet_null_test`)
	_, err := db.ExecContext(ctx, `
		CREATE TABLE jet_null_test (
			id       INT NOT NULL,
			required VARCHAR(50) NOT NULL,
			optional VARCHAR(50),
			with_def INT DEFAULT 0
		)`)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.ExecContext(ctx, `DROP TABLE IF EXISTS jet_null_test`) })

	qs := &cubridQuerySet{}
	tables, err := qs.GetTablesMetaData(db, "cubdb", "BASE TABLE")
	if err != nil {
		t.Fatal(err)
	}

	for _, tbl := range tables {
		if tbl.Name == "jet_null_test" {
			for _, col := range tbl.Columns {
				switch col.Name {
				case "id":
					if col.IsNullable {
						t.Error("id should NOT be nullable")
					}
				case "required":
					if col.IsNullable {
						t.Error("required should NOT be nullable")
					}
				case "optional":
					if !col.IsNullable {
						t.Error("optional SHOULD be nullable")
					}
				case "with_def":
					if !col.HasDefault {
						t.Error("with_def SHOULD have default")
					}
				}
			}
			return
		}
	}
	t.Error("jet_null_test not found")
}
