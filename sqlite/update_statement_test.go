package sqlite

import (
	"fmt"
	"strings"
	"testing"
)

func TestUpdateWithOneValue(t *testing.T) {
	expectedSQL := `
UPDATE db.table1
SET col_int = ?
WHERE table1.col_int >= ?
LIMIT ?;
`
	stmt := table1.UPDATE(table1ColInt).
		SET(1).
		WHERE(table1ColInt.GT_EQ(Int(33))).
		LIMIT(0)

	fmt.Println(stmt.Sql())

	assertStatementSql(t, stmt, expectedSQL, 1, int64(33), int64(0))
}

func TestUpdateWithValues(t *testing.T) {
	expectedSQL := `
UPDATE db.table1
SET col_int = ?,
    col_float = ?
WHERE table1.col_int >= ?
LIMIT ?;
`
	stmt := table1.UPDATE(table1ColInt, table1ColFloat).
		SET(1, 22.2).
		WHERE(table1ColInt.GT_EQ(Int(33))).
		LIMIT(0)

	fmt.Println(stmt.Sql())

	assertStatementSql(t, stmt, expectedSQL, 1, 22.2, int64(33), int64(0))
}

func TestUpdateOneColumnWithSelect(t *testing.T) {
	expectedSQL := `
UPDATE db.table1
SET col_float = (
         SELECT table1.col_float AS "table1.col_float"
         FROM db.table1
    )
WHERE table1.col1 = ?
LIMIT ?;
`
	stmt := table1.
		UPDATE(table1ColFloat).
		SET(
			table1.SELECT(table1ColFloat),
		).
		WHERE(table1Col1.EQ(Int(2))).
		LIMIT(0)

	assertStatementSql(t, stmt, expectedSQL, int64(2), int64(0))
}

func TestUpdateReservedWorldColumn(t *testing.T) {
	type table struct {
		Load string
	}

	loadColumn := StringColumn("Load")
	assertStatementSql(t,
		table1.UPDATE(loadColumn).
			MODEL(
				table{
					Load: "foo",
				},
			).
			WHERE(loadColumn.EQ(String("bar"))).
			LIMIT(0),
		strings.Replace(`
UPDATE db.table1
SET ''Load'' = ?
WHERE ''Load'' = ?
LIMIT ?;
`, "''", "`", -1), "foo", "bar", int64(0))
}

func TestUpdateWithLimit(t *testing.T) {
	expectedSQL := `
UPDATE db.table1
SET col_int = ?
WHERE table1.col_int >= ?
LIMIT ?;
`
	stmt := table1.UPDATE(table1ColInt).
		SET(1).
		WHERE(table1ColInt.GT_EQ(Int(33))).
		LIMIT(5)

	assertStatementSql(t, stmt, expectedSQL, 1, int64(33), int64(5))
}

func TestUpdateWithMultiTableAndLimit(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Error("Expected panic when using LIMIT with multi-table UPDATE statement")
		}
		if r.(string) != "jet: SQLite does not support LIMIT with multi-table UPDATE statements" {
			t.Errorf("Expected panic message about LIMIT with multi-table UPDATE, got: %v", r)
		}
	}()

	table1.UPDATE(table1ColInt).
		FROM(table2, table3).
		SET(1).
		WHERE(table1ColInt.GT_EQ(Int(33))).
		LIMIT(5)
}

func TestUpdateFromWithLimit(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Error("Expected panic when using LIMIT with UPDATE...FROM statement")
		}
		if r.(string) != "jet: SQLite does not support LIMIT with UPDATE...FROM statements" {
			t.Errorf("Expected panic message about LIMIT with UPDATE...FROM, got: %v", r)
		}
	}()

	// Set up an update statement with LIMIT first, then try to add FROM
	stmt := table1.UPDATE(table1ColInt).
		SET(1).
		WHERE(table1ColInt.GT_EQ(Int(33))).
		LIMIT(5)

	// This should panic
	stmt.FROM(table2)
}

func TestInvalidInputs(t *testing.T) {
	assertStatementSqlErr(t, table1.UPDATE(table1ColInt).SET(1), "jet: WHERE clause not set")
	assertStatementSqlErr(t, table1.UPDATE(nil).SET(1), "jet: nil column in columns list for SET clause")
}
