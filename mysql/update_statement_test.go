package mysql

import (
	"fmt"
	"strings"
	"testing"
)

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

func TestUpdateWithOneValue(t *testing.T) {
	expectedSQL := `
UPDATE db.table1
SET col_int = ?
WHERE table1.col_int >= ?;
`
	stmt := table1.UPDATE(table1ColInt).
		SET(1).
		WHERE(table1ColInt.GT_EQ(Int(33)))

	fmt.Println(stmt.Sql())

	assertStatementSql(t, stmt, expectedSQL, 1, int64(33))
}

func TestUpdateWithValues(t *testing.T) {
	expectedSQL := `
UPDATE db.table1
SET col_int = ?,
    col_float = ?
WHERE table1.col_int >= ?;
`
	stmt := table1.UPDATE(table1ColInt, table1ColFloat).
		SET(1, 22.2).
		WHERE(table1ColInt.GT_EQ(Int(33)))

	fmt.Println(stmt.Sql())

	assertStatementSql(t, stmt, expectedSQL, 1, 22.2, int64(33))
}

func TestUpdateOneColumnWithSelect(t *testing.T) {
	expectedSQL := `
UPDATE db.table1
SET col_float = (
         SELECT table1.col_float AS "table1.col_float"
         FROM db.table1
    )
WHERE table1.col1 = ?;
`
	stmt := table1.
		UPDATE(table1ColFloat).
		SET(
			table1.SELECT(table1ColFloat),
		).
		WHERE(table1Col1.EQ(Int(2)))

	assertStatementSql(t, stmt, expectedSQL, int64(2))
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

func LimitPanicStatement() {
	joinedTable := table1.INNER_JOIN(table2, table1Col1.EQ(table2Col3))
	joinedTable.UPDATE(table1ColInt).
		SET(1).
		WHERE(table1ColInt.GT_EQ(Int(33))).
		LIMIT(5)
}

func TestUpdateWithMultiTableAndLimit(t *testing.T) {
	assertPanicErr(t, func() { LimitPanicStatement() }, "jet: MySQL does not support LIMIT with multi-table UPDATE statements")
}

func TestInvalidInputs(t *testing.T) {
	assertStatementSqlErr(t, table1.UPDATE(table1ColInt).SET(1), "jet: WHERE clause not set")
	assertStatementSqlErr(t, table1.UPDATE(nil).SET(1), "jet: nil column in columns list for SET clause")
}
