package cubrid

import (
	"testing"
)

func TestUpdateWithOneValue(t *testing.T) {
	assertStatementSql(t, table1.UPDATE(table1ColInt).
		SET(1).
		WHERE(table1ColInt.GT_EQ(Int(33))), `
UPDATE db.table1
SET col_int = ?
WHERE table1.col_int >= ?;
`, 1, int64(33))
}

func TestUpdateWithValues(t *testing.T) {
	assertStatementSql(t, table1.UPDATE(table1ColInt, table1ColFloat).
		SET(1, 22.2).
		WHERE(table1ColInt.GT_EQ(Int(33))), `
UPDATE db.table1
SET col_int = ?,
    col_float = ?
WHERE table1.col_int >= ?;
`, 1, 22.2, int64(33))
}

func TestUpdateWithLimit(t *testing.T) {
	assertStatementSql(t, table1.UPDATE(table1ColInt).
		SET(1).
		WHERE(table1ColInt.GT_EQ(Int(33))).
		LIMIT(5), `
UPDATE db.table1
SET col_int = ?
WHERE table1.col_int >= ?
LIMIT ?;
`, 1, int64(33), int64(5))
}

func TestUpdateWithModel(t *testing.T) {
	type Table1Model struct {
		ColInt int
	}
	model := Table1Model{ColInt: 42}

	assertStatementSql(t, table1.UPDATE(table1ColInt).
		MODEL(model).
		WHERE(table1Col1.EQ(Int(1))), `
UPDATE db.table1
SET col_int = ?
WHERE table1.col1 = ?;
`, 42, int64(1))
}

func TestUpdateUnconditionally(t *testing.T) {
	assertStatementSqlErr(t, table1.UPDATE(table1ColInt).SET(1), "jet: WHERE clause not set")
}

func TestUpdateNilColumn(t *testing.T) {
	assertStatementSqlErr(t, table1.UPDATE(nil).SET(1), "jet: nil column in columns list for SET clause")
}
