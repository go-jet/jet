package sqlbuilder

import (
	"testing"
)

func TestUpdateWithOneValue(t *testing.T) {
	expectedSql := `
UPDATE db.table1
SET col_int = $1
WHERE table1.col_int >= $2;
`
	stmt := table1.UPDATE(table1ColInt).
		SET(1).
		WHERE(table1ColInt.GT_EQ(Int(33)))

	assertStatement(t, stmt, expectedSql, 1, int64(33))
}

func TestUpdateWithValues(t *testing.T) {
	expectedSql := `
UPDATE db.table1
SET (col_int, col_float) = ($1, $2)
WHERE table1.col_int >= $3;
`
	stmt := table1.UPDATE(table1ColInt, table1ColFloat).
		SET(1, 22.2).
		WHERE(table1ColInt.GT_EQ(Int(33)))

	assertStatement(t, stmt, expectedSql, 1, 22.2, int64(33))
}

func TestUpdateOneColumnWithSelect(t *testing.T) {
	expectedSql := `
UPDATE db.table1
SET col_float = (
     SELECT table1.col_float AS "table1.col_float"
     FROM db.table1
)
WHERE table1.col1 = $1
RETURNING table1.col1 AS "table1.col1";
`
	stmt := table1.
		UPDATE(table1ColFloat).
		SET(
			table1.SELECT(table1ColFloat),
		).
		WHERE(table1Col1.EQ(Int(2))).
		RETURNING(table1Col1)

	assertStatement(t, stmt, expectedSql, int64(2))
}

func TestUpdateColumnsWithSelect(t *testing.T) {
	expectedSql := `
UPDATE db.table1
SET (col1, col_float) = (
     SELECT table1.col_float AS "table1.col_float",
          table2.col3 AS "table2.col3"
     FROM db.table1
)
WHERE table1.col1 = $1
RETURNING table1.col1 AS "table1.col1";
`
	stmt := table1.UPDATE(table1Col1, table1ColFloat).
		SET(table1.SELECT(table1ColFloat, table2Col3)).
		WHERE(table1Col1.EQ(Int(2))).
		RETURNING(table1Col1)

	assertStatement(t, stmt, expectedSql, int64(2))
}

func TestInvalidInputs(t *testing.T) {
	assertStatementErr(t, table1.UPDATE(table1ColInt).SET(1, 2), "WHERE clause not set")
	assertStatementErr(t, table1.UPDATE(nil).SET(1, 2), "nil column in columns list")
}
