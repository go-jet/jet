package mysql

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestInvalidInsert(t *testing.T) {
	assertStatementSqlErr(t, table1.INSERT(nil).VALUES(1), "jet: nil column in columns list")
}

func TestInsertNilValue(t *testing.T) {
	assertStatementSql(t, table1.INSERT(table1Col1).VALUES(nil), `
INSERT INTO db.table1 (col1)
VALUES (?);
`, nil)
}

func TestInsertSingleValue(t *testing.T) {
	assertStatementSql(t, table1.INSERT(table1Col1).VALUES(1), `
INSERT INTO db.table1 (col1)
VALUES (?);
`, int(1))
}

func TestInsertWithColumnList(t *testing.T) {
	columnList := ColumnList{table3ColInt}

	columnList = append(columnList, table3StrCol)

	assertStatementSql(t, table3.INSERT(columnList).VALUES(1, 3), `
INSERT INTO db.table3 (col_int, col2)
VALUES (?, ?);
`, 1, 3)
}

func TestInsertDate(t *testing.T) {
	date := time.Date(1999, 1, 2, 3, 4, 5, 0, time.UTC)

	assertStatementSql(t, table1.INSERT(table1ColTimestamp).VALUES(date), `
INSERT INTO db.table1 (col_timestamp)
VALUES (?);
`, date)
}

func TestInsertMultipleValues(t *testing.T) {
	assertStatementSql(t, table1.INSERT(table1Col1, table1ColFloat, table1Col3).VALUES(1, 2, 3), `
INSERT INTO db.table1 (col1, col_float, col3)
VALUES (?, ?, ?);
`, 1, 2, 3)
}

func TestInsertMultipleRows(t *testing.T) {
	stmt := table1.INSERT(table1Col1, table1ColFloat).
		VALUES(1, 2).
		VALUES(11, 22).
		VALUES(111, 222)

	assertStatementSql(t, stmt, `
INSERT INTO db.table1 (col1, col_float)
VALUES (?, ?),
       (?, ?),
       (?, ?);
`, 1, 2, 11, 22, 111, 222)
}

func TestInsertValuesFromModel(t *testing.T) {
	type Table1Model struct {
		Col1     *int
		ColFloat float64
	}

	one := 1

	toInsert := Table1Model{
		Col1:     &one,
		ColFloat: 1.11,
	}

	stmt := table1.INSERT(table1Col1, table1ColFloat).
		MODEL(toInsert).
		MODEL(&toInsert)

	expectedSQL := `
INSERT INTO db.table1 (col1, col_float)
VALUES (?, ?),
       (?, ?);
`

	assertStatementSql(t, stmt, expectedSQL, int(1), float64(1.11), int(1), float64(1.11))
}

func TestInsertValuesFromModelColumnMismatch(t *testing.T) {
	defer func() {
		r := recover()
		require.Equal(t, r, "missing struct field for column : col1")
	}()
	type Table1Model struct {
		Col1Prim int
		Col2     string
	}

	newData := Table1Model{
		Col1Prim: 1,
		Col2:     "one",
	}

	table1.
		INSERT(table1Col1, table1ColFloat).
		MODEL(newData)
}

func TestInsertFromNonStructModel(t *testing.T) {

	defer func() {
		r := recover()
		require.Equal(t, r, "jet: data has to be a struct")
	}()

	table2.INSERT(table2ColInt).MODEL([]int{})
}

func TestInsertDefaultValue(t *testing.T) {
	stmt := table1.INSERT(table1Col1, table1ColFloat).
		VALUES(DEFAULT, "two")

	var expectedSQL = `
INSERT INTO db.table1 (col1, col_float)
VALUES (DEFAULT, ?);
`

	assertStatementSql(t, stmt, expectedSQL, "two")
}

func TestInsertOnDuplicateKeyUpdate(t *testing.T) {
	stmt := func() InsertStatement {
		return table1.INSERT(table1Col1, table1ColFloat).
			VALUES(DEFAULT, "two")
	}

	t.Run("empty list", func(t *testing.T) {
		stmt := stmt().ON_DUPLICATE_KEY_UPDATE()
		assertStatementSql(t, stmt, `
INSERT INTO db.table1 (col1, col_float)
VALUES (DEFAULT, ?);
`, "two")
	})

	t.Run("one set", func(t *testing.T) {
		stmt := stmt().ON_DUPLICATE_KEY_UPDATE(table1ColFloat.SET(Float(11.1)))
		assertStatementSql(t, stmt, `
INSERT INTO db.table1 (col1, col_float)
VALUES (DEFAULT, ?)
ON DUPLICATE KEY UPDATE col_float = ?;
`, "two", 11.1)
	})

	t.Run("all types set", func(t *testing.T) {
		stmt := stmt().ON_DUPLICATE_KEY_UPDATE(
			table1ColBool.SET(Bool(true)),
			table1ColInt.SET(Int(11)),
			table1ColFloat.SET(Float(11.1)),
			table1ColString.SET(String("str")),
			table1ColTime.SET(Time(11, 23, 11)),
			table1ColTimestamp.SET(Timestamp(2020, 1, 22, 3, 4, 5)),
			table1ColDate.SET(Date(2020, 12, 1)),
		)
		assertStatementSql(t, stmt, `
INSERT INTO db.table1 (col1, col_float)
VALUES (DEFAULT, ?)
ON DUPLICATE KEY UPDATE col_bool = ?,
                        col_int = ?,
                        col_float = ?,
                        col_string = ?,
                        col_time = CAST(? AS TIME),
                        col_timestamp = TIMESTAMP(?),
                        col_date = CAST(? AS DATE);
`, "two", true, int64(11), 11.1, "str", "11:23:11", "2020-01-22 03:04:05", "2020-12-01")
	})
}
