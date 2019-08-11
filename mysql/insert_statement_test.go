package mysql

import (
	"gotest.tools/assert"
	"testing"
	"time"
)

//TODO:
//func TestInvalidInsert(t *testing.T) {
//	assertStatementErr(t, table1.INSERT(table1Col1), "jet: no row values or query specified")
//	assertStatementErr(t, table1.INSERT(nil).VALUES(1), "jet: nil column in columns list")
//}

func TestInsertNilValue(t *testing.T) {
	assertStatement(t, table1.INSERT(table1Col1).VALUES(nil), `
INSERT INTO db.table1 (col1) VALUES
     (?);
`, nil)
}

func TestInsertSingleValue(t *testing.T) {
	assertStatement(t, table1.INSERT(table1Col1).VALUES(1), `
INSERT INTO db.table1 (col1) VALUES
     (?);
`, int(1))
}

func TestInsertWithColumnList(t *testing.T) {
	columnList := ColumnList(table3ColInt, table3StrCol)

	assertStatement(t, table3.INSERT(columnList).VALUES(1, 3), `
INSERT INTO db.table3 (col_int, col2) VALUES
     (?, ?);
`, 1, 3)
}

func TestInsertDate(t *testing.T) {
	date := time.Date(1999, 1, 2, 3, 4, 5, 0, time.UTC)

	assertStatement(t, table1.INSERT(table1ColTimestamp).VALUES(date), `
INSERT INTO db.table1 (col_timestamp) VALUES
     (?);
`, date)
}

func TestInsertMultipleValues(t *testing.T) {
	assertStatement(t, table1.INSERT(table1Col1, table1ColFloat, table1Col3).VALUES(1, 2, 3), `
INSERT INTO db.table1 (col1, col_float, col3) VALUES
     (?, ?, ?);
`, 1, 2, 3)
}

func TestInsertMultipleRows(t *testing.T) {
	stmt := table1.INSERT(table1Col1, table1ColFloat).
		VALUES(1, 2).
		VALUES(11, 22).
		VALUES(111, 222)

	assertStatement(t, stmt, `
INSERT INTO db.table1 (col1, col_float) VALUES
     (?, ?),
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
INSERT INTO db.table1 (col1, col_float) VALUES
     (?, ?),
     (?, ?);
`

	assertStatement(t, stmt, expectedSQL, int(1), float64(1.11), int(1), float64(1.11))
}

func TestInsertValuesFromModelColumnMismatch(t *testing.T) {
	defer func() {
		r := recover()
		assert.Equal(t, r, "missing struct field for column : col1")
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
		assert.Equal(t, r, "argument mismatch: expected struct, got []int")
	}()

	table2.INSERT(table2ColInt).MODEL([]int{})
}

func TestInsertDefaultValue(t *testing.T) {
	stmt := table1.INSERT(table1Col1, table1ColFloat).
		VALUES(DEFAULT, "two")

	var expectedSQL = `
INSERT INTO db.table1 (col1, col_float) VALUES
     (DEFAULT, ?);
`

	assertStatement(t, stmt, expectedSQL, "two")
}
