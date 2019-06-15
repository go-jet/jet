package sqlbuilder

import (
	"gotest.tools/assert"
	"testing"
	"time"
)

func TestInsertNoRow(t *testing.T) {
	_, _, err := table1.INSERT(table1Col1).Sql()

	assert.Assert(t, err != nil)
}

func TestInsertColumnLengthMismatch(t *testing.T) {
	_, _, err := table1.INSERT(table1Col1, table1ColFloat).VALUES(nil).Sql()

	//fmt.Println(err)
	assert.Assert(t, err != nil)
}

func TestInsertNilValue(t *testing.T) {
	query, args, err := table1.INSERT(table1Col1).VALUES(nil).Sql()

	assert.Equal(t, query, `
INSERT INTO db.table1 (col1) VALUES
     ($1);
`)
	assert.Equal(t, len(args), 1)
	assert.NilError(t, err)
}

func TestInsertNilColumn(t *testing.T) {
	_, _, err := table1.INSERT(nil).VALUES(1).Sql()

	assert.Assert(t, err != nil)
}

func TestInsertSingleValue(t *testing.T) {
	sql, _, err := table1.INSERT(table1Col1).VALUES(1).Sql()
	assert.NilError(t, err)

	assert.Equal(t, sql, `
INSERT INTO db.table1 (col1) VALUES
     ($1);
`)
}

func TestInsertDate(t *testing.T) {
	date := time.Date(1999, 1, 2, 3, 4, 5, 0, time.UTC)

	sql, _, err := table1.INSERT(table1ColTime).VALUES(date).Sql()
	assert.NilError(t, err)

	assert.Equal(t, sql, `
INSERT INTO db.table1 (colTime) VALUES
     ($1);
`)
}

func TestInsertMultipleValues(t *testing.T) {
	stmt := table1.INSERT(table1Col1, table1ColFloat, table1Col3)
	stmt.VALUES(1, 2, 3)

	sql, _, err := stmt.Sql()
	assert.NilError(t, err)

	expectedSql := `
INSERT INTO db.table1 (col1, colFloat, col3) VALUES
     ($1, $2, $3);
`

	assert.Equal(t, sql, expectedSql)
}

func TestInsertMultipleRows(t *testing.T) {
	stmt := table1.INSERT(table1Col1, table1ColFloat).
		VALUES(1, 2).
		VALUES(11, 22).
		VALUES(111, 222)

	sql, _, err := stmt.Sql()
	assert.NilError(t, err)

	expectedSql := `
INSERT INTO db.table1 (col1, colFloat) VALUES
     ($1, $2),
     ($3, $4),
     ($5, $6);
`

	assert.Equal(t, sql, expectedSql)
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
		USING(toInsert).
		USING(&toInsert)

	expectedSql := `
INSERT INTO db.table1 (col1, colFloat) VALUES
     ($1, $2),
     ($3, $4);
`

	assertStatement(t, stmt, expectedSql, int(1), float64(1.11), int(1), float64(1.11))
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

	stmt := table1.
		INSERT(table1Col1, table1ColFloat).
		USING(newData)

	_, _, err := stmt.Sql()

	assert.Assert(t, err != nil)
}

func TestInsertFromNonStructModel(t *testing.T) {

	defer func() {
		r := recover()
		assert.Equal(t, r, "argument mismatch: expected struct, got []int")
	}()

	table2.INSERT(table2ColInt).USING([]int{})
}

func TestInsertQuery(t *testing.T) {

	stmt := table1.INSERT(table1Col1).
		QUERY(table1.SELECT(table1Col1))

	var expectedSql = `
INSERT INTO db.table1 (col1) (
     SELECT table1.col1 AS "table1.col1"
     FROM db.table1
);
`
	assertStatement(t, stmt, expectedSql)
}

func TestInsertDefaultValue(t *testing.T) {
	stmt := table1.INSERT(table1Col1, table1ColFloat).
		VALUES(DEFAULT, "two")

	var expectedSql = `
INSERT INTO db.table1 (col1, colFloat) VALUES
     (DEFAULT, $1);
`

	assertStatement(t, stmt, expectedSql, "two")
}
