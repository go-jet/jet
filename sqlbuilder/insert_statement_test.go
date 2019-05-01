package sqlbuilder

import (
	"fmt"
	"gotest.tools/assert"
	"testing"
	"time"
)

func TestInsertNoColumn(t *testing.T) {
	_, _, err := table1.INSERT().VALUES().Sql()

	assert.Assert(t, err != nil)
}

func TestInsertNoRow(t *testing.T) {
	_, _, err := table1.INSERT(table1Col1).Sql()

	assert.Assert(t, err != nil)
}

func TestInsertColumnLengthMismatch(t *testing.T) {
	_, _, err := table1.INSERT(table1Col1, table1Col2).VALUES(nil).Sql()

	//fmt.Println(err)
	assert.Assert(t, err != nil)
}

func TestInsertNilValue(t *testing.T) {
	query, args, err := table1.INSERT(table1Col1).VALUES(nil).Sql()

	assert.Equal(t, query, "INSERT INTO db.table1 (col1) VALUES ($1);")
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

	assert.Equal(t, sql, "INSERT INTO db.table1 (col1) VALUES ($1);")
}

func TestInsertDate(t *testing.T) {
	date := time.Date(1999, 1, 2, 3, 4, 5, 0, time.UTC)

	sql, _, err := table1.INSERT(table1Col4).VALUES(date).Sql()
	assert.NilError(t, err)

	assert.Equal(t, sql, "INSERT INTO db.table1 (col4) VALUES ($1);")
}

func TestInsertMultipleValues(t *testing.T) {
	stmt := table1.INSERT(table1Col1, table1Col2, table1Col3)
	stmt.VALUES(1, 2, 3)

	sql, _, err := stmt.Sql()
	assert.NilError(t, err)

	assert.Equal(t, sql, "INSERT INTO db.table1 (col1,col2,col3) VALUES ($1, $2, $3);")
}

func TestInsertMultipleRows(t *testing.T) {
	stmt := table1.INSERT(table1Col1, table1Col2).
		VALUES(1, 2).
		VALUES(11, 22).
		VALUES(111, 222)

	sql, _, err := stmt.Sql()
	assert.NilError(t, err)

	assert.Equal(t, sql, "INSERT INTO db.table1 (col1,col2) VALUES ($1, $2), ($3, $4), ($5, $6);")
}

func TestInsertValuesFromModel(t *testing.T) {
	type Table1Model struct {
		Col1 int
		Col2 string
	}

	toInsert := Table1Model{
		Col1: 1,
		Col2: "one",
	}

	stmt := table1.INSERT(table1Col1, table1Col2).
		VALUES_MAPPING(toInsert)

	sql, _, err := stmt.Sql()

	assert.NilError(t, err)

	fmt.Println(sql)

	assert.Equal(t, sql, `INSERT INTO db.table1 (col1,col2) VALUES ($1, $2);`)
}

func TestInsertValuesFromModelColumnMismatch(t *testing.T) {
	type Table1Model struct {
		Col1Prim int
		Col2     string
	}

	toInsert := Table1Model{
		Col1Prim: 1,
		Col2:     "one",
	}

	stmt := table1.INSERT(table1Col1, table1Col2).
		VALUES_MAPPING(toInsert)

	_, _, err := stmt.Sql()

	//fmt.Println(err)
	assert.Assert(t, err != nil)
}

func TestInsertQuery(t *testing.T) {

	stmt := table1.INSERT(table1Col1).
		QUERY(table1.SELECT(table1Col1))

	stmtStr, _, err := stmt.Sql()

	assert.NilError(t, err)

	fmt.Println(stmtStr)
}

func TestInsertDefaultValue(t *testing.T) {
	stmt := table1.INSERT(table1Col1, table1Col2).
		VALUES(DEFAULT, "two")

	stmtStr, _, err := stmt.Sql()

	assert.NilError(t, err)

	fmt.Println(stmtStr)
}
