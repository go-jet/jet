package sqlbuilder

import (
	"fmt"
	"gotest.tools/assert"
	"testing"
	"time"
)

func TestInsertNoColumn(t *testing.T) {
	_, err := table1.INSERT().VALUES().String()

	assert.Assert(t, err != nil)
}

func TestInsertNoRow(t *testing.T) {
	_, err := table1.INSERT(table1Col1).String()

	assert.Assert(t, err != nil)
}

func TestInsertColumnLengthMismatch(t *testing.T) {
	_, err := table1.INSERT(table1Col1, table1Col2).VALUES(nil).String()

	fmt.Println(err)
	assert.Assert(t, err != nil)
}

func TestInsertNilValue(t *testing.T) {
	_, err := table1.INSERT(table1Col1).VALUES(nil).String()

	assert.Assert(t, err != nil)
}

func TestInsertNilColumn(t *testing.T) {
	_, err := table1.INSERT(nil).VALUES(1).String()

	assert.Assert(t, err != nil)
}

func TestInsertSingleValue(t *testing.T) {
	sql, err := table1.INSERT(table1Col1).VALUES(1).String()
	assert.NilError(t, err)

	assert.Equal(t, sql, "INSERT INTO db.table1 (col1) VALUES (1)")
}

func TestInsertDate(t *testing.T) {
	date := time.Date(1999, 1, 2, 3, 4, 5, 0, time.UTC)

	sql, err := table1.INSERT(table1Col4).VALUES(date).String()
	assert.NilError(t, err)

	assert.Equal(t, sql, "INSERT INTO db.table1 (col4) "+
		"VALUES ('1999-01-02 03:04:05.000000')")
}

func TestInsertMultipleValues(t *testing.T) {
	stmt := table1.INSERT(table1Col1, table1Col2, table1Col3)
	stmt.VALUES(1, 2, 3)

	sql, err := stmt.String()
	assert.NilError(t, err)

	assert.Equal(t, sql, "INSERT INTO db.table1 "+
		"(col1,col2,col3) "+
		"VALUES (1,2,3)")
}

func TestInsertMultipleRows(t *testing.T) {
	stmt := table1.INSERT(table1Col1, table1Col2).
		VALUES(1, 2).
		VALUES(11, 22).
		VALUES(111, 222)

	sql, err := stmt.String()
	assert.NilError(t, err)

	assert.Equal(t, sql, "INSERT INTO db.table1 "+
		"(col1,col2) "+
		"VALUES (1,2), (11,22), (111,222)")
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

	sql, err := stmt.String()

	assert.NilError(t, err)

	fmt.Println(sql)

	assert.Equal(t, sql, `INSERT INTO db.table1 (col1,col2) VALUES (1,'one')`)
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

	_, err := stmt.String()

	fmt.Println(err)
	assert.Assert(t, err != nil)
}
