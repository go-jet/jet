package postgres

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
VALUES ($1);
`, nil)
}

func TestInsertSingleValue(t *testing.T) {
	assertStatementSql(t, table1.INSERT(table1Col1).VALUES(1), `
INSERT INTO db.table1 (col1)
VALUES ($1);
`, int(1))
}

func TestInsertWithColumnList(t *testing.T) {
	columnList := ColumnList{table3ColInt, table3StrCol}

	assertStatementSql(t, table3.INSERT(columnList).VALUES(1, 3), `
INSERT INTO db.table3 (col_int, col2)
VALUES ($1, $2);
`, 1, 3)
}

func TestInsertDate(t *testing.T) {
	date := time.Date(1999, 1, 2, 3, 4, 5, 0, time.UTC)

	assertStatementSql(t, table1.INSERT(table1ColTime).VALUES(date), `
INSERT INTO db.table1 (col_time)
VALUES ($1);
`, date)
}

func TestInsertMultipleValues(t *testing.T) {
	assertStatementSql(t, table1.INSERT(table1Col1, table1ColFloat, table1ColBool).VALUES(1, 2, 3), `
INSERT INTO db.table1 (col1, col_float, col_bool)
VALUES ($1, $2, $3);
`, 1, 2, 3)
}

func TestInsertMultipleRows(t *testing.T) {
	stmt := table1.INSERT(table1Col1, table1ColFloat).
		VALUES(1, 2).
		VALUES(11, 22).
		VALUES(111, 222)

	assertStatementSql(t, stmt, `
INSERT INTO db.table1 (col1, col_float)
VALUES ($1, $2),
       ($3, $4),
       ($5, $6);
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
VALUES ($1, $2),
       ($3, $4);
`

	assertStatementSql(t, stmt, expectedSQL, 1, float64(1.11), 1, float64(1.11))
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

func TestInsertQuery(t *testing.T) {

	stmt := table1.INSERT(table1Col1).
		QUERY(table1.SELECT(table1Col1))

	var expectedSQL = `
INSERT INTO db.table1 (col1) (
     SELECT table1.col1 AS "table1.col1"
     FROM db.table1
);
`
	assertStatementSql(t, stmt, expectedSQL)
}

func TestInsertDefaultValue(t *testing.T) {
	stmt := table1.INSERT(table1Col1, table1ColFloat).
		VALUES(DEFAULT, "two")

	var expectedSQL = `
INSERT INTO db.table1 (col1, col_float)
VALUES (DEFAULT, $1);
`

	assertStatementSql(t, stmt, expectedSQL, "two")
}

func TestInsert_ON_CONFLICT(t *testing.T) {
	stmt := table1.INSERT(table1Col1, table1ColBool).
		VALUES("one", "two").
		VALUES("1", "2").
		VALUES("theta", "beta").
		ON_CONFLICT(table1ColBool).WHERE(table1ColBool.IS_NOT_FALSE()).
		DO_UPDATE(
			SET(table1ColBool.SET(Bool(true)),
				table2ColInt.SET(Int(1)),
				ColumnList{table1Col1, table1ColBool}.SET(ROW(Int(2), String("two"))),
			).WHERE(table1Col1.GT(Int(2))),
		).
		RETURNING(table1Col1, table1ColBool)

	assertDebugStatementSql(t, stmt, `
INSERT INTO db.table1 (col1, col_bool)
VALUES ('one', 'two'),
       ('1', '2'),
       ('theta', 'beta')
ON CONFLICT (col_bool) WHERE col_bool IS NOT FALSE DO UPDATE
       SET col_bool = TRUE::boolean,
           col_int = 1,
           (col1, col_bool) = ROW(2, 'two'::text)
       WHERE table1.col1 > 2
RETURNING table1.col1 AS "table1.col1",
          table1.col_bool AS "table1.col_bool";
`)
}

func TestInsert_ON_CONFLICT_ON_CONSTRAINT(t *testing.T) {
	stmt := table1.INSERT(table1Col1, table1ColBool).
		VALUES("one", "two").
		VALUES("1", "2").
		ON_CONFLICT().ON_CONSTRAINT("idk_primary_key").
		DO_UPDATE(
			SET(table1ColBool.SET(Bool(false)),
				table2ColInt.SET(Int(1)),
				ColumnList{table1Col1, table1ColBool}.SET(ROW(Int(2), String("two"))),
			).WHERE(table1Col1.GT(Int(2)))).
		RETURNING(table1Col1, table1ColBool)

	assertDebugStatementSql(t, stmt, `
INSERT INTO db.table1 (col1, col_bool)
VALUES ('one', 'two'),
       ('1', '2')
ON CONFLICT ON CONSTRAINT idk_primary_key DO UPDATE
       SET col_bool = FALSE::boolean,
           col_int = 1,
           (col1, col_bool) = ROW(2, 'two'::text)
       WHERE table1.col1 > 2
RETURNING table1.col1 AS "table1.col1",
          table1.col_bool AS "table1.col_bool";
`)
}
