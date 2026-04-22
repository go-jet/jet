package cubrid

import (
	"testing"
)

func TestReplaceIntoValues(t *testing.T) {
	assertStatementSql(t, REPLACE_INTO(table1, table1Col1, table1ColFloat).VALUES(1, 2.5), `
REPLACE INTO db.table1 (col1, col_float)
VALUES (?, ?);
`, 1, 2.5)
}

func TestReplaceIntoMultipleRows(t *testing.T) {
	assertStatementSql(t, REPLACE_INTO(table1, table1Col1).VALUES(1).VALUES(2).VALUES(3), `
REPLACE INTO db.table1 (col1)
VALUES (?),
       (?),
       (?);
`, 1, 2, 3)
}

func TestReplaceIntoModel(t *testing.T) {
	type Row struct {
		Col1     int
		ColFloat float64
	}

	assertStatementSql(t, REPLACE_INTO(table1, table1Col1, table1ColFloat).MODEL(Row{Col1: 10, ColFloat: 3.14}), `
REPLACE INTO db.table1 (col1, col_float)
VALUES (?, ?);
`, 10, 3.14)
}

func TestReplaceIntoQuery(t *testing.T) {
	assertStatementSql(t, REPLACE_INTO(table1, table1Col1).QUERY(SELECT(table2ColInt).FROM(table2)), `
REPLACE INTO db.table1 (col1) (
     SELECT table2.col_int AS "table2.col_int"
     FROM db.table2
);
`)
}

func TestReplaceIntoMODELS(t *testing.T) {
	type Row struct {
		Col1     int
		ColFloat float64
	}

	rows := []Row{{Col1: 1, ColFloat: 1.1}, {Col1: 2, ColFloat: 2.2}}

	assertStatementSql(t, REPLACE_INTO(table1, table1Col1, table1ColFloat).MODELS(rows), `
REPLACE INTO db.table1 (col1, col_float)
VALUES (?, ?),
       (?, ?);
`, 1, 1.1, 2, 2.2)
}
