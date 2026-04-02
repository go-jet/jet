package cubrid

import (
	"github.com/go-jet/jet/v2/internal/testutils"
	"testing"
)

func TestMergeUpdate(t *testing.T) {
	stmt := MERGE(table1).
		USING(table2).
		ON(table1ColInt.EQ(table2ColInt)).
		WHEN_MATCHED().THEN_UPDATE(
		table1ColFloat.SET(table2ColFloat),
	)

	testutils.AssertStatementSql(t, stmt, `
MERGE INTO db.table1
USING db.table2
ON (table1.col_int = table2.col_int)
WHEN MATCHED THEN UPDATE SET col_float = table2.col_float;
`)
}

func TestMergeDelete(t *testing.T) {
	stmt := MERGE(table1).
		USING(table2).
		ON(table1ColInt.EQ(table2ColInt)).
		WHEN_MATCHED().THEN_DELETE()

	testutils.AssertStatementSql(t, stmt, `
MERGE INTO db.table1
USING db.table2
ON (table1.col_int = table2.col_int)
WHEN MATCHED THEN DELETE;
`)
}

func TestMergeInsert(t *testing.T) {
	stmt := MERGE(table1).
		USING(table2).
		ON(table1ColInt.EQ(table2ColInt)).
		WHEN_NOT_MATCHED().THEN_INSERT(table1Col1, table1ColFloat).VALUES(1, 2.5)

	testutils.AssertStatementSql(t, stmt, `
MERGE INTO db.table1
USING db.table2
ON (table1.col_int = table2.col_int)
WHEN NOT MATCHED THEN INSERT (col1, col_float) VALUES (?, ?);
`, 1, 2.5)
}

func TestMergeUpdateAndInsert(t *testing.T) {
	stmt := MERGE(table1).
		USING(table2).
		ON(table1ColInt.EQ(table2ColInt)).
		WHEN_MATCHED().THEN_UPDATE(
		table1ColFloat.SET(table2ColFloat),
	).
		WHEN_NOT_MATCHED().THEN_INSERT(table1Col1, table1ColFloat).VALUES(1, 2.5)

	testutils.AssertStatementSql(t, stmt, `
MERGE INTO db.table1
USING db.table2
ON (table1.col_int = table2.col_int)
WHEN MATCHED THEN UPDATE SET col_float = table2.col_float
WHEN NOT MATCHED THEN INSERT (col1, col_float) VALUES (?, ?);
`, 1, 2.5)
}
