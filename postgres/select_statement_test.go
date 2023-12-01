package postgres

import (
	"testing"
)

func TestInvalidSelect(t *testing.T) {
	assertStatementSqlErr(t, SELECT(nil), "jet: Projection is nil")
}

func TestSelectColumnList(t *testing.T) {
	columnList := ColumnList{table2ColInt, table2ColFloat, table3ColInt}

	assertStatementSql(t, SELECT(columnList).FROM(table2), `
SELECT table2.col_int AS "table2.col_int",
     table2.col_float AS "table2.col_float",
     table3.col_int AS "table3.col_int"
FROM db.table2;
`)
}

func TestSelectLiterals(t *testing.T) {
	assertStatementSql(t, SELECT(Int(1), Float(2.2), Bool(false)).FROM(table1), `
SELECT $1,
     $2,
     $3::boolean
FROM db.table1;
`, int64(1), 2.2, false)
}

func TestSelectDistinct(t *testing.T) {
	assertStatementSql(t, SELECT(table1ColBool).DISTINCT().FROM(table1), `
SELECT DISTINCT table1.col_bool AS "table1.col_bool"
FROM db.table1;
`)
}

func TestSelectFrom(t *testing.T) {
	assertStatementSql(t, SELECT(table1ColInt, table2ColFloat).FROM(table1), `
SELECT table1.col_int AS "table1.col_int",
     table2.col_float AS "table2.col_float"
FROM db.table1;
`)
	assertStatementSql(t, SELECT(table1ColInt, table2ColFloat).FROM(table1.INNER_JOIN(table2, table1ColInt.EQ(table2ColInt))), `
SELECT table1.col_int AS "table1.col_int",
     table2.col_float AS "table2.col_float"
FROM db.table1
     INNER JOIN db.table2 ON (table1.col_int = table2.col_int);
`)
	assertStatementSql(t, table1.INNER_JOIN(table2, table1ColInt.EQ(table2ColInt)).SELECT(table1ColInt, table2ColFloat), `
SELECT table1.col_int AS "table1.col_int",
     table2.col_float AS "table2.col_float"
FROM db.table1
     INNER JOIN db.table2 ON (table1.col_int = table2.col_int);
`)
}

func TestSelectWhere(t *testing.T) {
	assertStatementSql(t, SELECT(table1ColInt).FROM(table1).WHERE(Bool(true)), `
SELECT table1.col_int AS "table1.col_int"
FROM db.table1
WHERE $1::boolean;
`, true)
	assertStatementSql(t, SELECT(table1ColInt).FROM(table1).WHERE(table1ColInt.GT_EQ(Int(10))), `
SELECT table1.col_int AS "table1.col_int"
FROM db.table1
WHERE table1.col_int >= $1;
`, int64(10))
}

func TestSelectGroupBy(t *testing.T) {
	assertStatementSql(t, SELECT(table2ColInt).FROM(table2).GROUP_BY(table2ColFloat), `
SELECT table2.col_int AS "table2.col_int"
FROM db.table2
GROUP BY table2.col_float;
`)
}

func TestSelectHaving(t *testing.T) {
	assertStatementSql(t, SELECT(table3ColInt).FROM(table3).HAVING(table1ColBool.EQ(Bool(true))), `
SELECT table3.col_int AS "table3.col_int"
FROM db.table3
HAVING table1.col_bool = $1::boolean;
`, true)
}

func TestSelectOrderBy(t *testing.T) {
	assertStatementSql(t, SELECT(table2ColFloat).FROM(table2).ORDER_BY(table2ColInt.DESC()), `
SELECT table2.col_float AS "table2.col_float"
FROM db.table2
ORDER BY table2.col_int DESC;
`)
	assertStatementSql(t, SELECT(table2ColFloat).FROM(table2).ORDER_BY(table2ColInt.DESC(), table2ColInt.ASC()), `
SELECT table2.col_float AS "table2.col_float"
FROM db.table2
ORDER BY table2.col_int DESC, table2.col_int ASC;
`)
}

func TestSelectLimitOffset(t *testing.T) {
	assertStatementSql(t, SELECT(table2ColInt).FROM(table2).LIMIT(10), `
SELECT table2.col_int AS "table2.col_int"
FROM db.table2
LIMIT $1;
`, int64(10))
	assertStatementSql(t, SELECT(table2ColInt).FROM(table2).LIMIT(10).OFFSET(2), `
SELECT table2.col_int AS "table2.col_int"
FROM db.table2
LIMIT $1
OFFSET $2;
`, int64(10), int64(2))
}

func TestSelectLock(t *testing.T) {
	assertStatementSql(t, SELECT(table1ColBool).FROM(table1).FOR(UPDATE()), `
SELECT table1.col_bool AS "table1.col_bool"
FROM db.table1
FOR UPDATE;
`)
	assertStatementSql(t, SELECT(table1ColBool).FROM(table1).FOR(SHARE().NOWAIT()), `
SELECT table1.col_bool AS "table1.col_bool"
FROM db.table1
FOR SHARE NOWAIT;
`)

	assertStatementSql(t, SELECT(table1ColBool).FROM(table1).FOR(KEY_SHARE().NOWAIT()), `
SELECT table1.col_bool AS "table1.col_bool"
FROM db.table1
FOR KEY SHARE NOWAIT;
`)
	assertStatementSql(t, SELECT(table1ColBool).FROM(table1).FOR(NO_KEY_UPDATE().SKIP_LOCKED()), `
SELECT table1.col_bool AS "table1.col_bool"
FROM db.table1
FOR NO KEY UPDATE SKIP LOCKED;
`)

	assertStatementSql(t, SELECT(table1ColBool).FROM(table1).FOR(UPDATE().OF(table1, table2).NOWAIT()), `
SELECT table1.col_bool AS "table1.col_bool"
FROM db.table1
FOR UPDATE OF table1, table2 NOWAIT;
`)
}
