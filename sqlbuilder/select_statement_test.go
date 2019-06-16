package sqlbuilder

import "testing"

func TestInvalidSelect(t *testing.T) {
	assertStatementErr(t, SELECT(nil), "projection is nil")
}

func TestSelectColumnList(t *testing.T) {
	columnList := ColumnList{table2ColInt, table2ColFloat, table3ColInt}

	assertStatement(t, SELECT(columnList).FROM(table2), `
SELECT table2.colInt AS "table2.colInt",
     table2.colFloat AS "table2.colFloat",
     table3.colInt AS "table3.colInt"
FROM db.table2;
`)
}

func TestSelectDistinct(t *testing.T) {
	assertStatement(t, SELECT(table1ColBool).DISTINCT(), `
SELECT DISTINCT table1.colBool AS "table1.colBool";
`)
}

func TestSelectFrom(t *testing.T) {
	assertStatement(t, SELECT(table1ColInt, table2ColFloat).FROM(table1), `
SELECT table1.colInt AS "table1.colInt",
     table2.colFloat AS "table2.colFloat"
FROM db.table1;
`)
	assertStatement(t, SELECT(table1ColInt, table2ColFloat).FROM(table1.INNER_JOIN(table2, table1ColInt.EQ(table2ColInt))), `
SELECT table1.colInt AS "table1.colInt",
     table2.colFloat AS "table2.colFloat"
FROM db.table1
     INNER JOIN db.table2 ON (table1.colInt = table2.colInt);
`)
	assertStatement(t, table1.INNER_JOIN(table2, table1ColInt.EQ(table2ColInt)).SELECT(table1ColInt, table2ColFloat), `
SELECT table1.colInt AS "table1.colInt",
     table2.colFloat AS "table2.colFloat"
FROM db.table1
     INNER JOIN db.table2 ON (table1.colInt = table2.colInt);
`)
}

func TestSelectWhere(t *testing.T) {
	assertStatement(t, SELECT(table1ColInt).FROM(table1).WHERE(Bool(true)), `
SELECT table1.colInt AS "table1.colInt"
FROM db.table1
WHERE $1;
`, true)
	assertStatement(t, SELECT(table1ColInt).FROM(table1).WHERE(table1ColInt.GT_EQ(Int(10))), `
SELECT table1.colInt AS "table1.colInt"
FROM db.table1
WHERE table1.colInt >= $1;
`, int64(10))
}

func TestSelectGroupBy(t *testing.T) {
	assertStatement(t, SELECT(table2ColInt).FROM(table2).GROUP_BY(table2ColFloat), `
SELECT table2.colInt AS "table2.colInt"
FROM db.table2
GROUP BY table2.colFloat;
`)
}

func TestSelectHaving(t *testing.T) {
	assertStatement(t, SELECT(table3ColInt).FROM(table3).HAVING(table1ColBool.EQ(Bool(true))), `
SELECT table3.colInt AS "table3.colInt"
FROM db.table3
HAVING table1.colBool = $1;
`, true)
}

func TestSelectOrderBy(t *testing.T) {
	assertStatement(t, SELECT(table2ColFloat).FROM(table2).ORDER_BY(table2ColInt.DESC()), `
SELECT table2.colFloat AS "table2.colFloat"
FROM db.table2
ORDER BY table2.colInt DESC;
`)
	assertStatement(t, SELECT(table2ColFloat).FROM(table2).ORDER_BY(table2ColInt.DESC(), table2ColInt.ASC()), `
SELECT table2.colFloat AS "table2.colFloat"
FROM db.table2
ORDER BY table2.colInt DESC, table2.colInt ASC;
`)
}

func TestSelectLimitOffset(t *testing.T) {
	assertStatement(t, SELECT(table2ColInt).FROM(table2).LIMIT(10), `
SELECT table2.colInt AS "table2.colInt"
FROM db.table2
LIMIT $1;
`, int64(10))
	assertStatement(t, SELECT(table2ColInt).FROM(table2).LIMIT(10).OFFSET(2), `
SELECT table2.colInt AS "table2.colInt"
FROM db.table2
LIMIT $1
OFFSET $2;
`, int64(10), int64(2))
}

func TestSelectLock(t *testing.T) {
	assertStatement(t, SELECT(table1ColBool).FROM(table1).FOR(UPDATE()), `
SELECT table1.colBool AS "table1.colBool"
FROM db.table1
FOR UPDATE;
`)
	assertStatement(t, SELECT(table1ColBool).FROM(table1).FOR(SHARE().NOWAIT()), `
SELECT table1.colBool AS "table1.colBool"
FROM db.table1
FOR SHARE NOWAIT;
`)

	assertStatement(t, SELECT(table1ColBool).FROM(table1).FOR(KEY_SHARE().NOWAIT()), `
SELECT table1.colBool AS "table1.colBool"
FROM db.table1
FOR KEY SHARE NOWAIT;
`)
	assertStatement(t, SELECT(table1ColBool).FROM(table1).FOR(NO_KEY_UPDATE().SKIP_LOCKED()), `
SELECT table1.colBool AS "table1.colBool"
FROM db.table1
FOR NO KEY UPDATE SKIP LOCKED;
`)
}
