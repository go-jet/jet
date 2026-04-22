package cubrid

import (
	"testing"
)

func TestUnion(t *testing.T) {
	select1 := SELECT(table1ColBool).FROM(table1)
	select2 := SELECT(table2ColBool).FROM(table2)

	assertStatementSql(t, select1.UNION(select2), `
(
     SELECT table1.col_bool AS "table1.col_bool"
     FROM db.table1
)
UNION
(
     SELECT table2.col_bool AS "table2.col_bool"
     FROM db.table2
);
`)
}

func TestUnionAll(t *testing.T) {
	select1 := SELECT(table1ColBool).FROM(table1)
	select2 := SELECT(table2ColBool).FROM(table2)

	assertStatementSql(t, select1.UNION_ALL(select2), `
(
     SELECT table1.col_bool AS "table1.col_bool"
     FROM db.table1
)
UNION ALL
(
     SELECT table2.col_bool AS "table2.col_bool"
     FROM db.table2
);
`)
}

func TestIntersect(t *testing.T) {
	select1 := SELECT(table1ColBool).FROM(table1)
	select2 := SELECT(table2ColBool).FROM(table2)

	assertStatementSql(t, select1.INTERSECT(select2), `
(
     SELECT table1.col_bool AS "table1.col_bool"
     FROM db.table1
)
INTERSECT
(
     SELECT table2.col_bool AS "table2.col_bool"
     FROM db.table2
);
`)
}

func TestExcept(t *testing.T) {
	select1 := SELECT(table1ColBool).FROM(table1)
	select2 := SELECT(table2ColBool).FROM(table2)

	assertStatementSql(t, select1.EXCEPT(select2), `
(
     SELECT table1.col_bool AS "table1.col_bool"
     FROM db.table1
)
EXCEPT
(
     SELECT table2.col_bool AS "table2.col_bool"
     FROM db.table2
);
`)
}

func TestIntersectAll(t *testing.T) {
	select1 := SELECT(table1ColBool).FROM(table1)
	select2 := SELECT(table2ColBool).FROM(table2)

	assertStatementSql(t, select1.INTERSECT_ALL(select2), `
(
     SELECT table1.col_bool AS "table1.col_bool"
     FROM db.table1
)
INTERSECT ALL
(
     SELECT table2.col_bool AS "table2.col_bool"
     FROM db.table2
);
`)
}

func TestExceptAll(t *testing.T) {
	select1 := SELECT(table1ColBool).FROM(table1)
	select2 := SELECT(table2ColBool).FROM(table2)

	assertStatementSql(t, select1.EXCEPT_ALL(select2), `
(
     SELECT table1.col_bool AS "table1.col_bool"
     FROM db.table1
)
EXCEPT ALL
(
     SELECT table2.col_bool AS "table2.col_bool"
     FROM db.table2
);
`)
}

func TestSetStatementOrderByLimitOffset(t *testing.T) {
	select1 := SELECT(table1ColBool).FROM(table1)
	select2 := SELECT(table2ColBool).FROM(table2)

	assertStatementSql(t,
		select1.UNION(select2).ORDER_BY(table1ColBool.ASC()).LIMIT(10).OFFSET(5), `
(
     SELECT table1.col_bool AS "table1.col_bool"
     FROM db.table1
)
UNION
(
     SELECT table2.col_bool AS "table2.col_bool"
     FROM db.table2
)
ORDER BY "table1.col_bool" ASC
LIMIT ?
OFFSET ?;
`, int64(10), int64(5))
}

func TestSetStatementAsTable(t *testing.T) {
	select1 := SELECT(table1ColBool).FROM(table1)
	select2 := SELECT(table2ColBool).FROM(table2)
	subQ := select1.UNION(select2).AsTable("combined")

	assertStatementSql(t,
		SELECT(subQ.AllColumns()).FROM(subQ), `
SELECT combined."table1.col_bool" AS "table1.col_bool"
FROM (
          (
               SELECT table1.col_bool AS "table1.col_bool"
               FROM db.table1
          )
          UNION
          (
               SELECT table2.col_bool AS "table2.col_bool"
               FROM db.table2
          )
     ) AS combined;
`)
}

func TestSetStatementIntersectAllFunc(t *testing.T) {
	select1 := SELECT(table1ColBool).FROM(table1)
	select2 := SELECT(table2ColBool).FROM(table2)

	assertStatementSql(t, INTERSECT_ALL(select1, select2), `
(
     SELECT table1.col_bool AS "table1.col_bool"
     FROM db.table1
)
INTERSECT ALL
(
     SELECT table2.col_bool AS "table2.col_bool"
     FROM db.table2
);
`)
}

func TestSetStatementExceptAllFunc(t *testing.T) {
	select1 := SELECT(table1ColBool).FROM(table1)
	select2 := SELECT(table2ColBool).FROM(table2)

	assertStatementSql(t, EXCEPT_ALL(select1, select2), `
(
     SELECT table1.col_bool AS "table1.col_bool"
     FROM db.table1
)
EXCEPT ALL
(
     SELECT table2.col_bool AS "table2.col_bool"
     FROM db.table2
);
`)
}
