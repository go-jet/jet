package cubrid

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
SELECT ?,
     ?,
     ?
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
}

func TestSelectWhere(t *testing.T) {
	assertStatementSql(t, SELECT(table1ColInt).FROM(table1).WHERE(Bool(true)), `
SELECT table1.col_int AS "table1.col_int"
FROM db.table1
WHERE ?;
`, true)
	assertStatementSql(t, SELECT(table1ColInt).FROM(table1).WHERE(table1ColInt.GT_EQ(Int(10))), `
SELECT table1.col_int AS "table1.col_int"
FROM db.table1
WHERE table1.col_int >= ?;
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
HAVING table1.col_bool = ?;
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
LIMIT ?;
`, int64(10))
	assertStatementSql(t, SELECT(table2ColInt).FROM(table2).LIMIT(10).OFFSET(2), `
SELECT table2.col_int AS "table2.col_int"
FROM db.table2
LIMIT ?
OFFSET ?;
`, int64(10), int64(2))
}

func TestSelect_NOT_EXISTS(t *testing.T) {
	assertStatementSql(t,
		SELECT(table1ColInt).
			FROM(table1).
			WHERE(
				NOT(EXISTS(
					SELECT(table2ColInt).
						FROM(table2).
						WHERE(
							table1ColInt.EQ(table2ColInt),
						),
				))), `
SELECT table1.col_int AS "table1.col_int"
FROM db.table1
WHERE NOT (EXISTS (
           SELECT table2.col_int AS "table2.col_int"
           FROM db.table2
           WHERE table1.col_int = table2.col_int
      ));
`)
}

func TestSelectWINDOW(t *testing.T) {
	assertStatementSql(t,
		SELECT(table1ColInt, ROW_NUMBER().OVER(Window("w1"))).
			FROM(table1).
			WINDOW("w1").AS(ORDER_BY(table1ColInt.ASC())), `
SELECT table1.col_int AS "table1.col_int",
     ROW_NUMBER() OVER (w1)
FROM db.table1
WINDOW w1 AS (ORDER BY table1.col_int ASC);
`)
}

func TestSelectAsTable(t *testing.T) {
	subQ := SELECT(table1ColInt).FROM(table1).AsTable("sub")
	assertStatementSql(t,
		SELECT(subQ.AllColumns()).FROM(subQ), `
SELECT sub."table1.col_int" AS "table1.col_int"
FROM (
          SELECT table1.col_int AS "table1.col_int"
          FROM db.table1
     ) AS sub;
`)
}

func TestSelectOrderByNullsFirst(t *testing.T) {
	assertStatementSql(t, SELECT(table2ColFloat).FROM(table2).ORDER_BY(table2ColInt.ASC().NULLS_FIRST()), `
SELECT table2.col_float AS "table2.col_float"
FROM db.table2
ORDER BY table2.col_int ASC NULLS FIRST;
`)
}

func TestSelectOrderByNullsLast(t *testing.T) {
	assertStatementSql(t, SELECT(table2ColFloat).FROM(table2).ORDER_BY(table2ColInt.DESC().NULLS_LAST()), `
SELECT table2.col_float AS "table2.col_float"
FROM db.table2
ORDER BY table2.col_int DESC NULLS LAST;
`)
}

func TestSelectPRECEDING_FOLLOWING(t *testing.T) {
	assertStatementSql(t,
		SELECT(
			ROW_NUMBER().OVER(
				ORDER_BY(table1Col1.ASC()).ROWS(PRECEDING(1), FOLLOWING(1)),
			),
		).FROM(table1), `
SELECT ROW_NUMBER() OVER (ORDER BY table1.col1 ASC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
FROM db.table1;
`)
}
