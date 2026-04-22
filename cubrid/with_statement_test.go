package cubrid

import (
	"testing"
)

func TestWITH(t *testing.T) {
	cte := CTE("my_cte")

	stmt := WITH(
		cte.AS(SELECT(table1ColInt).FROM(table1)),
	)(
		SELECT(cte.AllColumns()).FROM(cte),
	)

	assertStatementSql(t, stmt, `
WITH my_cte AS (
     SELECT table1.col_int AS "table1.col_int"
     FROM db.table1
)
SELECT my_cte."table1.col_int" AS "table1.col_int"
FROM my_cte;
`)
}

func TestWITH_RECURSIVE(t *testing.T) {
	cte := CTE("rec_cte")

	stmt := WITH_RECURSIVE(
		cte.AS(SELECT(table1ColInt).FROM(table1)),
	)(
		SELECT(cte.AllColumns()).FROM(cte),
	)

	assertStatementSql(t, stmt, `
WITH RECURSIVE rec_cte AS (
     SELECT table1.col_int AS "table1.col_int"
     FROM db.table1
)
SELECT rec_cte."table1.col_int" AS "table1.col_int"
FROM rec_cte;
`)
}

func TestCTE_ALIAS(t *testing.T) {
	cte := CTE("my_cte")

	aliased := cte.ALIAS("alias1")

	assertStatementSql(t,
		WITH(
			cte.AS(SELECT(table1ColInt).FROM(table1)),
		)(
			SELECT(aliased.AllColumns()).FROM(aliased),
		), `
WITH my_cte AS (
     SELECT table1.col_int AS "table1.col_int"
     FROM db.table1
)
SELECT alias1."table1.col_int" AS "table1.col_int"
FROM my_cte AS alias1;
`)
}
