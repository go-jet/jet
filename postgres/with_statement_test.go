package postgres

import (
	"testing"

	"github.com/go-jet/jet/v2/internal/testutils"
)

func TestCTE_Materialized(t *testing.T) {
	col := IntegerColumn("colo")
	tbl := NewTable("db", "t", "", col)

	c1 := CTE("c1")
	c2 := c1.ALIAS("c2")

	stmt := WITH(
		c1.AS_MATERIALIZED(
			SELECT(col).FROM(tbl),
		),
	)(
		SELECT(
			c1.AllColumns().As("c1.*"),
			c2.AllColumns().As("c2.*"),
		).FROM(
			c1.INNER_JOIN(c2, Bool(true)),
		),
	)

	testutils.AssertStatementSql(t, stmt, `
WITH c1 AS MATERIALIZED (
     SELECT t.colo AS "t.colo"
     FROM db.t
)
SELECT c1."t.colo" AS "c1.colo",
     c2."t.colo" AS "c2.colo"
FROM c1
     INNER JOIN c1 AS c2 ON $1::boolean;
`)
}

func TestCTE_NotMaterialized(t *testing.T) {
	col := IntegerColumn("colo")
	tbl := NewTable("db", "t", "", col)

	n := CTE("n1")

	stmt := WITH(
		n.AS_NOT_MATERIALIZED(SELECT(col).FROM(tbl)),
	)(
		SELECT(n.AllColumns().As("n1.*")).FROM(n),
	)

	testutils.AssertStatementSql(t, stmt, `
WITH n1 AS NOT MATERIALIZED (
     SELECT t.colo AS "t.colo"
     FROM db.t
)
SELECT n1."t.colo" AS "n1.colo"
FROM n1;
`)
}
