package cubrid

import (
	"testing"
)

func TestLATERAL(t *testing.T) {
	lateralQ := LATERAL(
		SELECT(table2ColInt).FROM(table2).WHERE(table2ColInt.EQ(table1ColInt)),
	).AS("lat")

	assertStatementSql(t,
		SELECT(table1ColInt, lateralQ.AllColumns()).
			FROM(table1.CROSS_JOIN(lateralQ)), `
SELECT table1.col_int AS "table1.col_int",
     lat."table2.col_int" AS "table2.col_int"
FROM db.table1
     CROSS JOIN LATERAL (
          SELECT table2.col_int AS "table2.col_int"
          FROM db.table2
          WHERE table2.col_int = table1.col_int
     ) AS lat;
`)
}
