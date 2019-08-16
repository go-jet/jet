package postgres

import (
	"testing"
)

func TestSelectSets(t *testing.T) {
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
