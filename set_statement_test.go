package jet

import (
	"gotest.tools/assert"
	"testing"
)

func TestUnionTwoSelect(t *testing.T) {
	var expectedSql = `
(
     (
          SELECT table1.col1 AS "table1.col1"
          FROM db.table1
     )
     UNION
     (
          SELECT table2.col3 AS "table2.col3"
          FROM db.table2
     )
);
`
	unionStmt1 := table1.
		SELECT(table1Col1).
		UNION(
			table2.SELECT(table2Col3),
		)

	unionStmt2 := UNION(table1.SELECT(table1Col1), table2.SELECT(table2Col3))

	assertStatement(t, unionStmt1, expectedSql)
	assertStatement(t, unionStmt2, expectedSql)
}

func TestUnionNilSelect(t *testing.T) {
	unionStmt := table1.
		SELECT(table1Col1).
		UNION(nil)

	assertStatementErr(t, unionStmt, "jet: select statement is nil")
}

func TestUnionThreeSelect1(t *testing.T) {

	unionStmt1 := table1.SELECT(table1Col1).
		UNION(
			table2.SELECT(table2Col3),
		).
		UNION(
			table3.SELECT(table3Col1),
		)

	var expectedSql = `
(
     
     (
          (
               SELECT table1.col1 AS "table1.col1"
               FROM db.table1
          )
          UNION
          (
               SELECT table2.col3 AS "table2.col3"
               FROM db.table2
          )
     )
     UNION
     (
          SELECT table3.col1 AS "table3.col1"
          FROM db.table3
     )
);
`

	assertStatement(t, unionStmt1, expectedSql)
}

func TestUnionThreeSelect2(t *testing.T) {

	unionStmt2 := UNION(
		table1.SELECT(table1Col1),
		table2.SELECT(table2Col3),
		table3.SELECT(table3Col1),
	)

	var expectedSql = `
(
     (
          SELECT table1.col1 AS "table1.col1"
          FROM db.table1
     )
     UNION
     (
          SELECT table2.col3 AS "table2.col3"
          FROM db.table2
     )
     UNION
     (
          SELECT table3.col1 AS "table3.col1"
          FROM db.table3
     )
);
`

	assertStatement(t, unionStmt2, expectedSql)
}

func TestUnionWithOrderBy(t *testing.T) {
	unionStmt := UNION(
		table1.SELECT(table1Col1),
		table2.SELECT(table2Col3),
	).
		ORDER_BY(table1Col1.ASC())

	assertStatement(t, unionStmt, `
(
     (
          SELECT table1.col1 AS "table1.col1"
          FROM db.table1
     )
     UNION
     (
          SELECT table2.col3 AS "table2.col3"
          FROM db.table2
     )
)
ORDER BY "table1.col1" ASC;
`)
}

func TestUnionWithLimitAndOffset(t *testing.T) {
	query, args, err := UNION(
		table1.SELECT(table1Col1),
		table2.SELECT(table2Col3),
	).
		LIMIT(10).
		OFFSET(11).Sql()

	assert.NilError(t, err)
	assert.Equal(t, query, `
(
     (
          SELECT table1.col1 AS "table1.col1"
          FROM db.table1
     )
     UNION
     (
          SELECT table2.col3 AS "table2.col3"
          FROM db.table2
     )
)
LIMIT $1
OFFSET $2;
`)
	assert.Equal(t, len(args), 2)
}

func TestUnionInUnion(t *testing.T) {
	expectedSql := `
(
     (
          SELECT table2.col3 AS "table2.col3",
               table2.col3 AS "table2.col3"
          FROM db.table2
     )
     UNION
     
     (
          (
               SELECT table1.col1 AS "table1.col1"
               FROM db.table1
          )
          UNION ALL
          (
               SELECT table2.col3 AS "table2.col3"
               FROM db.table2
          )
     )
);
`
	query := UNION(
		SELECT(table2Col3, table2Col3).FROM(table2),
		UNION_ALL(table1.SELECT(table1Col1), table2.SELECT(table2Col3)),
	)

	assertStatement(t, query, expectedSql)
}

func TestUnionALL(t *testing.T) {
	query, args, err := UNION_ALL(
		table1.SELECT(table1Col1),
		table2.SELECT(table2Col3),
	).Sql()

	assert.NilError(t, err)
	assert.Equal(t, query, `
(
     (
          SELECT table1.col1 AS "table1.col1"
          FROM db.table1
     )
     UNION ALL
     (
          SELECT table2.col3 AS "table2.col3"
          FROM db.table2
     )
);
`)
	assert.Equal(t, len(args), 0)
}

func TestINTERSECT(t *testing.T) {
	query, args, err := INTERSECT(
		table1.SELECT(table1Col1),
		table2.SELECT(table2Col3),
	).Sql()

	assert.NilError(t, err)
	assert.Equal(t, query, `
(
     (
          SELECT table1.col1 AS "table1.col1"
          FROM db.table1
     )
     INTERSECT
     (
          SELECT table2.col3 AS "table2.col3"
          FROM db.table2
     )
);
`)
	assert.Equal(t, len(args), 0)
}

func TestINTERSECT_ALL(t *testing.T) {
	query, args, err := INTERSECT_ALL(
		table1.SELECT(table1Col1),
		table2.SELECT(table2Col3),
	).Sql()

	assert.NilError(t, err)
	assert.Equal(t, query, `
(
     (
          SELECT table1.col1 AS "table1.col1"
          FROM db.table1
     )
     INTERSECT ALL
     (
          SELECT table2.col3 AS "table2.col3"
          FROM db.table2
     )
);
`)
	assert.Equal(t, len(args), 0)
}

func TestEXCEPT(t *testing.T) {
	query, args, err := EXCEPT(
		table1.SELECT(table1Col1),
		table2.SELECT(table2Col3),
	).Sql()

	assert.NilError(t, err)
	assert.Equal(t, query, `
(
     (
          SELECT table1.col1 AS "table1.col1"
          FROM db.table1
     )
     EXCEPT
     (
          SELECT table2.col3 AS "table2.col3"
          FROM db.table2
     )
);
`)
	assert.Equal(t, len(args), 0)
}

func TestEXCEPT_ALL(t *testing.T) {
	query, args, err := EXCEPT_ALL(
		table1.SELECT(table1Col1),
		table2.SELECT(table2Col3),
	).Sql()

	assert.NilError(t, err)
	assert.Equal(t, query, `
(
     (
          SELECT table1.col1 AS "table1.col1"
          FROM db.table1
     )
     EXCEPT ALL
     (
          SELECT table2.col3 AS "table2.col3"
          FROM db.table2
     )
);
`)
	assert.Equal(t, len(args), 0)
}
