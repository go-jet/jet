package sqlbuilder

import (
	"fmt"
	"gotest.tools/assert"
	"testing"
)

func TestUnionNoSelect(t *testing.T) {
	query, args, err := UNION().Sql()

	assert.Assert(t, err != nil)
	//fmt.Println(err.Error())
	fmt.Print(query, args)
}

func TestUnionOneSelect(t *testing.T) {
	query, args, err := UNION(
		table1.SELECT(table1Col1),
	).Sql()

	assert.Assert(t, err != nil)
	fmt.Println(err.Error())
	fmt.Println(query)
	fmt.Println(args)
}

func TestUnionTwoSelect(t *testing.T) {
	query, args, err := UNION(
		table1.SELECT(table1Col1),
		table2.SELECT(table2Col3),
	).Sql()

	assert.NilError(t, err)
	assert.Equal(t, query, `((SELECT table1.col1 AS "table1.col1" FROM db.table1) UNION (SELECT table2.col3 AS "table2.col3" FROM db.table2))`)
	assert.Equal(t, len(args), 0)
}

func TestUnionThreeSelect(t *testing.T) {
	query, args, err := UNION(
		table1.SELECT(table1Col1),
		table2.SELECT(table2Col3),
		table3.SELECT(table3Col1),
	).Sql()

	assert.NilError(t, err)
	assert.Equal(t, query, `((SELECT table1.col1 AS "table1.col1" FROM db.table1) UNION (SELECT table2.col3 AS "table2.col3" FROM db.table2) UNION (SELECT table3.col1 AS "table3.col1" FROM db.table3))`)
	assert.Equal(t, len(args), 0)
}

func TestUnionWithOrderBy(t *testing.T) {
	query, args, err := UNION(
		table1.SELECT(table1Col1),
		table2.SELECT(table2Col3),
	).ORDER_BY(table1Col1.Asc()).Sql()

	assert.NilError(t, err)
	assert.Equal(t, query, `((SELECT table1.col1 AS "table1.col1" FROM db.table1) UNION (SELECT table2.col3 AS "table2.col3" FROM db.table2)) ORDER BY table1.col1 ASC`)
	assert.Equal(t, len(args), 0)
}

func TestUnionWithLimit(t *testing.T) {
	query, args, err := UNION(
		table1.SELECT(table1Col1),
		table2.SELECT(table2Col3),
	).LIMIT(10).OFFSET(11).Sql()

	assert.NilError(t, err)
	assert.Equal(t, query, `((SELECT table1.col1 AS "table1.col1" FROM db.table1) UNION (SELECT table2.col3 AS "table2.col3" FROM db.table2)) LIMIT $1 OFFSET $2`)
	assert.Equal(t, len(args), 2)
}
