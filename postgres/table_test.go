package postgres

import (
	"testing"
)

func TestJoinNilInputs(t *testing.T) {
	assertSerializeErr(t, table2.INNER_JOIN(nil, table1ColBool.EQ(table2ColBool)),
		"jet: right hand side of join operation is nil table")
	assertSerializeErr(t, table2.INNER_JOIN(table1, nil),
		"jet: join condition is nil")
}

func TestINNER_JOIN(t *testing.T) {
	assertSerialize(t, table1.
		INNER_JOIN(table2, table1ColInt.EQ(table2ColInt)),
		`db.table1
INNER JOIN db.table2 ON (table1.col_int = table2.col_int)`)
	assertSerialize(t, table1.
		INNER_JOIN(table2, table1ColInt.EQ(table2ColInt)).
		INNER_JOIN(table3, table1ColInt.EQ(table3ColInt)),
		`db.table1
INNER JOIN db.table2 ON (table1.col_int = table2.col_int)
INNER JOIN db.table3 ON (table1.col_int = table3.col_int)`)
	assertSerialize(t, table1.
		INNER_JOIN(table2, table1ColInt.EQ(Int(1))).
		INNER_JOIN(table3, table1ColInt.EQ(Int(2))),
		`db.table1
INNER JOIN db.table2 ON (table1.col_int = $1)
INNER JOIN db.table3 ON (table1.col_int = $2)`, int64(1), int64(2))
}

func TestLEFT_JOIN(t *testing.T) {
	assertSerialize(t, table1.
		LEFT_JOIN(table2, table1ColInt.EQ(table2ColInt)),
		`db.table1
LEFT JOIN db.table2 ON (table1.col_int = table2.col_int)`)
	assertSerialize(t, table1.
		LEFT_JOIN(table2, table1ColInt.EQ(table2ColInt)).
		LEFT_JOIN(table3, table1ColInt.EQ(table3ColInt)),
		`db.table1
LEFT JOIN db.table2 ON (table1.col_int = table2.col_int)
LEFT JOIN db.table3 ON (table1.col_int = table3.col_int)`)
	assertSerialize(t, table1.
		LEFT_JOIN(table2, table1ColInt.EQ(Int(1))).
		LEFT_JOIN(table3, table1ColInt.EQ(Int(2))),
		`db.table1
LEFT JOIN db.table2 ON (table1.col_int = $1)
LEFT JOIN db.table3 ON (table1.col_int = $2)`, int64(1), int64(2))
}

func TestRIGHT_JOIN(t *testing.T) {
	assertSerialize(t, table1.
		RIGHT_JOIN(table2, table1ColInt.EQ(table2ColInt)),
		`db.table1
RIGHT JOIN db.table2 ON (table1.col_int = table2.col_int)`)
	assertSerialize(t, table1.
		RIGHT_JOIN(table2, table1ColInt.EQ(table2ColInt)).
		RIGHT_JOIN(table3, table1ColInt.EQ(table3ColInt)),
		`db.table1
RIGHT JOIN db.table2 ON (table1.col_int = table2.col_int)
RIGHT JOIN db.table3 ON (table1.col_int = table3.col_int)`)
	assertSerialize(t, table1.
		RIGHT_JOIN(table2, table1ColInt.EQ(Int(1))).
		RIGHT_JOIN(table3, table1ColInt.EQ(Int(2))),
		`db.table1
RIGHT JOIN db.table2 ON (table1.col_int = $1)
RIGHT JOIN db.table3 ON (table1.col_int = $2)`, int64(1), int64(2))
}

func TestFULL_JOIN(t *testing.T) {
	assertSerialize(t, table1.
		FULL_JOIN(table2, table1ColInt.EQ(table2ColInt)),
		`db.table1
FULL JOIN db.table2 ON (table1.col_int = table2.col_int)`)
	assertSerialize(t, table1.
		FULL_JOIN(table2, table1ColInt.EQ(table2ColInt)).
		FULL_JOIN(table3, table1ColInt.EQ(table3ColInt)),
		`db.table1
FULL JOIN db.table2 ON (table1.col_int = table2.col_int)
FULL JOIN db.table3 ON (table1.col_int = table3.col_int)`)
	assertSerialize(t, table1.
		FULL_JOIN(table2, table1ColInt.EQ(Int(1))).
		FULL_JOIN(table3, table1ColInt.EQ(Int(2))),
		`db.table1
FULL JOIN db.table2 ON (table1.col_int = $1)
FULL JOIN db.table3 ON (table1.col_int = $2)`, int64(1), int64(2))
}

func TestCROSS_JOIN(t *testing.T) {
	assertSerialize(t, table1.
		CROSS_JOIN(table2),
		`db.table1
CROSS JOIN db.table2`)
	assertSerialize(t, table1.
		CROSS_JOIN(table2).
		CROSS_JOIN(table3),
		`db.table1
CROSS JOIN db.table2
CROSS JOIN db.table3`)
}

func TestImplicitCROSS_JOIN(t *testing.T) {
	assertDebugStatementSql(t,
		SELECT(table1Col1, table2Col3).
			FROM(table1, table2),
		`
SELECT table1.col1 AS "table1.col1",
     table2.col3 AS "table2.col3"
FROM db.table1,
     db.table2;
`)
	assertDebugStatementSql(t,
		SELECT(
			table1Col1, table2Col3,
		).FROM(table1, table2, table3),
		`
SELECT table1.col1 AS "table1.col1",
     table2.col3 AS "table2.col3"
FROM db.table1,
     db.table2,
     db.table3;
`)
}
