package cubrid

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
}

func TestLEFT_JOIN(t *testing.T) {
	assertSerialize(t, table1.
		LEFT_JOIN(table2, table1ColInt.EQ(table2ColInt)),
		`db.table1
LEFT JOIN db.table2 ON (table1.col_int = table2.col_int)`)
}

func TestRIGHT_JOIN(t *testing.T) {
	assertSerialize(t, table1.
		RIGHT_JOIN(table2, table1ColInt.EQ(table2ColInt)),
		`db.table1
RIGHT JOIN db.table2 ON (table1.col_int = table2.col_int)`)
}

func TestFULL_JOIN(t *testing.T) {
	assertSerialize(t, table1.
		FULL_JOIN(table2, table1ColInt.EQ(table2ColInt)),
		`db.table1
FULL JOIN db.table2 ON (table1.col_int = table2.col_int)`)
}

func TestCROSS_JOIN(t *testing.T) {
	assertSerialize(t, table1.
		CROSS_JOIN(table2),
		`db.table1
CROSS JOIN db.table2`)
}
