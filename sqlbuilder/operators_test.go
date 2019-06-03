package sqlbuilder

import "testing"

func TestCase1(t *testing.T) {
	query := CASE().
		WHEN(table3Col1.EQ(Int(1))).THEN(table3Col1.ADD(Int(1))).
		WHEN(table3Col1.EQ(Int(2))).THEN(table3Col1.ADD(Int(2)))

	assertExpressionSerialize(t, query, `(CASE WHEN table3.col1 = $1 THEN table3.col1 + $2 WHEN table3.col1 = $3 THEN table3.col1 + $4 END)`,
		int64(1), int64(1), int64(2), int64(2))
}

func TestCase2(t *testing.T) {
	query := CASE(table3Col1).
		WHEN(Int(1)).THEN(table3Col1.ADD(Int(1))).
		WHEN(Int(2)).THEN(table3Col1.ADD(Int(2))).
		ELSE(Int(0))

	assertExpressionSerialize(t, query, `(CASE table3.col1 WHEN $1 THEN table3.col1 + $2 WHEN $3 THEN table3.col1 + $4 ELSE $5 END)`,
		int64(1), int64(1), int64(2), int64(2), int64(0))
}
