package jet

import "testing"

func TestOperatorNOT(t *testing.T) {
	notExpression := NOT(Int(2).EQ(Int(1)))

	assertClauseSerialize(t, NOT(table1ColBool), "(NOT table1.col_bool)")
	assertClauseSerialize(t, notExpression, "(NOT ($1 = $2))", int64(2), int64(1))
	assertProjectionSerialize(t, notExpression.AS("alias_not_expression"), `(NOT ($1 = $2)) AS "alias_not_expression"`, int64(2), int64(1))
	assertClauseSerialize(t, notExpression.AND(Int(4).EQ(Int(5))), `((NOT ($1 = $2)) AND ($3 = $4))`, int64(2), int64(1), int64(4), int64(5))
}

func TestCase1(t *testing.T) {
	query := CASE().
		WHEN(table3Col1.EQ(Int(1))).THEN(table3Col1.ADD(Int(1))).
		WHEN(table3Col1.EQ(Int(2))).THEN(table3Col1.ADD(Int(2)))

	assertClauseSerialize(t, query, `(CASE WHEN table3.col1 = $1 THEN table3.col1 + $2 WHEN table3.col1 = $3 THEN table3.col1 + $4 END)`,
		int64(1), int64(1), int64(2), int64(2))
}

func TestCase2(t *testing.T) {
	query := CASE(table3Col1).
		WHEN(Int(1)).THEN(table3Col1.ADD(Int(1))).
		WHEN(Int(2)).THEN(table3Col1.ADD(Int(2))).
		ELSE(Int(0))

	assertClauseSerialize(t, query, `(CASE table3.col1 WHEN $1 THEN table3.col1 + $2 WHEN $3 THEN table3.col1 + $4 ELSE $5 END)`,
		int64(1), int64(1), int64(2), int64(2), int64(0))
}
