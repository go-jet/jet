package sqlbuilder

import (
	"gotest.tools/assert"
	"testing"
)

func TestCase1(t *testing.T) {
	query := CASE().
		WHEN(table3Col1.EqL(1)).THEN(table3Col1.Add(IntLiteral(1))).
		WHEN(table3Col1.EqL(2)).THEN(table3Col1.Add(IntLiteral(2)))

	queryData := &queryData{}

	err := query.Serialize(queryData)

	assert.NilError(t, err)
	assert.Equal(t, queryData.buff.String(), `(CASE WHEN table3.col1 = $1 THEN table3.col1 + $2 WHEN table3.col1 = $3 THEN table3.col1 + $4 END)`)
}

func TestCase2(t *testing.T) {
	query := CASE(table3Col1).
		WHEN(IntLiteral(1)).THEN(table3Col1.Add(IntLiteral(1))).
		WHEN(IntLiteral(2)).THEN(table3Col1.Add(IntLiteral(2))).
		ELSE(IntLiteral(0))

	queryData := &queryData{}

	err := query.Serialize(queryData)

	assert.NilError(t, err)
	assert.Equal(t, queryData.buff.String(), `(CASE table3.col1 WHEN $1 THEN table3.col1 + $2 WHEN $3 THEN table3.col1 + $4 ELSE $5 END)`)
}
