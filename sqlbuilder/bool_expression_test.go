package sqlbuilder

import (
	"gotest.tools/assert"
	"testing"
)

func TestBinaryExpression(t *testing.T) {
	boolExpression := Eq(Literal(2), Literal(3))

	out := queryData{}
	err := boolExpression.Serialize(&out)

	assert.NilError(t, err)

	assert.Equal(t, out.buff.String(), "$1 = $2")
	assert.Equal(t, len(out.args), 2)

	t.Run("alias", func(t *testing.T) {
		alias := boolExpression.AS("alias_eq_expression")

		out := queryData{}
		err := alias.SerializeForProjection(&out)

		assert.NilError(t, err)
		assert.Equal(t, out.buff.String(), `$1 = $2 AS "alias_eq_expression"`)
	})

	t.Run("and", func(t *testing.T) {
		exp := boolExpression.And(Eq(Literal(4), Literal(5)))

		out := queryData{}
		err := exp.Serialize(&out)

		assert.NilError(t, err)
		assert.Equal(t, out.buff.String(), `($1 = $2 AND $3 = $4)`)
	})

	t.Run("or", func(t *testing.T) {
		exp := boolExpression.Or(Eq(Literal(4), Literal(5)))

		out := queryData{}
		err := exp.Serialize(&out)

		assert.NilError(t, err)
		assert.Equal(t, out.buff.String(), `($1 = $2 OR $3 = $4)`)
	})
}

func TestUnaryExpression(t *testing.T) {
	notExpression := Not(Eq(Literal(2), Literal(1)))

	out := queryData{}
	err := notExpression.Serialize(&out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), " NOT $1 = $2")

	t.Run("alias", func(t *testing.T) {
		alias := notExpression.AS("alias_not_expression")

		out := queryData{}
		err := alias.SerializeForProjection(&out)

		assert.NilError(t, err)
		assert.Equal(t, out.buff.String(), ` NOT $1 = $2 AS "alias_not_expression"`)
	})

	t.Run("and", func(t *testing.T) {
		exp := notExpression.And(Eq(Literal(4), Literal(5)))

		out := queryData{}
		err := exp.Serialize(&out)

		assert.NilError(t, err)
		assert.Equal(t, out.buff.String(), `( NOT $1 = $2 AND $3 = $4)`)
	})
}

func TestUnaryIsTrueExpression(t *testing.T) {
	notExpression := IsTrue(Eq(Literal(2), Literal(1)))

	out := queryData{}
	err := notExpression.Serialize(&out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), " IS TRUE $1 = $2")

	t.Run("and", func(t *testing.T) {
		exp := notExpression.And(Eq(Literal(4), Literal(5)))

		out := queryData{}
		err := exp.Serialize(&out)

		assert.NilError(t, err)
		assert.Equal(t, out.buff.String(), `( IS TRUE $1 = $2 AND $3 = $4)`)
	})
}

func TestBoolLiteral(t *testing.T) {
	literal := newBoolLiteralExpression(true)

	out := queryData{}
	err := literal.Serialize(&out)

	assert.NilError(t, err)

	assert.Equal(t, out.buff.String(), "$1")
}

func TestExists(t *testing.T) {
	query := EXISTS(
		table2.
			SELECT(Literal(1)).
			WHERE(table1Col1.Eq(table2Col3)),
	)

	out := queryData{}
	err := query.Serialize(&out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "EXISTS (SELECT $1 FROM db.table2 WHERE table1.col1 = table2.col3)")
}

func TestIn(t *testing.T) {
	query := Literal(1.11).IN(table1.SELECT(table1Col1))

	out := queryData{}
	err := query.Serialize(&out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), `$1 IN (SELECT table1.col1 AS "table1.col1" FROM db.table1)`)

	query2 := ROW(Literal(12), table1Col1).IN(table2.SELECT(table2Col3, table3Col1))

	out = queryData{}
	err = query2.Serialize(&out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), `(ROW($1, table1.col1) IN (SELECT table2.col3 AS "table2.col3", table3.col1 AS "table3.col1" FROM db.table2))`)
}
