package sqlbuilder

import (
	"bytes"
	"gotest.tools/assert"
	"testing"
)

func TestBinaryExpression(t *testing.T) {
	boolExpression := Eq(Literal(2), Literal(3))

	out := bytes.Buffer{}
	err := boolExpression.SerializeSql(&out)

	assert.NilError(t, err)
	assert.Equal(t, out.String(), "2 = 3")

	t.Run("alias", func(t *testing.T) {
		alias := boolExpression.As("alias_eq_expression")

		out := bytes.Buffer{}
		err := alias.SerializeSql(&out)

		assert.NilError(t, err)
		assert.Equal(t, out.String(), `2 = 3 AS "alias_eq_expression"`)
	})

	t.Run("and", func(t *testing.T) {
		exp := boolExpression.And(Eq(Literal(4), Literal(5)))

		out := bytes.Buffer{}
		err := exp.SerializeSql(&out)

		assert.NilError(t, err)
		assert.Equal(t, out.String(), `(2 = 3 AND 4 = 5)`)
	})

	t.Run("or", func(t *testing.T) {
		exp := boolExpression.Or(Eq(Literal(4), Literal(5)))

		out := bytes.Buffer{}
		err := exp.SerializeSql(&out)

		assert.NilError(t, err)
		assert.Equal(t, out.String(), `(2 = 3 OR 4 = 5)`)
	})
}

func TestUnaryExpression(t *testing.T) {
	notExpression := Not(Eq(Literal(2), Literal(1)))

	out := bytes.Buffer{}
	err := notExpression.SerializeSql(&out)

	assert.NilError(t, err)
	assert.Equal(t, out.String(), " NOT 2 = 1")

	t.Run("alias", func(t *testing.T) {
		alias := notExpression.As("alias_not_expression")

		out := bytes.Buffer{}
		err := alias.SerializeSql(&out)

		assert.NilError(t, err)
		assert.Equal(t, out.String(), ` NOT 2 = 1 AS "alias_not_expression"`)
	})

	t.Run("and", func(t *testing.T) {
		exp := notExpression.And(Eq(Literal(4), Literal(5)))

		out := bytes.Buffer{}
		err := exp.SerializeSql(&out)

		assert.NilError(t, err)
		assert.Equal(t, out.String(), `( NOT 2 = 1 AND 4 = 5)`)
	})
}

func TestUnaryIsTrueExpression(t *testing.T) {
	notExpression := IsTrue(Eq(Literal(2), Literal(1)))

	out := bytes.Buffer{}
	err := notExpression.SerializeSql(&out)

	assert.NilError(t, err)
	assert.Equal(t, out.String(), " IS TRUE 2 = 1")

	t.Run("and", func(t *testing.T) {
		exp := notExpression.And(Eq(Literal(4), Literal(5)))

		out := bytes.Buffer{}
		err := exp.SerializeSql(&out)

		assert.NilError(t, err)
		assert.Equal(t, out.String(), `( IS TRUE 2 = 1 AND 4 = 5)`)
	})
}

func TestBoolLiteral(t *testing.T) {
	literal := NewBoolLiteralExpression(true)

	out := bytes.Buffer{}
	err := literal.SerializeSql(&out)

	assert.NilError(t, err)

	assert.Equal(t, out.String(), "true")
}
