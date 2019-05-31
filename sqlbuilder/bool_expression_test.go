package sqlbuilder

import (
	"fmt"
	"gotest.tools/assert"
	"testing"
)

func TestBoolExpressionEQ(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColBool.EQ(table2ColBool)), "(table1.colBool = table2.colBool)")
	assert.Equal(t, getTestSerialize(t, table1ColBool.AND(table2ColBool).EQ(table2ColBool)), "((table1.colBool AND table2.colBool) = table2.colBool)")
}

func TestBoolExpressionNOT_EQ(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColBool.NOT_EQ(table2ColBool)), "(table1.colBool != table2.colBool)")
	assert.Equal(t, getTestSerialize(t, table1ColBool.AND(table2ColBool).NOT_EQ(table2ColBool)), "((table1.colBool AND table2.colBool) != table2.colBool)")
}

func TestBoolExpressionIS_TRUE(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColBool.IS_TRUE()), "table1.colBool IS TRUE")
}

func TestBoolExpressionIS_NOT_TRUE(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColBool.IS_NOT_TRUE()), "table1.colBool IS NOT TRUE")
}

func TestBoolExpressionIS_FALSE(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColBool.IS_FALSE()), "table1.colBool IS FALSE")
}

func TestBoolExpressionIS_NOT_FALSE(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColBool.IS_NOT_FALSE()), "table1.colBool IS NOT FALSE")
}

func TestBoolExpressionIS_UNKNOWN(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColBool.IS_UNKNOWN()), "table1.colBool IS UNKNOWN")
}

func TestBoolExpressionIS_NOT_UNKNOWN(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table1ColBool.IS_NOT_UNKNOWN()), "table1.colBool IS NOT UNKNOWN")
}

func TestBinaryExpression(t *testing.T) {
	boolExpression := EQ(Literal(2), Literal(3))

	out := queryData{}
	err := boolExpression.serialize(select_statement, &out)

	assert.NilError(t, err)

	assert.Equal(t, out.buff.String(), "($1 = $2)")
	assert.Equal(t, len(out.args), 2)

	t.Run("alias", func(t *testing.T) {
		alias := boolExpression.AS("alias_eq_expression")

		out := queryData{}
		err := alias.serializeForProjection(select_statement, &out)

		assert.NilError(t, err)
		assert.Equal(t, out.buff.String(), `($1 = $2) AS "alias_eq_expression"`)
	})

	t.Run("and", func(t *testing.T) {
		exp := boolExpression.AND(EQ(Literal(4), Literal(5)))

		out := queryData{}
		err := exp.serialize(select_statement, &out)

		assert.NilError(t, err)
		assert.Equal(t, out.buff.String(), `(($1 = $2) AND ($3 = $4))`)
	})

	t.Run("or", func(t *testing.T) {
		exp := boolExpression.OR(EQ(Literal(4), Literal(5)))

		out := queryData{}
		err := exp.serialize(select_statement, &out)

		assert.NilError(t, err)
		assert.Equal(t, out.buff.String(), `(($1 = $2) OR ($3 = $4))`)
	})
}

func TestUnaryExpression(t *testing.T) {
	notExpression := NOT(EQ(Literal(2), Literal(1)))

	out := queryData{}
	err := notExpression.serialize(select_statement, &out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "NOT ($1 = $2)")

	t.Run("alias", func(t *testing.T) {
		alias := notExpression.AS("alias_not_expression")

		out := queryData{}
		err := alias.serializeForProjection(select_statement, &out)

		assert.NilError(t, err)
		assert.Equal(t, out.buff.String(), `NOT ($1 = $2) AS "alias_not_expression"`)
	})

	t.Run("and", func(t *testing.T) {
		exp := notExpression.AND(EQ(Literal(4), Literal(5)))

		out := queryData{}
		err := exp.serialize(select_statement, &out)

		assert.NilError(t, err)
		assert.Equal(t, out.buff.String(), `(NOT ($1 = $2) AND ($3 = $4))`)
	})
}

func TestUnaryIsTrueExpression(t *testing.T) {
	exp := IS_TRUE(EQ(Literal(2), Literal(1)))

	out := queryData{}
	err := exp.serialize(select_statement, &out)

	assert.NilError(t, err)
	assert.Equal(t, out.buff.String(), "($1 = $2) IS TRUE")

	t.Run("and", func(t *testing.T) {
		exp := exp.AND(EQ(Literal(4), Literal(5)))

		out := queryData{}
		err := exp.serialize(select_statement, &out)

		assert.NilError(t, err)
		assert.Equal(t, out.buff.String(), `(($1 = $2) IS TRUE AND ($3 = $4))`)
	})
}

func TestBoolLiteral(t *testing.T) {
	literal := Bool(true)

	out := queryData{}
	err := literal.serialize(select_statement, &out)

	assert.NilError(t, err)

	assert.Equal(t, out.buff.String(), "$1")
}

func TestExists(t *testing.T) {
	query := EXISTS(
		table2.
			SELECT(Literal(1)).
			WHERE(table1Col1.EQ(table2Col3)),
	)

	out := queryData{}
	err := query.serialize(select_statement, &out)

	fmt.Println(out.buff.String())

	assert.NilError(t, err)

	expectedSql :=
		`EXISTS (
     SELECT $1
     FROM db.table2
     WHERE table1.col1 = table2.col3
)`
	assert.Equal(t, out.buff.String(), expectedSql)
}

func TestIn(t *testing.T) {
	query := Literal(1.11).IN(table1.SELECT(table1Col1))

	out := queryData{}
	err := query.serialize(select_statement, &out, NO_WRAP)

	assert.NilError(t, err)
	fmt.Println(out.buff.String())
	assert.Equal(t, out.buff.String(), `$1 IN (
     SELECT table1.col1 AS "table1.col1"
     FROM db.table1
)`)

	query2 := ROW(Literal(12), table1Col1).IN(table2.SELECT(table2Col3, table3Col1))

	out = queryData{}
	err = query2.serialize(select_statement, &out)

	assert.NilError(t, err)
	fmt.Println(out.buff.String())
	assert.Equal(t, out.buff.String(), `(ROW($1, table1.col1) IN (
     SELECT table2.col3 AS "table2.col3",
          table3.col1 AS "table3.col1"
     FROM db.table2
))`)
}
