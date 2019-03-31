package sqlbuilder

import (
	"github.com/dropbox/godropbox/database/sqltypes"
	"github.com/pkg/errors"
)

type NumericExpression interface {
	Expression

	Eq(expression NumericExpression) BoolExpression
	NotEq(expression NumericExpression) BoolExpression
	GtEq(rhs NumericExpression) BoolExpression
	LtEq(rhs NumericExpression) BoolExpression

	Add(expression NumericExpression) NumericExpression
	Sub(expression NumericExpression) NumericExpression
	Mul(expression NumericExpression) NumericExpression
	Div(expression NumericExpression) NumericExpression
}

type numericInterfaceImpl struct {
	parent NumericExpression
}

func (n *numericInterfaceImpl) Eq(expression NumericExpression) BoolExpression {
	return Eq(n.parent, expression)
}

func (n *numericInterfaceImpl) NotEq(expression NumericExpression) BoolExpression {
	return Neq(n.parent, expression)
}

func (n *numericInterfaceImpl) GtEq(expression NumericExpression) BoolExpression {
	return Gte(n.parent, expression)
}

func (n *numericInterfaceImpl) LtEq(expression NumericExpression) BoolExpression {
	return Lte(n.parent, expression)
}

func (n *numericInterfaceImpl) Add(expression NumericExpression) NumericExpression {
	return newBinaryNumericExpression(n.parent, expression, []byte(" + "))
}

func (n *numericInterfaceImpl) Sub(expression NumericExpression) NumericExpression {
	return newBinaryNumericExpression(n.parent, expression, []byte(" - "))
}

func (n *numericInterfaceImpl) Mul(expression NumericExpression) NumericExpression {
	return newBinaryNumericExpression(n.parent, expression, []byte(" * "))
}

func (n *numericInterfaceImpl) Div(expression NumericExpression) NumericExpression {
	return newBinaryNumericExpression(n.parent, expression, []byte(" / "))
}

//---------------------------------------------------//
type numericLiteral struct {
	numericInterfaceImpl
	literalExpression
}

func NewNumericLiteral(value interface{}) NumericExpression {
	numericLiteral := numericLiteral{}

	sqlValue, err := sqltypes.BuildValue(value)
	if err != nil {
		panic(errors.Wrap(err, "Invalid literal value"))
	}
	numericLiteral.literalExpression = *NewLiteralExpression(sqlValue)
	numericLiteral.numericInterfaceImpl.parent = &numericLiteral

	return &numericLiteral
}

//---------------------------------------------------//
type binaryNumericExpression struct {
	expressionInterfaceImpl
	numericInterfaceImpl

	binaryExpression
}

func newBinaryNumericExpression(lhs, rhs Expression, operator []byte) NumericExpression {
	numericExpression := binaryNumericExpression{}

	numericExpression.binaryExpression = newBinaryExpression(lhs, rhs, operator)

	numericExpression.expressionInterfaceImpl.parent = &numericExpression
	numericExpression.numericInterfaceImpl.parent = &numericExpression

	return &numericExpression
}
