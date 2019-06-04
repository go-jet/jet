package sqlbuilder

import "errors"

type FloatExpression interface {
	Expression

	EQ(rhs FloatExpression) BoolExpression
	NOT_EQ(rhs FloatExpression) BoolExpression
	IS_DISTINCT_FROM(rhs FloatExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs FloatExpression) BoolExpression

	LT(rhs FloatExpression) BoolExpression
	LT_EQ(rhs FloatExpression) BoolExpression
	GT(rhs FloatExpression) BoolExpression
	GT_EQ(rhs FloatExpression) BoolExpression

	ADD(rhs FloatExpression) FloatExpression
	SUB(rhs FloatExpression) FloatExpression
	MUL(rhs FloatExpression) FloatExpression
	DIV(rhs FloatExpression) FloatExpression
	MOD(rhs FloatExpression) FloatExpression
	POW(rhs FloatExpression) FloatExpression
}

type floatInterfaceImpl struct {
	parent FloatExpression
}

func (n *floatInterfaceImpl) EQ(rhs FloatExpression) BoolExpression {
	return EQ(n.parent, rhs)
}

func (n *floatInterfaceImpl) NOT_EQ(rhs FloatExpression) BoolExpression {
	return NOT_EQ(n.parent, rhs)
}

func (n *floatInterfaceImpl) IS_DISTINCT_FROM(rhs FloatExpression) BoolExpression {
	return IS_DISTINCT_FROM(n.parent, rhs)
}

func (n *floatInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs FloatExpression) BoolExpression {
	return IS_NOT_DISTINCT_FROM(n.parent, rhs)
}

func (n *floatInterfaceImpl) GT(rhs FloatExpression) BoolExpression {
	return GT(n.parent, rhs)
}

func (n *floatInterfaceImpl) GT_EQ(rhs FloatExpression) BoolExpression {
	return GT_EQ(n.parent, rhs)
}

func (n *floatInterfaceImpl) LT(expression FloatExpression) BoolExpression {
	return LT(n.parent, expression)
}

func (n *floatInterfaceImpl) LT_EQ(expression FloatExpression) BoolExpression {
	return LT_EQ(n.parent, expression)
}

func (n *floatInterfaceImpl) ADD(expression FloatExpression) FloatExpression {
	return newBinaryFloatExpression(n.parent, expression, "+")
}

func (n *floatInterfaceImpl) SUB(expression FloatExpression) FloatExpression {
	return newBinaryFloatExpression(n.parent, expression, "-")
}

func (n *floatInterfaceImpl) MUL(expression FloatExpression) FloatExpression {
	return newBinaryFloatExpression(n.parent, expression, "*")
}

func (n *floatInterfaceImpl) DIV(expression FloatExpression) FloatExpression {
	return newBinaryFloatExpression(n.parent, expression, "/")
}

func (n *floatInterfaceImpl) MOD(expression FloatExpression) FloatExpression {
	return newBinaryFloatExpression(n.parent, expression, "%")
}

func (n *floatInterfaceImpl) POW(expression FloatExpression) FloatExpression {
	return newBinaryFloatExpression(n.parent, expression, "^")
}

//---------------------------------------------------//
type binaryFloatExpression struct {
	expressionInterfaceImpl
	floatInterfaceImpl

	binaryOpExpression
}

func newBinaryFloatExpression(lhs, rhs FloatExpression, operator string) FloatExpression {
	floatExpression := binaryFloatExpression{}

	floatExpression.binaryOpExpression = newBinaryExpression(lhs, rhs, operator)

	floatExpression.expressionInterfaceImpl.parent = &floatExpression
	floatExpression.floatInterfaceImpl.parent = &floatExpression

	return &floatExpression
}

////---------------------------------------------------//
type floatExpressionWrapper struct {
	expressionInterfaceImpl
	floatInterfaceImpl

	expression Expression
}

func newFloatExpressionWrap(expression Expression) FloatExpression {
	floatExpressionWrap := floatExpressionWrapper{}

	floatExpressionWrap.expression = expression

	floatExpressionWrap.expressionInterfaceImpl.parent = &floatExpressionWrap
	floatExpressionWrap.floatInterfaceImpl.parent = &floatExpressionWrap

	return &floatExpressionWrap
}

func (n *floatExpressionWrapper) serialize(statement statementType, out *queryData, options ...serializeOption) error {
	if n == nil {
		return errors.New("Float expressions wrapper is nil. ")
	}
	//out.writeString("(")
	err := n.expression.serialize(statement, out)
	//out.writeString(")")

	return err
}
