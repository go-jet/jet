package sqlbuilder

import "errors"

type numericExpression interface {
	expression

	EQ(rhs numericExpression) boolExpression
	NOT_EQ(rhs numericExpression) boolExpression
	IS_DISTINCT_FROM(rhs numericExpression) boolExpression
	IS_NOT_DISTINCT_FROM(rhs numericExpression) boolExpression

	LT(rhs numericExpression) boolExpression
	LT_EQ(rhs numericExpression) boolExpression
	GT(rhs numericExpression) boolExpression
	GT_EQ(rhs numericExpression) boolExpression

	ADD(rhs numericExpression) numericExpression
	SUB(rhs numericExpression) numericExpression
	MUL(rhs numericExpression) numericExpression
	DIV(rhs numericExpression) numericExpression
}

type numericInterfaceImpl struct {
	parent numericExpression
}

func (n *numericInterfaceImpl) EQ(rhs numericExpression) boolExpression {
	return EQ(n.parent, rhs)
}

func (n *numericInterfaceImpl) NOT_EQ(rhs numericExpression) boolExpression {
	return NOT_EQ(n.parent, rhs)
}

func (n *numericInterfaceImpl) IS_DISTINCT_FROM(rhs numericExpression) boolExpression {
	return IS_DISTINCT_FROM(n.parent, rhs)
}

func (n *numericInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs numericExpression) boolExpression {
	return IS_NOT_DISTINCT_FROM(n.parent, rhs)
}

func (n *numericInterfaceImpl) GT(rhs numericExpression) boolExpression {
	return GT(n.parent, rhs)
}

func (n *numericInterfaceImpl) GT_EQ(rhs numericExpression) boolExpression {
	return GT_EQ(n.parent, rhs)
}

func (n *numericInterfaceImpl) LT(expression numericExpression) boolExpression {
	return LT(n.parent, expression)
}

func (n *numericInterfaceImpl) LT_EQ(expression numericExpression) boolExpression {
	return LT_EQ(n.parent, expression)
}

func (n *numericInterfaceImpl) ADD(expression numericExpression) numericExpression {
	return newBinaryNumericExpression(n.parent, expression, "+")
}

func (n *numericInterfaceImpl) SUB(expression numericExpression) numericExpression {
	return newBinaryNumericExpression(n.parent, expression, "-")
}

func (n *numericInterfaceImpl) MUL(expression numericExpression) numericExpression {
	return newBinaryNumericExpression(n.parent, expression, "*")
}

func (n *numericInterfaceImpl) DIV(expression numericExpression) numericExpression {
	return newBinaryNumericExpression(n.parent, expression, "/")
}

//---------------------------------------------------//
type binaryNumericExpression struct {
	expressionInterfaceImpl
	numericInterfaceImpl

	binaryOpExpression
}

func newBinaryNumericExpression(lhs, rhs expression, operator string) numericExpression {
	numericExpression := binaryNumericExpression{}

	numericExpression.binaryOpExpression = newBinaryExpression(lhs, rhs, operator)

	numericExpression.expressionInterfaceImpl.parent = &numericExpression
	numericExpression.numericInterfaceImpl.parent = &numericExpression

	return &numericExpression
}

//---------------------------------------------------//
type numericExpressionWrapper struct {
	expressionInterfaceImpl
	numericInterfaceImpl

	expression expression
}

func newNumericExpressionWrap(expression expression) numericExpression {
	numericExpressionWrap := numericExpressionWrapper{}

	numericExpressionWrap.expression = expression

	numericExpressionWrap.expressionInterfaceImpl.parent = &numericExpressionWrap
	numericExpressionWrap.numericInterfaceImpl.parent = &numericExpressionWrap

	return &numericExpressionWrap
}

func (n *numericExpressionWrapper) serialize(statement statementType, out *queryData) error {
	if n == nil {
		return errors.New("Numeric expression wrapper is nil. ")
	}
	//out.writeString("(")
	err := n.expression.serialize(statement, out)
	//out.writeString(")")

	return err
}
