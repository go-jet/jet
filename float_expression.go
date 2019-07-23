package jet

//FloatExpression is interface for SQL float columns
type FloatExpression interface {
	Expression
	numericExpression

	EQ(rhs FloatExpression) BoolExpression
	NOT_EQ(rhs FloatExpression) BoolExpression
	IS_DISTINCT_FROM(rhs FloatExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs FloatExpression) BoolExpression

	LT(rhs FloatExpression) BoolExpression
	LT_EQ(rhs FloatExpression) BoolExpression
	GT(rhs FloatExpression) BoolExpression
	GT_EQ(rhs FloatExpression) BoolExpression

	ADD(rhs NumericExpression) FloatExpression
	SUB(rhs NumericExpression) FloatExpression
	MUL(rhs NumericExpression) FloatExpression
	DIV(rhs NumericExpression) FloatExpression
	MOD(rhs NumericExpression) FloatExpression
	POW(rhs NumericExpression) FloatExpression
}

type floatInterfaceImpl struct {
	numericExpressionImpl
	parent FloatExpression
}

func (n *floatInterfaceImpl) EQ(rhs FloatExpression) BoolExpression {
	return eq(n.parent, rhs)
}

func (n *floatInterfaceImpl) NOT_EQ(rhs FloatExpression) BoolExpression {
	return notEq(n.parent, rhs)
}

func (n *floatInterfaceImpl) IS_DISTINCT_FROM(rhs FloatExpression) BoolExpression {
	return isDistinctFrom(n.parent, rhs)
}

func (n *floatInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs FloatExpression) BoolExpression {
	return isNotDistinctFrom(n.parent, rhs)
}

func (n *floatInterfaceImpl) GT(rhs FloatExpression) BoolExpression {
	return gt(n.parent, rhs)
}

func (n *floatInterfaceImpl) GT_EQ(rhs FloatExpression) BoolExpression {
	return gtEq(n.parent, rhs)
}

func (n *floatInterfaceImpl) LT(expression FloatExpression) BoolExpression {
	return lt(n.parent, expression)
}

func (n *floatInterfaceImpl) LT_EQ(expression FloatExpression) BoolExpression {
	return ltEq(n.parent, expression)
}

func (n *floatInterfaceImpl) ADD(expression NumericExpression) FloatExpression {
	return newBinaryFloatExpression(n.parent, expression, "+")
}

func (n *floatInterfaceImpl) SUB(expression NumericExpression) FloatExpression {
	return newBinaryFloatExpression(n.parent, expression, "-")
}

func (n *floatInterfaceImpl) MUL(expression NumericExpression) FloatExpression {
	return newBinaryFloatExpression(n.parent, expression, "*")
}

func (n *floatInterfaceImpl) DIV(expression NumericExpression) FloatExpression {
	return newBinaryFloatExpression(n.parent, expression, "/")
}

func (n *floatInterfaceImpl) MOD(expression NumericExpression) FloatExpression {
	return newBinaryFloatExpression(n.parent, expression, "%")
}

func (n *floatInterfaceImpl) POW(expression NumericExpression) FloatExpression {
	return newBinaryFloatExpression(n.parent, expression, "^")
}

//---------------------------------------------------//
type binaryFloatExpression struct {
	expressionInterfaceImpl
	floatInterfaceImpl

	binaryOpExpression
}

func newBinaryFloatExpression(lhs, rhs Expression, operator string) FloatExpression {
	floatExpression := binaryFloatExpression{}

	floatExpression.binaryOpExpression = newBinaryExpression(lhs, rhs, operator)

	floatExpression.expressionInterfaceImpl.parent = &floatExpression
	floatExpression.floatInterfaceImpl.parent = &floatExpression

	return &floatExpression
}

//---------------------------------------------------//

type floatExpressionWrapper struct {
	floatInterfaceImpl
	Expression
}

func newFloatExpressionWrap(expression Expression) FloatExpression {
	floatExpressionWrap := floatExpressionWrapper{Expression: expression}
	floatExpressionWrap.floatInterfaceImpl.parent = &floatExpressionWrap
	return &floatExpressionWrap
}

// FloatExp is date expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as float expression.
// Does not add sql cast to generated sql builder output.
func FloatExp(expression Expression) FloatExpression {
	return newFloatExpressionWrap(expression)
}
