package jet

// FloatExpression is interface for SQL float columns
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
	BETWEEN(min, max FloatExpression) BoolExpression
	NOT_BETWEEN(min, max FloatExpression) BoolExpression

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
	return Eq(n.parent, rhs)
}

func (n *floatInterfaceImpl) NOT_EQ(rhs FloatExpression) BoolExpression {
	return NotEq(n.parent, rhs)
}

func (n *floatInterfaceImpl) IS_DISTINCT_FROM(rhs FloatExpression) BoolExpression {
	return IsDistinctFrom(n.parent, rhs)
}

func (n *floatInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs FloatExpression) BoolExpression {
	return IsNotDistinctFrom(n.parent, rhs)
}

func (n *floatInterfaceImpl) GT(rhs FloatExpression) BoolExpression {
	return Gt(n.parent, rhs)
}

func (n *floatInterfaceImpl) GT_EQ(rhs FloatExpression) BoolExpression {
	return GtEq(n.parent, rhs)
}

func (n *floatInterfaceImpl) LT(rhs FloatExpression) BoolExpression {
	return Lt(n.parent, rhs)
}

func (n *floatInterfaceImpl) LT_EQ(rhs FloatExpression) BoolExpression {
	return LtEq(n.parent, rhs)
}

func (n *floatInterfaceImpl) BETWEEN(min, max FloatExpression) BoolExpression {
	return NewBetweenOperatorExpression(n.parent, min, max, false)
}

func (n *floatInterfaceImpl) NOT_BETWEEN(min, max FloatExpression) BoolExpression {
	return NewBetweenOperatorExpression(n.parent, min, max, true)
}

func (n *floatInterfaceImpl) ADD(rhs NumericExpression) FloatExpression {
	return FloatExp(Add(n.parent, rhs))
}

func (n *floatInterfaceImpl) SUB(rhs NumericExpression) FloatExpression {
	return FloatExp(Sub(n.parent, rhs))
}

func (n *floatInterfaceImpl) MUL(rhs NumericExpression) FloatExpression {
	return FloatExp(Mul(n.parent, rhs))
}

func (n *floatInterfaceImpl) DIV(rhs NumericExpression) FloatExpression {
	return FloatExp(Div(n.parent, rhs))
}

func (n *floatInterfaceImpl) MOD(rhs NumericExpression) FloatExpression {
	return FloatExp(Mod(n.parent, rhs))
}

func (n *floatInterfaceImpl) POW(rhs NumericExpression) FloatExpression {
	return POW(n.parent, rhs)
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
