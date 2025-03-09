package jet

// IntervalExpression interface
type IntervalExpression interface {
	Expression
	isInterval()

	EQ(rhs IntervalExpression) BoolExpression
	NOT_EQ(rhs IntervalExpression) BoolExpression
	IS_DISTINCT_FROM(rhs IntervalExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs IntervalExpression) BoolExpression

	LT(rhs IntervalExpression) BoolExpression
	LT_EQ(rhs IntervalExpression) BoolExpression
	GT(rhs IntervalExpression) BoolExpression
	GT_EQ(rhs IntervalExpression) BoolExpression
	BETWEEN(min, max IntervalExpression) BoolExpression
	NOT_BETWEEN(min, max IntervalExpression) BoolExpression

	ADD(rhs IntervalExpression) IntervalExpression
	SUB(rhs IntervalExpression) IntervalExpression

	MUL(rhs NumericExpression) IntervalExpression
	DIV(rhs NumericExpression) IntervalExpression
}

type intervalInterfaceImpl struct {
	root IntervalExpression
}

func (i *intervalInterfaceImpl) isInterval() {}

func (i *intervalInterfaceImpl) EQ(rhs IntervalExpression) BoolExpression {
	return Eq(i.root, rhs)
}

func (i *intervalInterfaceImpl) NOT_EQ(rhs IntervalExpression) BoolExpression {
	return NotEq(i.root, rhs)
}

func (i *intervalInterfaceImpl) IS_DISTINCT_FROM(rhs IntervalExpression) BoolExpression {
	return IsDistinctFrom(i.root, rhs)
}

func (i *intervalInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs IntervalExpression) BoolExpression {
	return IsNotDistinctFrom(i.root, rhs)
}

func (i *intervalInterfaceImpl) LT(rhs IntervalExpression) BoolExpression {
	return Lt(i.root, rhs)
}

func (i *intervalInterfaceImpl) LT_EQ(rhs IntervalExpression) BoolExpression {
	return LtEq(i.root, rhs)
}

func (i *intervalInterfaceImpl) GT(rhs IntervalExpression) BoolExpression {
	return Gt(i.root, rhs)
}

func (i *intervalInterfaceImpl) GT_EQ(rhs IntervalExpression) BoolExpression {
	return GtEq(i.root, rhs)
}

func (i *intervalInterfaceImpl) BETWEEN(min, max IntervalExpression) BoolExpression {
	return NewBetweenOperatorExpression(i.root, min, max, false)
}

func (i *intervalInterfaceImpl) NOT_BETWEEN(min, max IntervalExpression) BoolExpression {
	return NewBetweenOperatorExpression(i.root, min, max, true)
}

func (i *intervalInterfaceImpl) ADD(rhs IntervalExpression) IntervalExpression {
	return IntervalExp(Add(i.root, rhs))
}

func (i *intervalInterfaceImpl) SUB(rhs IntervalExpression) IntervalExpression {
	return IntervalExp(Sub(i.root, rhs))
}

func (i *intervalInterfaceImpl) MUL(rhs NumericExpression) IntervalExpression {
	return IntervalExp(Mul(i.root, rhs))
}

func (i *intervalInterfaceImpl) DIV(rhs NumericExpression) IntervalExpression {
	return IntervalExp(Div(i.root, rhs))
}

type intervalWrapper struct {
	intervalInterfaceImpl
	Expression
}

func newIntervalExpressionWrap(expression Expression) IntervalExpression {
	intervalWrap := &intervalWrapper{Expression: expression}
	intervalWrap.intervalInterfaceImpl.root = intervalWrap
	expression.setRoot(intervalWrap)
	return intervalWrap
}

// IntervalExp is interval expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as interval expression.
// Does not add sql cast to generated sql builder output.
func IntervalExp(expression Expression) IntervalExpression {
	return newIntervalExpressionWrap(expression)
}

// Interval interface
type Interval interface {
	Serializer
	isInterval()
}
