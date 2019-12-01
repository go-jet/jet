package jet

// DateExpression is interface for date types
type DateExpression interface {
	Expression

	EQ(rhs DateExpression) BoolExpression
	NOT_EQ(rhs DateExpression) BoolExpression
	IS_DISTINCT_FROM(rhs DateExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs DateExpression) BoolExpression

	LT(rhs DateExpression) BoolExpression
	LT_EQ(rhs DateExpression) BoolExpression
	GT(rhs DateExpression) BoolExpression
	GT_EQ(rhs DateExpression) BoolExpression

	ADD(rhs Interval) TimestampExpression
	SUB(rhs Interval) TimestampExpression
}

type dateInterfaceImpl struct {
	parent DateExpression
}

func (d *dateInterfaceImpl) EQ(rhs DateExpression) BoolExpression {
	return eq(d.parent, rhs)
}

func (d *dateInterfaceImpl) NOT_EQ(rhs DateExpression) BoolExpression {
	return notEq(d.parent, rhs)
}

func (d *dateInterfaceImpl) IS_DISTINCT_FROM(rhs DateExpression) BoolExpression {
	return isDistinctFrom(d.parent, rhs)
}

func (d *dateInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs DateExpression) BoolExpression {
	return isNotDistinctFrom(d.parent, rhs)
}

func (d *dateInterfaceImpl) LT(rhs DateExpression) BoolExpression {
	return lt(d.parent, rhs)
}

func (d *dateInterfaceImpl) LT_EQ(rhs DateExpression) BoolExpression {
	return ltEq(d.parent, rhs)
}

func (d *dateInterfaceImpl) GT(rhs DateExpression) BoolExpression {
	return gt(d.parent, rhs)
}

func (d *dateInterfaceImpl) GT_EQ(rhs DateExpression) BoolExpression {
	return gtEq(d.parent, rhs)
}

func (d *dateInterfaceImpl) ADD(rhs Interval) TimestampExpression {
	return TimestampExp(newBinaryOperatorExpression(d.parent, rhs, "+"))
}

func (d *dateInterfaceImpl) SUB(rhs Interval) TimestampExpression {
	return TimestampExp(newBinaryOperatorExpression(d.parent, rhs, "-"))
}

//---------------------------------------------------//

type dateExpressionWrapper struct {
	dateInterfaceImpl
	Expression
}

func newDateExpressionWrap(expression Expression) DateExpression {
	dateExpressionWrap := dateExpressionWrapper{Expression: expression}
	dateExpressionWrap.dateInterfaceImpl.parent = &dateExpressionWrap
	return &dateExpressionWrap
}

// DateExp is date expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as date expression.
// Does not add sql cast to generated sql builder output.
func DateExp(expression Expression) DateExpression {
	return newDateExpressionWrap(expression)
}
