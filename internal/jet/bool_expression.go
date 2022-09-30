package jet

// BoolExpression interface
type BoolExpression interface {
	Expression

	// Check if this expression is equal to rhs
	EQ(rhs BoolExpression) BoolExpression
	// Check if this expression is not equal to rhs
	NOT_EQ(rhs BoolExpression) BoolExpression
	// Check if this expression is distinct to rhs
	IS_DISTINCT_FROM(rhs BoolExpression) BoolExpression
	// Check if this expression is not distinct to rhs
	IS_NOT_DISTINCT_FROM(rhs BoolExpression) BoolExpression

	// Check if this expression is true
	IS_TRUE() BoolExpression
	// Check if this expression is not true
	IS_NOT_TRUE() BoolExpression
	// Check if this expression is false
	IS_FALSE() BoolExpression
	// Check if this expression is not false
	IS_NOT_FALSE() BoolExpression
	// Check if this expression is unknown
	IS_UNKNOWN() BoolExpression
	// Check if this expression is not unknown
	IS_NOT_UNKNOWN() BoolExpression

	// expression AND operator rhs
	AND(rhs BoolExpression) BoolExpression
	// expression OR operator rhs
	OR(rhs BoolExpression) BoolExpression
}

type boolInterfaceImpl struct {
	parent BoolExpression
}

func (b *boolInterfaceImpl) EQ(expression BoolExpression) BoolExpression {
	return Eq(b.parent, expression)
}

func (b *boolInterfaceImpl) NOT_EQ(expression BoolExpression) BoolExpression {
	return NotEq(b.parent, expression)
}

func (b *boolInterfaceImpl) IS_DISTINCT_FROM(rhs BoolExpression) BoolExpression {
	return IsDistinctFrom(b.parent, rhs)
}

func (b *boolInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs BoolExpression) BoolExpression {
	return IsNotDistinctFrom(b.parent, rhs)
}

func (b *boolInterfaceImpl) AND(expression BoolExpression) BoolExpression {
	return newBinaryBoolOperatorExpression(b.parent, expression, "AND")
}

func (b *boolInterfaceImpl) OR(expression BoolExpression) BoolExpression {
	return newBinaryBoolOperatorExpression(b.parent, expression, "OR")
}

func (b *boolInterfaceImpl) IS_TRUE() BoolExpression {
	return newPostfixBoolOperatorExpression(b.parent, "IS TRUE")
}

func (b *boolInterfaceImpl) IS_NOT_TRUE() BoolExpression {
	return newPostfixBoolOperatorExpression(b.parent, "IS NOT TRUE")
}

func (b *boolInterfaceImpl) IS_FALSE() BoolExpression {
	return newPostfixBoolOperatorExpression(b.parent, "IS FALSE")
}

func (b *boolInterfaceImpl) IS_NOT_FALSE() BoolExpression {
	return newPostfixBoolOperatorExpression(b.parent, "IS NOT FALSE")
}

func (b *boolInterfaceImpl) IS_UNKNOWN() BoolExpression {
	return newPostfixBoolOperatorExpression(b.parent, "IS UNKNOWN")
}

func (b *boolInterfaceImpl) IS_NOT_UNKNOWN() BoolExpression {
	return newPostfixBoolOperatorExpression(b.parent, "IS NOT UNKNOWN")
}

func newBinaryBoolOperatorExpression(lhs, rhs Expression, operator string, additionalParams ...Expression) BoolExpression {
	return BoolExp(NewBinaryOperatorExpression(lhs, rhs, operator, additionalParams...))
}

func newPrefixBoolOperatorExpression(expression Expression, operator string) BoolExpression {
	return BoolExp(newPrefixOperatorExpression(expression, operator))
}

func newPostfixBoolOperatorExpression(expression Expression, operator string) BoolExpression {
	return BoolExp(newPostfixOperatorExpression(expression, operator))
}

type boolExpressionWrapper struct {
	boolInterfaceImpl
	Expression
}

func newBoolExpressionWrap(expression Expression) BoolExpression {
	boolExpressionWrap := boolExpressionWrapper{Expression: expression}
	boolExpressionWrap.boolInterfaceImpl.parent = &boolExpressionWrap
	return &boolExpressionWrap
}

// BoolExp is bool expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as bool expression.
// Does not add sql cast to generated sql builder output.
func BoolExp(expression Expression) BoolExpression {
	return newBoolExpressionWrap(expression)
}
