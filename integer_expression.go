package jet

type IntegerExpression interface {
	Expression
	numericExpression

	// Check if expression is equal to rhs
	EQ(rhs IntegerExpression) BoolExpression
	// Check if expression is not equal to rhs
	NOT_EQ(rhs IntegerExpression) BoolExpression
	// Check if expression is distinct from rhs
	IS_DISTINCT_FROM(rhs IntegerExpression) BoolExpression
	// Check if expression is not distinct from rhs
	IS_NOT_DISTINCT_FROM(rhs IntegerExpression) BoolExpression

	// Check if expression is less then rhs
	LT(rhs IntegerExpression) BoolExpression
	// Check if expression is less then equal rhs
	LT_EQ(rhs IntegerExpression) BoolExpression
	// Check if expression is greater then rhs
	GT(rhs IntegerExpression) BoolExpression
	// Check if expression is greater then equal rhs
	GT_EQ(rhs IntegerExpression) BoolExpression

	// expression + rhs
	ADD(rhs IntegerExpression) IntegerExpression
	// expression - rhs
	SUB(rhs IntegerExpression) IntegerExpression
	// expression * rhs
	MUL(rhs IntegerExpression) IntegerExpression
	// expression / rhs
	DIV(rhs IntegerExpression) IntegerExpression
	// expression % rhs
	MOD(rhs IntegerExpression) IntegerExpression
	// expression ^ rhs
	POW(rhs IntegerExpression) IntegerExpression

	// expression & rhs
	BIT_AND(rhs IntegerExpression) IntegerExpression
	// expression | rhs
	BIT_OR(rhs IntegerExpression) IntegerExpression
	// expression # rhs
	BIT_XOR(rhs IntegerExpression) IntegerExpression
	// ~expression
	BIT_NOT() IntegerExpression
	// expression << rhs
	BIT_SHIFT_LEFT(shift IntegerExpression) IntegerExpression
	// expression >> rhs
	BIT_SHIFT_RIGHT(shift IntegerExpression) IntegerExpression
}

type integerInterfaceImpl struct {
	numericExpressionImpl
	parent IntegerExpression
}

func (i *integerInterfaceImpl) EQ(rhs IntegerExpression) BoolExpression {
	return eq(i.parent, rhs)
}

func (i *integerInterfaceImpl) NOT_EQ(rhs IntegerExpression) BoolExpression {
	return notEq(i.parent, rhs)
}

func (i *integerInterfaceImpl) IS_DISTINCT_FROM(rhs IntegerExpression) BoolExpression {
	return isDistinctFrom(i.parent, rhs)
}

func (i *integerInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs IntegerExpression) BoolExpression {
	return isNotDistinctFrom(i.parent, rhs)
}

func (i *integerInterfaceImpl) GT(rhs IntegerExpression) BoolExpression {
	return gt(i.parent, rhs)
}

func (i *integerInterfaceImpl) GT_EQ(rhs IntegerExpression) BoolExpression {
	return gtEq(i.parent, rhs)
}

func (i *integerInterfaceImpl) LT(expression IntegerExpression) BoolExpression {
	return lt(i.parent, expression)
}

func (i *integerInterfaceImpl) LT_EQ(expression IntegerExpression) BoolExpression {
	return ltEq(i.parent, expression)
}

func (i *integerInterfaceImpl) ADD(expression IntegerExpression) IntegerExpression {
	return newBinaryIntegerExpression(i.parent, expression, "+")
}

func (i *integerInterfaceImpl) SUB(expression IntegerExpression) IntegerExpression {
	return newBinaryIntegerExpression(i.parent, expression, "-")
}

func (i *integerInterfaceImpl) MUL(expression IntegerExpression) IntegerExpression {
	return newBinaryIntegerExpression(i.parent, expression, "*")
}

func (i *integerInterfaceImpl) DIV(expression IntegerExpression) IntegerExpression {
	return newBinaryIntegerExpression(i.parent, expression, "/")
}

func (n *integerInterfaceImpl) MOD(expression IntegerExpression) IntegerExpression {
	return newBinaryIntegerExpression(n.parent, expression, "%")
}

func (n *integerInterfaceImpl) POW(expression IntegerExpression) IntegerExpression {
	return newBinaryIntegerExpression(n.parent, expression, "^")
}

func (i *integerInterfaceImpl) BIT_AND(expression IntegerExpression) IntegerExpression {
	return newBinaryIntegerExpression(i.parent, expression, "&")
}

func (i *integerInterfaceImpl) BIT_OR(expression IntegerExpression) IntegerExpression {
	return newBinaryIntegerExpression(i.parent, expression, "|")
}

func (i *integerInterfaceImpl) BIT_XOR(expression IntegerExpression) IntegerExpression {
	return newBinaryIntegerExpression(i.parent, expression, "#")
}

func (i *integerInterfaceImpl) BIT_NOT() IntegerExpression {
	return newPrefixIntegerOpExpression(i.parent, "~")
}

func (i *integerInterfaceImpl) BIT_SHIFT_LEFT(intExpression IntegerExpression) IntegerExpression {
	return newBinaryIntegerExpression(i.parent, intExpression, "<<")
}

func (i *integerInterfaceImpl) BIT_SHIFT_RIGHT(intExpression IntegerExpression) IntegerExpression {
	return newBinaryIntegerExpression(i.parent, intExpression, ">>")
}

//---------------------------------------------------//
type binaryIntegerExpression struct {
	expressionInterfaceImpl
	integerInterfaceImpl

	binaryOpExpression
}

func newBinaryIntegerExpression(lhs, rhs IntegerExpression, operator string) IntegerExpression {
	integerExpression := binaryIntegerExpression{}

	integerExpression.expressionInterfaceImpl.parent = &integerExpression
	integerExpression.integerInterfaceImpl.parent = &integerExpression

	integerExpression.binaryOpExpression = newBinaryExpression(lhs, rhs, operator)

	return &integerExpression
}

//---------------------------------------------------//
type prefixIntegerOpExpression struct {
	expressionInterfaceImpl
	integerInterfaceImpl

	prefixOpExpression
}

func newPrefixIntegerOpExpression(expression IntegerExpression, operator string) IntegerExpression {
	integerExpression := prefixIntegerOpExpression{}
	integerExpression.prefixOpExpression = newPrefixExpression(expression, operator)

	integerExpression.expressionInterfaceImpl.parent = &integerExpression
	integerExpression.integerInterfaceImpl.parent = &integerExpression

	return &integerExpression
}

//---------------------------------------------------//
type integerExpressionWrapper struct {
	integerInterfaceImpl

	Expression
}

func newIntExpressionWrap(expression Expression) IntegerExpression {
	intExpressionWrap := integerExpressionWrapper{Expression: expression}

	intExpressionWrap.integerInterfaceImpl.parent = &intExpressionWrap

	return &intExpressionWrap
}

func IntExp(expression Expression) IntegerExpression {
	return newIntExpressionWrap(expression)
}
