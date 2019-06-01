package sqlbuilder

type IntegerExpression interface {
	expression

	EQ(rhs IntegerExpression) BoolExpression
	NOT_EQ(rhs IntegerExpression) BoolExpression
	IS_DISTINCT_FROM(rhs IntegerExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs IntegerExpression) BoolExpression

	LT(rhs IntegerExpression) BoolExpression
	LT_EQ(rhs IntegerExpression) BoolExpression
	GT(rhs IntegerExpression) BoolExpression
	GT_EQ(rhs IntegerExpression) BoolExpression

	ADD(rhs IntegerExpression) IntegerExpression
	SUB(rhs IntegerExpression) IntegerExpression
	MUL(rhs IntegerExpression) IntegerExpression
	DIV(rhs IntegerExpression) IntegerExpression
	MOD(rhs IntegerExpression) IntegerExpression
	POW(rhs IntegerExpression) IntegerExpression

	BIT_AND(expression IntegerExpression) IntegerExpression
	BIT_OR(expression IntegerExpression) IntegerExpression
	BIT_XOR(expression IntegerExpression) IntegerExpression
	BIT_NOT() IntegerExpression
	BIT_SHIFT_LEFT(intExpression IntegerExpression) IntegerExpression
	BIT_SHIFT_RIGHT(intExpression IntegerExpression) IntegerExpression
}

type integerInterfaceImpl struct {
	parent IntegerExpression
}

func (i *integerInterfaceImpl) EQ(rhs IntegerExpression) BoolExpression {
	return EQ(i.parent, rhs)
}

func (i *integerInterfaceImpl) NOT_EQ(rhs IntegerExpression) BoolExpression {
	return NOT_EQ(i.parent, rhs)
}

func (i *integerInterfaceImpl) IS_DISTINCT_FROM(rhs IntegerExpression) BoolExpression {
	return IS_DISTINCT_FROM(i.parent, rhs)
}

func (i *integerInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs IntegerExpression) BoolExpression {
	return IS_NOT_DISTINCT_FROM(i.parent, rhs)
}

func (i *integerInterfaceImpl) GT(rhs IntegerExpression) BoolExpression {
	return GT(i.parent, rhs)
}

func (i *integerInterfaceImpl) GT_EQ(rhs IntegerExpression) BoolExpression {
	return GT_EQ(i.parent, rhs)
}

func (i *integerInterfaceImpl) LT(expression IntegerExpression) BoolExpression {
	return LT(i.parent, expression)
}

func (i *integerInterfaceImpl) LT_EQ(expression IntegerExpression) BoolExpression {
	return LT_EQ(i.parent, expression)
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
