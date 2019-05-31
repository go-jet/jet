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

	BitAnd(expression IntegerExpression) IntegerExpression
	BitOr(expression IntegerExpression) IntegerExpression
	BitXor(expression IntegerExpression) IntegerExpression
	BitNot() IntegerExpression
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
	return NewBinaryIntegerExpression(i.parent, expression, "+")
}

func (i *integerInterfaceImpl) SUB(expression IntegerExpression) IntegerExpression {
	return NewBinaryIntegerExpression(i.parent, expression, "-")
}

func (i *integerInterfaceImpl) MUL(expression IntegerExpression) IntegerExpression {
	return NewBinaryIntegerExpression(i.parent, expression, "*")
}

func (i *integerInterfaceImpl) DIV(expression IntegerExpression) IntegerExpression {
	return NewBinaryIntegerExpression(i.parent, expression, "/")
}

func (i *integerInterfaceImpl) BitAnd(expression IntegerExpression) IntegerExpression {
	return NewBinaryIntegerExpression(i.parent, expression, "&")
}

func (i *integerInterfaceImpl) BitOr(expression IntegerExpression) IntegerExpression {
	return NewBinaryIntegerExpression(i.parent, expression, "|")
}

func (i *integerInterfaceImpl) BitXor(expression IntegerExpression) IntegerExpression {
	return NewBinaryIntegerExpression(i.parent, expression, "#")
}

func (i *integerInterfaceImpl) BitNot() IntegerExpression {
	return NewPrefixIntegerOpExpression(i.parent, "~")
}

//---------------------------------------------------//
type binaryIntegerExpression struct {
	expressionInterfaceImpl
	integerInterfaceImpl

	binaryOpExpression
}

func NewBinaryIntegerExpression(lhs, rhs IntegerExpression, operator string) IntegerExpression {
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

func NewPrefixIntegerOpExpression(expression IntegerExpression, operator string) IntegerExpression {
	integerExpression := prefixIntegerOpExpression{}
	integerExpression.prefixOpExpression = newPrefixExpression(expression, operator)

	integerExpression.expressionInterfaceImpl.parent = &integerExpression
	integerExpression.integerInterfaceImpl.parent = &integerExpression

	return &integerExpression
}
