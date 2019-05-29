package sqlbuilder

type integerExpression interface {
	numericExpression

	BitAnd(expression integerExpression) integerExpression
	BitOr(expression integerExpression) integerExpression
	BitXor(expression integerExpression) integerExpression
	BitNot() integerExpression
}

type integerInterfaceImpl struct {
	parent integerExpression
}

func (i *integerInterfaceImpl) BitAnd(expression integerExpression) integerExpression {
	return NewBinaryIntegerExpression(i.parent, expression, " & ")
}

func (i *integerInterfaceImpl) BitOr(expression integerExpression) integerExpression {
	return NewBinaryIntegerExpression(i.parent, expression, " | ")
}

func (i *integerInterfaceImpl) BitXor(expression integerExpression) integerExpression {
	return NewBinaryIntegerExpression(i.parent, expression, " # ")
}

func (i *integerInterfaceImpl) BitNot() integerExpression {
	return NewPrefixIntegerExpression(i.parent, " ~")
}

//---------------------------------------------------//
type binaryIntegerExpression struct {
	expressionInterfaceImpl
	numericInterfaceImpl
	integerInterfaceImpl

	binaryOpExpression
}

func NewBinaryIntegerExpression(lhs, rhs integerExpression, operator string) integerExpression {
	integerExpression := binaryIntegerExpression{}

	integerExpression.expressionInterfaceImpl.parent = &integerExpression
	integerExpression.numericInterfaceImpl.parent = &integerExpression
	integerExpression.integerInterfaceImpl.parent = &integerExpression

	integerExpression.binaryOpExpression = newBinaryExpression(lhs, rhs, operator)

	return &integerExpression
}

//---------------------------------------------------//
type prefixIntegerExpression struct {
	expressionInterfaceImpl
	numericInterfaceImpl
	integerInterfaceImpl

	prefixOpExpression
}

func NewPrefixIntegerExpression(expression integerExpression, operator string) integerExpression {
	integerExpression := prefixIntegerExpression{}
	integerExpression.prefixOpExpression = newPrefixExpression(expression, operator)

	integerExpression.expressionInterfaceImpl.parent = &integerExpression
	integerExpression.numericInterfaceImpl.parent = &integerExpression
	integerExpression.integerInterfaceImpl.parent = &integerExpression

	return &integerExpression
}
