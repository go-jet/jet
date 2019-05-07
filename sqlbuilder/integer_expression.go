package sqlbuilder

type integerExpression interface {
	numericExpression

	//AddInt(value int) integerExpression
	//AddInt64(value int) integerExpression

	BitAnd(expression integerExpression) integerExpression
	BitOr(expression integerExpression) integerExpression
	BitXor(expression integerExpression) integerExpression
	BitNot() integerExpression
}

type integerInterfaceImpl struct {
	parent integerExpression
}

//func (i *integerInterfaceImpl) AddInt(expression integerExpression) integerExpression {
//	return NewBinaryIntegerExpression(i.parent, expression, " & ")
//}
//
//func (i *integerInterfaceImpl) AddInt64(expression integerExpression) integerExpression {
//	return NewBinaryIntegerExpression(i.parent, expression, " & ")
//}

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

	binaryExpression
}

func NewBinaryIntegerExpression(lhs, rhs integerExpression, operator string) integerExpression {
	integerExpression := binaryIntegerExpression{}

	integerExpression.expressionInterfaceImpl.parent = &integerExpression
	integerExpression.numericInterfaceImpl.parent = &integerExpression
	integerExpression.integerInterfaceImpl.parent = &integerExpression

	integerExpression.binaryExpression = newBinaryExpression(lhs, rhs, operator)

	return &integerExpression
}

//---------------------------------------------------//
type prefixIntegerExpression struct {
	expressionInterfaceImpl
	numericInterfaceImpl
	integerInterfaceImpl

	prefixExpression
}

func NewPrefixIntegerExpression(expression integerExpression, operator string) integerExpression {
	integerExpression := prefixIntegerExpression{}
	integerExpression.prefixExpression = newPrefixExpression(expression, operator)

	integerExpression.expressionInterfaceImpl.parent = &integerExpression
	integerExpression.numericInterfaceImpl.parent = &integerExpression
	integerExpression.integerInterfaceImpl.parent = &integerExpression

	return &integerExpression
}
