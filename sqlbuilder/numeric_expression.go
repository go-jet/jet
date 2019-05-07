package sqlbuilder

type numericExpression interface {
	expression

	Eq(expression numericExpression) boolExpression
	EqL(literal interface{}) boolExpression
	NotEq(expression numericExpression) boolExpression
	NotEqL(literal interface{}) boolExpression

	Gt(rhs numericExpression) boolExpression
	GtEq(rhs numericExpression) boolExpression
	GtEqL(literal interface{}) boolExpression

	LtEq(rhs numericExpression) boolExpression
	LtEqL(literal interface{}) boolExpression

	Add(expression numericExpression) numericExpression
	Sub(expression numericExpression) numericExpression
	Mul(expression numericExpression) numericExpression
	Div(expression numericExpression) numericExpression
}

type numericInterfaceImpl struct {
	parent numericExpression
}

func (n *numericInterfaceImpl) Eq(expression numericExpression) boolExpression {
	return Eq(n.parent, expression)
}

func (n *numericInterfaceImpl) EqL(literal interface{}) boolExpression {
	return Eq(n.parent, Literal(literal))
}

func (n *numericInterfaceImpl) NotEq(expression numericExpression) boolExpression {
	return NotEq(n.parent, expression)
}

func (n *numericInterfaceImpl) NotEqL(literal interface{}) boolExpression {
	return NotEq(n.parent, Literal(literal))
}

func (n *numericInterfaceImpl) Gt(expression numericExpression) boolExpression {
	return Gt(n.parent, expression)
}

func (n *numericInterfaceImpl) GtEq(expression numericExpression) boolExpression {
	return GtEq(n.parent, expression)
}

func (n *numericInterfaceImpl) GtEqL(literal interface{}) boolExpression {
	return GtEq(n.parent, Literal(literal))
}

func (n *numericInterfaceImpl) LtEq(expression numericExpression) boolExpression {
	return LtEq(n.parent, expression)
}

func (n *numericInterfaceImpl) LtEqL(literal interface{}) boolExpression {
	return LtEq(n.parent, Literal(literal))
}

func (n *numericInterfaceImpl) Add(expression numericExpression) numericExpression {
	return newBinaryNumericExpression(n.parent, expression, "+")
}

func (n *numericInterfaceImpl) Sub(expression numericExpression) numericExpression {
	return newBinaryNumericExpression(n.parent, expression, "-")
}

func (n *numericInterfaceImpl) Mul(expression numericExpression) numericExpression {
	return newBinaryNumericExpression(n.parent, expression, "*")
}

func (n *numericInterfaceImpl) Div(expression numericExpression) numericExpression {
	return newBinaryNumericExpression(n.parent, expression, "/")
}

//---------------------------------------------------//
type numericLiteral struct {
	numericInterfaceImpl
	literalExpression
}

func NewNumericLiteral(value interface{}) numericExpression {
	numericLiteral := numericLiteral{}
	numericLiteral.literalExpression = *Literal(value)

	numericLiteral.numericInterfaceImpl.parent = &numericLiteral

	return &numericLiteral
}

//---------------------------------------------------//
type binaryNumericExpression struct {
	expressionInterfaceImpl
	numericInterfaceImpl

	binaryExpression
}

func newBinaryNumericExpression(lhs, rhs expression, operator string) numericExpression {
	numericExpression := binaryNumericExpression{}

	numericExpression.binaryExpression = newBinaryExpression(lhs, rhs, operator)

	numericExpression.expressionInterfaceImpl.parent = &numericExpression
	numericExpression.numericInterfaceImpl.parent = &numericExpression

	return &numericExpression
}

//---------------------------------------------------//
type numericExpressionWrapper struct {
	expressionInterfaceImpl
	numericInterfaceImpl

	expression expression
}

func newNumericExpressionWrap(expression expression) numericExpression {
	numericExpressionWrap := numericExpressionWrapper{}

	numericExpressionWrap.expression = expression

	numericExpressionWrap.expressionInterfaceImpl.parent = &numericExpressionWrap
	numericExpressionWrap.numericInterfaceImpl.parent = &numericExpressionWrap

	return &numericExpressionWrap
}

func (c *numericExpressionWrapper) serialize(out *queryData) error {
	out.WriteString("(")
	err := c.expression.serialize(out)
	out.WriteString(")")

	return err
}
