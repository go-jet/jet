package sqlbuilder

type NumericExpression interface {
	Expression

	Eq(expression NumericExpression) BoolExpression
	EqL(literal interface{}) BoolExpression
	NotEq(expression NumericExpression) BoolExpression
	NotEqL(literal interface{}) BoolExpression

	Gt(rhs NumericExpression) BoolExpression
	GtEq(rhs NumericExpression) BoolExpression
	GtEqL(literal interface{}) BoolExpression

	LtEq(rhs NumericExpression) BoolExpression
	LtEqL(literal interface{}) BoolExpression

	Add(expression NumericExpression) NumericExpression
	Sub(expression NumericExpression) NumericExpression
	Mul(expression NumericExpression) NumericExpression
	Div(expression NumericExpression) NumericExpression
}

type numericInterfaceImpl struct {
	parent NumericExpression
}

func (n *numericInterfaceImpl) Eq(expression NumericExpression) BoolExpression {
	return Eq(n.parent, expression)
}

func (n *numericInterfaceImpl) EqL(literal interface{}) BoolExpression {
	return Eq(n.parent, Literal(literal))
}

func (n *numericInterfaceImpl) NotEq(expression NumericExpression) BoolExpression {
	return NotEq(n.parent, expression)
}

func (n *numericInterfaceImpl) NotEqL(literal interface{}) BoolExpression {
	return NotEq(n.parent, Literal(literal))
}

func (n *numericInterfaceImpl) Gt(expression NumericExpression) BoolExpression {
	return Gt(n.parent, expression)
}

func (n *numericInterfaceImpl) GtEq(expression NumericExpression) BoolExpression {
	return GtEq(n.parent, expression)
}

func (n *numericInterfaceImpl) GtEqL(literal interface{}) BoolExpression {
	return GtEq(n.parent, Literal(literal))
}

func (n *numericInterfaceImpl) LtEq(expression NumericExpression) BoolExpression {
	return LtEq(n.parent, expression)
}

func (n *numericInterfaceImpl) LtEqL(literal interface{}) BoolExpression {
	return LtEq(n.parent, Literal(literal))
}

func (n *numericInterfaceImpl) Add(expression NumericExpression) NumericExpression {
	return newBinaryNumericExpression(n.parent, expression, " + ")
}

func (n *numericInterfaceImpl) Sub(expression NumericExpression) NumericExpression {
	return newBinaryNumericExpression(n.parent, expression, " - ")
}

func (n *numericInterfaceImpl) Mul(expression NumericExpression) NumericExpression {
	return newBinaryNumericExpression(n.parent, expression, " * ")
}

func (n *numericInterfaceImpl) Div(expression NumericExpression) NumericExpression {
	return newBinaryNumericExpression(n.parent, expression, " / ")
}

//---------------------------------------------------//
type numericLiteral struct {
	numericInterfaceImpl
	literalExpression
}

func NewNumericLiteral(value interface{}) NumericExpression {
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

func newBinaryNumericExpression(lhs, rhs Expression, operator string) NumericExpression {
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

	expression Expression
}

func newNumericExpressionWrap(expression Expression) NumericExpression {
	numericExpressionWrap := numericExpressionWrapper{}

	numericExpressionWrap.expression = expression

	numericExpressionWrap.expressionInterfaceImpl.parent = &numericExpressionWrap
	numericExpressionWrap.numericInterfaceImpl.parent = &numericExpressionWrap

	return &numericExpressionWrap
}

func (c *numericExpressionWrapper) Serialize(out *queryData, options ...serializeOption) error {
	out.WriteString("(")
	err := c.expression.Serialize(out, options...)
	out.WriteString(")")

	return err
}
