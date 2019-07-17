package jet

type NumericExpression interface {
	Expression
	numericExpression
}

type numericExpression interface {
	isNumericExpression()
}

type numericExpressionImpl struct{}

func (n *numericExpressionImpl) isNumericExpression() {}
