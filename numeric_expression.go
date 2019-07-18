package jet

// NumericExpression is common interface for all integer and float expressions
type NumericExpression interface {
	Expression
	numericExpression
}

type numericExpression interface {
	isNumericExpression()
}

type numericExpressionImpl struct{}

func (n *numericExpressionImpl) isNumericExpression() {}
