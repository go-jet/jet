package jet

// TimeExpression interface
type TimeExpression interface {
	Expression

	EQ(rhs TimeExpression) BoolExpression
	NOT_EQ(rhs TimeExpression) BoolExpression
	IS_DISTINCT_FROM(rhs TimeExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs TimeExpression) BoolExpression

	LT(rhs TimeExpression) BoolExpression
	LT_EQ(rhs TimeExpression) BoolExpression
	GT(rhs TimeExpression) BoolExpression
	GT_EQ(rhs TimeExpression) BoolExpression
}

type timeInterfaceImpl struct {
	parent TimeExpression
}

func (t *timeInterfaceImpl) EQ(rhs TimeExpression) BoolExpression {
	return eq(t.parent, rhs)
}

func (t *timeInterfaceImpl) NOT_EQ(rhs TimeExpression) BoolExpression {
	return notEq(t.parent, rhs)
}

func (t *timeInterfaceImpl) IS_DISTINCT_FROM(rhs TimeExpression) BoolExpression {
	return isDistinctFrom(t.parent, rhs)
}

func (t *timeInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs TimeExpression) BoolExpression {
	return isNotDistinctFrom(t.parent, rhs)
}

func (t *timeInterfaceImpl) LT(rhs TimeExpression) BoolExpression {
	return lt(t.parent, rhs)
}

func (t *timeInterfaceImpl) LT_EQ(rhs TimeExpression) BoolExpression {
	return ltEq(t.parent, rhs)
}

func (t *timeInterfaceImpl) GT(rhs TimeExpression) BoolExpression {
	return gt(t.parent, rhs)
}

func (t *timeInterfaceImpl) GT_EQ(rhs TimeExpression) BoolExpression {
	return gtEq(t.parent, rhs)
}

//---------------------------------------------------//
type prefixTimeExpression struct {
	ExpressionInterfaceImpl
	timeInterfaceImpl

	prefixOpExpression
}

//func newPrefixTimeExpression(operator string, expression Expression) TimeExpression {
//	timeExpr := prefixTimeExpression{}
//	timeExpr.prefixOpExpression = newPrefixExpression(expression, operator)
//
//	timeExpr.ExpressionInterfaceImpl.Parent = &timeExpr
//	timeExpr.timeInterfaceImpl.Parent = &timeExpr
//
//	return &timeExpr
//}

//---------------------------------------------------//

type timeExpressionWrapper struct {
	timeInterfaceImpl
	Expression
}

func newTimeExpressionWrap(expression Expression) TimeExpression {
	timeExpressionWrap := timeExpressionWrapper{Expression: expression}
	timeExpressionWrap.timeInterfaceImpl.parent = &timeExpressionWrap
	return &timeExpressionWrap
}

// TimeExp is time expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as time expression.
// Does not add sql cast to generated sql builder output.
func TimeExp(expression Expression) TimeExpression {
	return newTimeExpressionWrap(expression)
}
