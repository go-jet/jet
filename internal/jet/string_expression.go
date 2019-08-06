package jet

const (
	StringConcatOperator = "||"
)

// StringExpression interface
type StringExpression interface {
	Expression

	EQ(rhs StringExpression) BoolExpression
	NOT_EQ(rhs StringExpression) BoolExpression
	IS_DISTINCT_FROM(rhs StringExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs StringExpression) BoolExpression

	LT(rhs StringExpression) BoolExpression
	LT_EQ(rhs StringExpression) BoolExpression
	GT(rhs StringExpression) BoolExpression
	GT_EQ(rhs StringExpression) BoolExpression

	CONCAT(rhs Expression) StringExpression

	LIKE(pattern StringExpression) BoolExpression
	NOT_LIKE(pattern StringExpression) BoolExpression

	REGEXP_LIKE(pattern StringExpression, matchType ...string) BoolExpression
}

type stringInterfaceImpl struct {
	parent StringExpression
}

func (s *stringInterfaceImpl) EQ(rhs StringExpression) BoolExpression {
	return eq(s.parent, rhs)
}

func (s *stringInterfaceImpl) NOT_EQ(rhs StringExpression) BoolExpression {
	return notEq(s.parent, rhs)
}

func (s *stringInterfaceImpl) IS_DISTINCT_FROM(rhs StringExpression) BoolExpression {
	return isDistinctFrom(s.parent, rhs)
}

func (s *stringInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs StringExpression) BoolExpression {
	return isNotDistinctFrom(s.parent, rhs)
}

func (s *stringInterfaceImpl) GT(rhs StringExpression) BoolExpression {
	return gt(s.parent, rhs)
}

func (s *stringInterfaceImpl) GT_EQ(rhs StringExpression) BoolExpression {
	return gtEq(s.parent, rhs)
}

func (s *stringInterfaceImpl) LT(rhs StringExpression) BoolExpression {
	return lt(s.parent, rhs)
}

func (s *stringInterfaceImpl) LT_EQ(rhs StringExpression) BoolExpression {
	return ltEq(s.parent, rhs)
}

func (s *stringInterfaceImpl) CONCAT(rhs Expression) StringExpression {
	return newBinaryStringExpression(s.parent, rhs, StringConcatOperator)
}

func (s *stringInterfaceImpl) LIKE(pattern StringExpression) BoolExpression {
	return newBinaryBoolOperator(s.parent, pattern, "LIKE")
}

func (s *stringInterfaceImpl) NOT_LIKE(pattern StringExpression) BoolExpression {
	return newBinaryBoolOperator(s.parent, pattern, "NOT LIKE")
}

func (s *stringInterfaceImpl) REGEXP_LIKE(pattern StringExpression, matchType ...string) BoolExpression {
	return REGEXP_LIKE(s.parent, pattern, matchType...)
}

//---------------------------------------------------//
type binaryStringExpression struct {
	expressionInterfaceImpl
	stringInterfaceImpl

	binaryOpExpression
}

func newBinaryStringExpression(lhs, rhs Expression, operator string) StringExpression {
	boolExpression := binaryStringExpression{}

	boolExpression.binaryOpExpression = newBinaryExpression(lhs, rhs, operator)
	boolExpression.expressionInterfaceImpl.parent = &boolExpression
	boolExpression.stringInterfaceImpl.parent = &boolExpression

	return &boolExpression
}

//---------------------------------------------------//

type stringExpressionWrapper struct {
	stringInterfaceImpl
	Expression
}

func newStringExpressionWrap(expression Expression) StringExpression {
	stringExpressionWrap := stringExpressionWrapper{Expression: expression}
	stringExpressionWrap.stringInterfaceImpl.parent = &stringExpressionWrap
	return &stringExpressionWrap
}

// StringExp is string expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as string expression.
// Does not add sql cast to generated sql builder output.
func StringExp(expression Expression) StringExpression {
	return newStringExpressionWrap(expression)
}
