package sqlbuilder

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
	SIMILAR_TO(pattern StringExpression) BoolExpression
	NOT_SIMILAR_TO(pattern StringExpression) BoolExpression
}

type stringInterfaceImpl struct {
	parent StringExpression
}

func (s *stringInterfaceImpl) EQ(rhs StringExpression) BoolExpression {
	return EQ(s.parent, rhs)
}

func (s *stringInterfaceImpl) NOT_EQ(rhs StringExpression) BoolExpression {
	return NOT_EQ(s.parent, rhs)
}

func (s *stringInterfaceImpl) IS_DISTINCT_FROM(rhs StringExpression) BoolExpression {
	return IS_DISTINCT_FROM(s.parent, rhs)
}

func (s *stringInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs StringExpression) BoolExpression {
	return IS_NOT_DISTINCT_FROM(s.parent, rhs)
}

func (s *stringInterfaceImpl) GT(rhs StringExpression) BoolExpression {
	return GT(s.parent, rhs)
}

func (s *stringInterfaceImpl) GT_EQ(rhs StringExpression) BoolExpression {
	return GT_EQ(s.parent, rhs)
}

func (s *stringInterfaceImpl) LT(rhs StringExpression) BoolExpression {
	return LT(s.parent, rhs)
}

func (s *stringInterfaceImpl) LT_EQ(rhs StringExpression) BoolExpression {
	return LT_EQ(s.parent, rhs)
}

func (s *stringInterfaceImpl) CONCAT(rhs Expression) StringExpression {
	return newBinaryStringExpression(s.parent, rhs, "||")
}

func (s *stringInterfaceImpl) LIKE(pattern StringExpression) BoolExpression {
	return newBinaryBoolExpression(s.parent, pattern, "LIKE")
}

func (s *stringInterfaceImpl) NOT_LIKE(pattern StringExpression) BoolExpression {
	return newBinaryBoolExpression(s.parent, pattern, "NOT LIKE")
}

func (s *stringInterfaceImpl) SIMILAR_TO(pattern StringExpression) BoolExpression {
	return newBinaryBoolExpression(s.parent, pattern, "SIMILAR TO")
}

func (s *stringInterfaceImpl) NOT_SIMILAR_TO(pattern StringExpression) BoolExpression {
	return newBinaryBoolExpression(s.parent, pattern, "NOT SIMILAR TO")
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
