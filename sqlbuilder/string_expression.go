package sqlbuilder

type stringExpression interface {
	expression

	EQ(rhs stringExpression) BoolExpression
	NOT_EQ(rhs stringExpression) BoolExpression
	IS_DISTINCT_FROM(rhs stringExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs stringExpression) BoolExpression

	LT(rhs stringExpression) BoolExpression
	LT_EQ(rhs stringExpression) BoolExpression
	GT(rhs stringExpression) BoolExpression
	GT_EQ(rhs stringExpression) BoolExpression
}

type stringInterfaceImpl struct {
	parent stringExpression
}

func (s *stringInterfaceImpl) EQ(rhs stringExpression) BoolExpression {
	return EQ(s.parent, rhs)
}

func (s *stringInterfaceImpl) NOT_EQ(rhs stringExpression) BoolExpression {
	return NOT_EQ(s.parent, rhs)
}

func (s *stringInterfaceImpl) IS_DISTINCT_FROM(rhs stringExpression) BoolExpression {
	return IS_DISTINCT_FROM(s.parent, rhs)
}

func (s *stringInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs stringExpression) BoolExpression {
	return IS_NOT_DISTINCT_FROM(s.parent, rhs)
}

func (s *stringInterfaceImpl) GT(rhs stringExpression) BoolExpression {
	return GT(s.parent, rhs)
}

func (s *stringInterfaceImpl) GT_EQ(rhs stringExpression) BoolExpression {
	return GT_EQ(s.parent, rhs)
}

func (s *stringInterfaceImpl) LT(rhs stringExpression) BoolExpression {
	return LT(s.parent, rhs)
}

func (s *stringInterfaceImpl) LT_EQ(rhs stringExpression) BoolExpression {
	return LT_EQ(s.parent, rhs)
}
