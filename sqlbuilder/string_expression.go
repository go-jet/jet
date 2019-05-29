package sqlbuilder

type stringExpression interface {
	expression

	EQ(rhs stringExpression) boolExpression
	NOT_EQ(rhs stringExpression) boolExpression
	LT(rhs stringExpression) boolExpression
	LT_EQ(rhs stringExpression) boolExpression
	GT(rhs stringExpression) boolExpression
	GT_EQ(rhs stringExpression) boolExpression
}

type stringInterfaceImpl struct {
	parent stringExpression
}

func (s *stringInterfaceImpl) EQ(rhs stringExpression) boolExpression {
	return EQ(s.parent, rhs)
}

func (s *stringInterfaceImpl) NOT_EQ(rhs stringExpression) boolExpression {
	return NOT_EQ(s.parent, rhs)
}

func (s *stringInterfaceImpl) GT(rhs stringExpression) boolExpression {
	return GT(s.parent, rhs)
}

func (s *stringInterfaceImpl) GT_EQ(rhs stringExpression) boolExpression {
	return GT_EQ(s.parent, rhs)
}

func (s *stringInterfaceImpl) LT(rhs stringExpression) boolExpression {
	return LT(s.parent, rhs)
}

func (s *stringInterfaceImpl) LT_EQ(rhs stringExpression) boolExpression {
	return LT_EQ(s.parent, rhs)
}
