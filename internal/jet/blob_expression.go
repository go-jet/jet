package jet

// BlobExpression interface
type BlobExpression interface {
	Expression

	isStringOrBlob()

	EQ(rhs BlobExpression) BoolExpression
	NOT_EQ(rhs BlobExpression) BoolExpression
	IS_DISTINCT_FROM(rhs BlobExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs BlobExpression) BoolExpression

	LT(rhs BlobExpression) BoolExpression
	LT_EQ(rhs BlobExpression) BoolExpression
	GT(rhs BlobExpression) BoolExpression
	GT_EQ(rhs BlobExpression) BoolExpression
	BETWEEN(min, max BlobExpression) BoolExpression
	NOT_BETWEEN(min, max BlobExpression) BoolExpression

	CONCAT(rhs BlobExpression) BlobExpression

	LIKE(pattern BlobExpression) BoolExpression
	NOT_LIKE(pattern BlobExpression) BoolExpression
}

type blobInterfaceImpl struct {
	parent BlobExpression
}

func (s *blobInterfaceImpl) isStringOrBlob() {}

func (s *blobInterfaceImpl) EQ(rhs BlobExpression) BoolExpression {
	return Eq(s.parent, rhs)
}

func (s *blobInterfaceImpl) NOT_EQ(rhs BlobExpression) BoolExpression {
	return NotEq(s.parent, rhs)
}

func (s *blobInterfaceImpl) IS_DISTINCT_FROM(rhs BlobExpression) BoolExpression {
	return IsDistinctFrom(s.parent, rhs)
}

func (s *blobInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs BlobExpression) BoolExpression {
	return IsNotDistinctFrom(s.parent, rhs)
}

func (s *blobInterfaceImpl) GT(rhs BlobExpression) BoolExpression {
	return Gt(s.parent, rhs)
}

func (s *blobInterfaceImpl) GT_EQ(rhs BlobExpression) BoolExpression {
	return GtEq(s.parent, rhs)
}

func (s *blobInterfaceImpl) LT(rhs BlobExpression) BoolExpression {
	return Lt(s.parent, rhs)
}

func (s *blobInterfaceImpl) LT_EQ(rhs BlobExpression) BoolExpression {
	return LtEq(s.parent, rhs)
}

func (s *blobInterfaceImpl) BETWEEN(min, max BlobExpression) BoolExpression {
	return NewBetweenOperatorExpression(s.parent, min, max, false)
}

func (s *blobInterfaceImpl) NOT_BETWEEN(min, max BlobExpression) BoolExpression {
	return NewBetweenOperatorExpression(s.parent, min, max, true)
}

func (s *blobInterfaceImpl) CONCAT(rhs BlobExpression) BlobExpression {
	return BlobExp(newBinaryStringOperatorExpression(s.parent, rhs, StringConcatOperator))
}

func (s *blobInterfaceImpl) LIKE(pattern BlobExpression) BoolExpression {
	return newBinaryBoolOperatorExpression(s.parent, pattern, "LIKE")
}

func (s *blobInterfaceImpl) NOT_LIKE(pattern BlobExpression) BoolExpression {
	return newBinaryBoolOperatorExpression(s.parent, pattern, "NOT LIKE")
}

//---------------------------------------------------//

type blobExpressionWrapper struct {
	blobInterfaceImpl
	Expression
}

func newBlobExpressionWrap(expression Expression) BlobExpression {
	blobExpressionWrap := blobExpressionWrapper{Expression: expression}
	blobExpressionWrap.blobInterfaceImpl.parent = &blobExpressionWrap
	return &blobExpressionWrap
}

// BlobExp is blob expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as blob expression.
// Does not add sql cast to generated sql builder output.
func BlobExp(expression Expression) BlobExpression {
	return newBlobExpressionWrap(expression)
}
