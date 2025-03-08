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

func (b *blobInterfaceImpl) isStringOrBlob() {}

func (b *blobInterfaceImpl) EQ(rhs BlobExpression) BoolExpression {
	return Eq(b.parent, rhs)
}

func (b *blobInterfaceImpl) NOT_EQ(rhs BlobExpression) BoolExpression {
	return NotEq(b.parent, rhs)
}

func (b *blobInterfaceImpl) IS_DISTINCT_FROM(rhs BlobExpression) BoolExpression {
	return IsDistinctFrom(b.parent, rhs)
}

func (b *blobInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs BlobExpression) BoolExpression {
	return IsNotDistinctFrom(b.parent, rhs)
}

func (b *blobInterfaceImpl) GT(rhs BlobExpression) BoolExpression {
	return Gt(b.parent, rhs)
}

func (b *blobInterfaceImpl) GT_EQ(rhs BlobExpression) BoolExpression {
	return GtEq(b.parent, rhs)
}

func (b *blobInterfaceImpl) LT(rhs BlobExpression) BoolExpression {
	return Lt(b.parent, rhs)
}

func (b *blobInterfaceImpl) LT_EQ(rhs BlobExpression) BoolExpression {
	return LtEq(b.parent, rhs)
}

func (b *blobInterfaceImpl) BETWEEN(min, max BlobExpression) BoolExpression {
	return NewBetweenOperatorExpression(b.parent, min, max, false)
}

func (b *blobInterfaceImpl) NOT_BETWEEN(min, max BlobExpression) BoolExpression {
	return NewBetweenOperatorExpression(b.parent, min, max, true)
}

func (b *blobInterfaceImpl) CONCAT(rhs BlobExpression) BlobExpression {
	return BlobExp(newBinaryStringOperatorExpression(b.parent, rhs, StringConcatOperator))
}

func (b *blobInterfaceImpl) LIKE(pattern BlobExpression) BoolExpression {
	return newBinaryBoolOperatorExpression(b.parent, pattern, "LIKE")
}

func (b *blobInterfaceImpl) NOT_LIKE(pattern BlobExpression) BoolExpression {
	return newBinaryBoolOperatorExpression(b.parent, pattern, "NOT LIKE")
}

//---------------------------------------------------//

type blobExpressionWrapper struct {
	Expression
	blobInterfaceImpl
}

func newBlobExpressionWrap(expression Expression) BlobExpression {
	blobExpressionWrap := &blobExpressionWrapper{Expression: expression}
	blobExpressionWrap.blobInterfaceImpl.parent = blobExpressionWrap
	expression.setParent(blobExpressionWrap)
	return blobExpressionWrap
}

// BlobExp is blob expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as blob expression.
// Does not add sql cast to generated sql builder output.
func BlobExp(expression Expression) BlobExpression {
	return newBlobExpressionWrap(expression)
}
