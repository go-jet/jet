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
	root BlobExpression
}

func (b *blobInterfaceImpl) isStringOrBlob() {}

func (b *blobInterfaceImpl) EQ(rhs BlobExpression) BoolExpression {
	return Eq(b.root, rhs)
}

func (b *blobInterfaceImpl) NOT_EQ(rhs BlobExpression) BoolExpression {
	return NotEq(b.root, rhs)
}

func (b *blobInterfaceImpl) IS_DISTINCT_FROM(rhs BlobExpression) BoolExpression {
	return IsDistinctFrom(b.root, rhs)
}

func (b *blobInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs BlobExpression) BoolExpression {
	return IsNotDistinctFrom(b.root, rhs)
}

func (b *blobInterfaceImpl) GT(rhs BlobExpression) BoolExpression {
	return Gt(b.root, rhs)
}

func (b *blobInterfaceImpl) GT_EQ(rhs BlobExpression) BoolExpression {
	return GtEq(b.root, rhs)
}

func (b *blobInterfaceImpl) LT(rhs BlobExpression) BoolExpression {
	return Lt(b.root, rhs)
}

func (b *blobInterfaceImpl) LT_EQ(rhs BlobExpression) BoolExpression {
	return LtEq(b.root, rhs)
}

func (b *blobInterfaceImpl) BETWEEN(min, max BlobExpression) BoolExpression {
	return NewBetweenOperatorExpression(b.root, min, max, false)
}

func (b *blobInterfaceImpl) NOT_BETWEEN(min, max BlobExpression) BoolExpression {
	return NewBetweenOperatorExpression(b.root, min, max, true)
}

func (b *blobInterfaceImpl) CONCAT(rhs BlobExpression) BlobExpression {
	return BlobExp(newBinaryStringOperatorExpression(b.root, rhs, StringConcatOperator))
}

func (b *blobInterfaceImpl) LIKE(pattern BlobExpression) BoolExpression {
	return newBinaryBoolOperatorExpression(b.root, pattern, "LIKE")
}

func (b *blobInterfaceImpl) NOT_LIKE(pattern BlobExpression) BoolExpression {
	return newBinaryBoolOperatorExpression(b.root, pattern, "NOT LIKE")
}

//---------------------------------------------------//

type blobExpressionWrapper struct {
	Expression
	blobInterfaceImpl
}

func newBlobExpressionWrap(expression Expression) BlobExpression {
	blobExpressionWrap := &blobExpressionWrapper{Expression: expression}
	blobExpressionWrap.blobInterfaceImpl.root = blobExpressionWrap
	expression.setRoot(blobExpressionWrap)
	return blobExpressionWrap
}

// BlobExp is blob expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as blob expression.
// Does not add sql cast to generated sql builder output.
func BlobExp(expression Expression) BlobExpression {
	return newBlobExpressionWrap(expression)
}
