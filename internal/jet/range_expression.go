package jet

// Range Expression is interface for date range types
type Range[T Expression] interface {
	Expression

	EQ(rhs Range[T]) BoolExpression
	NOT_EQ(rhs Range[T]) BoolExpression

	LT(rhs Range[T]) BoolExpression
	LT_EQ(rhs Range[T]) BoolExpression
	GT(rhs Range[T]) BoolExpression
	GT_EQ(rhs Range[T]) BoolExpression

	CONTAINS(rhs T) BoolExpression
	CONTAINS_RANGE(rhs Range[T]) BoolExpression
	OVERLAP(rhs Range[T]) BoolExpression
	UNION(rhs Range[T]) Range[T]
	INTERSECTION(rhs Range[T]) Range[T]
	DIFFERENCE(rhs Range[T]) Range[T]

	UPPER_BOUND() T
	LOWER_BOUND() T
	IS_EMPTY() BoolExpression
	LOWER_INC() BoolExpression
	UPPER_INC() BoolExpression
	LOWER_INF() BoolExpression
	UPPER_INF() BoolExpression
}

type rangeInterfaceImpl[T Expression] struct {
	parent Range[T]
}

func (r *rangeInterfaceImpl[T]) EQ(rhs Range[T]) BoolExpression {
	return Eq(r.parent, rhs)
}

func (r *rangeInterfaceImpl[T]) NOT_EQ(rhs Range[T]) BoolExpression {
	return NotEq(r.parent, rhs)
}

func (r *rangeInterfaceImpl[T]) LT(rhs Range[T]) BoolExpression {
	return Lt(r.parent, rhs)
}

func (r *rangeInterfaceImpl[T]) LT_EQ(rhs Range[T]) BoolExpression {
	return LtEq(r.parent, rhs)

}

func (r *rangeInterfaceImpl[T]) GT(rhs Range[T]) BoolExpression {
	return Gt(r.parent, rhs)

}

func (r *rangeInterfaceImpl[T]) GT_EQ(rhs Range[T]) BoolExpression {
	return GtEq(r.parent, rhs)
}

func (r *rangeInterfaceImpl[T]) CONTAINS(rhs T) BoolExpression {
	return Contains(r.parent, rhs)
}

func (r *rangeInterfaceImpl[T]) CONTAINS_RANGE(rhs Range[T]) BoolExpression {
	return Contains(r.parent, rhs)
}

func (r *rangeInterfaceImpl[T]) OVERLAP(rhs Range[T]) BoolExpression {
	return Overlap(r.parent, rhs)
}

func (r *rangeInterfaceImpl[T]) UNION(rhs Range[T]) Range[T] {
	return RangeExp[T](Add(r.parent, rhs))
}

func (r *rangeInterfaceImpl[T]) INTERSECTION(rhs Range[T]) Range[T] {
	return RangeExp[T](Mul(r.parent, rhs))
}

func (r *rangeInterfaceImpl[T]) DIFFERENCE(rhs Range[T]) Range[T] {
	return RangeExp[T](Sub(r.parent, rhs))
}

func (r *rangeInterfaceImpl[T]) UPPER_BOUND() T {
	return UPPER_BOUND(r.parent)
}

func (r *rangeInterfaceImpl[T]) LOWER_BOUND() T {
	return LOWER_BOUND(r.parent)
}

func (r *rangeInterfaceImpl[T]) IS_EMPTY() BoolExpression {
	return IS_EMPTY(r.parent)
}

func (r *rangeInterfaceImpl[T]) LOWER_INC() BoolExpression {
	return LOWER_INC(r.parent)
}

func (r *rangeInterfaceImpl[T]) UPPER_INC() BoolExpression {
	return UPPER_INC(r.parent)
}

func (r *rangeInterfaceImpl[T]) LOWER_INF() BoolExpression {
	return LOWER_INF(r.parent)
}

func (r *rangeInterfaceImpl[T]) UPPER_INF() BoolExpression {
	return UPPER_INF(r.parent)
}

//---------------------------------------------------//

type rangeExpressionWrapper[T Expression] struct {
	rangeInterfaceImpl[T]
	Expression
}

func newRangeExpressionWrap[T Expression](expression Expression) Range[T] {
	rangeExpressionWrap := rangeExpressionWrapper[T]{Expression: expression}
	rangeExpressionWrap.rangeInterfaceImpl.parent = &rangeExpressionWrap
	return &rangeExpressionWrap
}

// RangeExp is range expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as range expression.
// Does not add sql cast to generated sql builder output.
func RangeExp[T Expression](expression Expression) Range[T] {
	return newRangeExpressionWrap[T](expression)
}

// different range expression wrappers
var (
	Int4RangeExp = RangeExp[Int4Expression]
	Int8RangeExp = RangeExp[Int8Expression]
	NumRangeExp  = RangeExp[NumericExpression]
	DateRangeExp = RangeExp[DateExpression]
	TsRangeExp   = RangeExp[TimestampExpression]
	TstzRangeExp = RangeExp[TimestampzExpression]
)
