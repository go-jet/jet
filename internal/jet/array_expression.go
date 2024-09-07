package jet

// Array interface
type Array[E Expression] interface {
	Expression

	EQ(rhs Array[E]) BoolExpression
	NOT_EQ(rhs Array[E]) BoolExpression
	LT(rhs Array[E]) BoolExpression
	GT(rhs Array[E]) BoolExpression
	LT_EQ(rhs Array[E]) BoolExpression
	GT_EQ(rhs Array[E]) BoolExpression

	CONTAINS(rhs Array[E]) BoolExpression
	IS_CONTAINED_BY(rhs Array[E]) BoolExpression
	OVERLAP(rhs Array[E]) BoolExpression
	CONCAT(rhs Array[E]) Array[E]
	CONCAT_ELEMENT(E) Array[E]

	AT(expression IntegerExpression) Expression
}

type arrayInterfaceImpl[E Expression] struct {
	parent Array[E]
}

type BinaryBoolOp func(Expression, Expression) BoolExpression

func (a arrayInterfaceImpl[E]) EQ(rhs Array[E]) BoolExpression {
	return Eq(a.parent, rhs)
}

func (a arrayInterfaceImpl[E]) NOT_EQ(rhs Array[E]) BoolExpression {
	return NotEq(a.parent, rhs)
}

func (a arrayInterfaceImpl[E]) LT(rhs Array[E]) BoolExpression {
	return Lt(a.parent, rhs)
}

func (a arrayInterfaceImpl[E]) GT(rhs Array[E]) BoolExpression {
	return Gt(a.parent, rhs)
}

func (a arrayInterfaceImpl[E]) LT_EQ(rhs Array[E]) BoolExpression {
	return LtEq(a.parent, rhs)
}

func (a arrayInterfaceImpl[E]) GT_EQ(rhs Array[E]) BoolExpression {
	return GtEq(a.parent, rhs)
}

func (a arrayInterfaceImpl[E]) CONTAINS(rhs Array[E]) BoolExpression {
	return Contains(a.parent, rhs)
}

func (a arrayInterfaceImpl[E]) IS_CONTAINED_BY(rhs Array[E]) BoolExpression {
	return IsContainedBy(a.parent, rhs)
}

func (a arrayInterfaceImpl[E]) OVERLAP(rhs Array[E]) BoolExpression {
	return Overlap(a.parent, rhs)
}

func (a arrayInterfaceImpl[E]) CONCAT(rhs Array[E]) Array[E] {
	return ArrayExp[E](NewBinaryOperatorExpression(a.parent, rhs, "||"))
}

func (a arrayInterfaceImpl[E]) CONCAT_ELEMENT(rhs E) Array[E] {
	return ArrayExp[E](NewBinaryOperatorExpression(a.parent, rhs, "||"))
}

func (a arrayInterfaceImpl[E]) AT(expression IntegerExpression) Expression {
	return arraySubscriptExpr(a.parent, expression)
}

type arrayExpressionWrapper[E Expression] struct {
	arrayInterfaceImpl[E]
	Expression
}

func newArrayExpressionWrap[E Expression](expression Expression) Array[E] {
	arrayExpressionWrapper := arrayExpressionWrapper[E]{Expression: expression}
	arrayExpressionWrapper.arrayInterfaceImpl.parent = &arrayExpressionWrapper
	return &arrayExpressionWrapper
}

// ArrayExp is array expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as array expression.
// Does not add sql cast to generated sql builder output.
func ArrayExp[E Expression](expression Expression) Array[E] {
	return newArrayExpressionWrap[E](expression)
}
