package jet

// ArrayExpression interface
type ArrayExpression[E Expression] interface {
	Expression

	EQ(rhs ArrayExpression[E]) BoolExpression
	NOT_EQ(rhs ArrayExpression[E]) BoolExpression
	LT(rhs ArrayExpression[E]) BoolExpression
	GT(rhs ArrayExpression[E]) BoolExpression
	LT_EQ(rhs ArrayExpression[E]) BoolExpression
	GT_EQ(rhs ArrayExpression[E]) BoolExpression

	CONTAINS(rhs ArrayExpression[E]) BoolExpression
	IS_CONTAINED_BY(rhs ArrayExpression[E]) BoolExpression
	OVERLAP(rhs ArrayExpression[E]) BoolExpression
	CONCAT(rhs ArrayExpression[E]) ArrayExpression[E]
	CONCAT_ELEMENT(E) ArrayExpression[E]

	AT(expression IntegerExpression) Expression
}

type arrayInterfaceImpl[E Expression] struct {
	parent ArrayExpression[E]
}

type BinaryBoolOp func(Expression, Expression) BoolExpression

func (a arrayInterfaceImpl[E]) EQ(rhs ArrayExpression[E]) BoolExpression {
	return Eq(a.parent, rhs)
}

func (a arrayInterfaceImpl[E]) NOT_EQ(rhs ArrayExpression[E]) BoolExpression {
	return NotEq(a.parent, rhs)
}

func (a arrayInterfaceImpl[E]) LT(rhs ArrayExpression[E]) BoolExpression {
	return Lt(a.parent, rhs)
}

func (a arrayInterfaceImpl[E]) GT(rhs ArrayExpression[E]) BoolExpression {
	return Gt(a.parent, rhs)
}

func (a arrayInterfaceImpl[E]) LT_EQ(rhs ArrayExpression[E]) BoolExpression {
	return LtEq(a.parent, rhs)
}

func (a arrayInterfaceImpl[E]) GT_EQ(rhs ArrayExpression[E]) BoolExpression {
	return GtEq(a.parent, rhs)
}

func (a arrayInterfaceImpl[E]) CONTAINS(rhs ArrayExpression[E]) BoolExpression {
	return Contains(a.parent, rhs)
}

func (a arrayInterfaceImpl[E]) IS_CONTAINED_BY(rhs ArrayExpression[E]) BoolExpression {
	return IsContainedBy(a.parent, rhs)
}

func (a arrayInterfaceImpl[E]) OVERLAP(rhs ArrayExpression[E]) BoolExpression {
	return Overlap(a.parent, rhs)
}

func (a arrayInterfaceImpl[E]) CONCAT(rhs ArrayExpression[E]) ArrayExpression[E] {
	return ArrayExp[E](NewBinaryOperatorExpression(a.parent, rhs, "||"))
}

func (a arrayInterfaceImpl[E]) CONCAT_ELEMENT(rhs E) ArrayExpression[E] {
	return ArrayExp[E](NewBinaryOperatorExpression(a.parent, rhs, "||"))
}

func (a arrayInterfaceImpl[E]) AT(expression IntegerExpression) Expression {
	return arraySubscriptExpr(a.parent, expression)
}

type arrayExpressionWrapper[E Expression] struct {
	arrayInterfaceImpl[E]
	Expression
}

func newArrayExpressionWrap[E Expression](expression Expression) ArrayExpression[E] {
	arrayExpressionWrapper := arrayExpressionWrapper[E]{Expression: expression}
	arrayExpressionWrapper.arrayInterfaceImpl.parent = &arrayExpressionWrapper
	return &arrayExpressionWrapper
}

// ArrayExp is array expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as array expression.
// Does not add sql cast to generated sql builder output.
func ArrayExp[E Expression](expression Expression) ArrayExpression[E] {
	return newArrayExpressionWrap[E](expression)
}
