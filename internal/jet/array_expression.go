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

	AT(expression IntegerExpression) E
}

type arrayInterfaceImpl[E Expression] struct {
	root Expression
}

type BinaryBoolOp func(Expression, Expression) BoolExpression

func (a arrayInterfaceImpl[E]) EQ(rhs Array[E]) BoolExpression {
	return Eq(a.root, rhs)
}

func (a arrayInterfaceImpl[E]) NOT_EQ(rhs Array[E]) BoolExpression {
	return NotEq(a.root, rhs)
}

func (a arrayInterfaceImpl[E]) LT(rhs Array[E]) BoolExpression {
	return Lt(a.root, rhs)
}

func (a arrayInterfaceImpl[E]) GT(rhs Array[E]) BoolExpression {
	return Gt(a.root, rhs)
}

func (a arrayInterfaceImpl[E]) LT_EQ(rhs Array[E]) BoolExpression {
	return LtEq(a.root, rhs)
}

func (a arrayInterfaceImpl[E]) GT_EQ(rhs Array[E]) BoolExpression {
	return GtEq(a.root, rhs)
}

func (a arrayInterfaceImpl[E]) CONTAINS(rhs Array[E]) BoolExpression {
	return Contains(a.root, rhs)
}

func (a arrayInterfaceImpl[E]) IS_CONTAINED_BY(rhs Array[E]) BoolExpression {
	return IsContainedBy(a.root, rhs)
}

func (a arrayInterfaceImpl[E]) OVERLAP(rhs Array[E]) BoolExpression {
	return Overlap(a.root, rhs)
}

func (a arrayInterfaceImpl[E]) CONCAT(rhs Array[E]) Array[E] {
	return ArrayExp[E](NewBinaryOperatorExpression(a.root, rhs, "||"))
}

func (a arrayInterfaceImpl[E]) CONCAT_ELEMENT(rhs E) Array[E] {
	return ArrayExp[E](NewBinaryOperatorExpression(a.root, rhs, "||"))
}

func (a arrayInterfaceImpl[E]) AT(at IntegerExpression) E {
	return CastToArrayElemType[E](a.root, AtomicCustomExpression(a.root, Token("["), at, Token("]")))
}

type arrayExpressionWrapper[E Expression] struct {
	arrayInterfaceImpl[E]
	Expression
}

func (b *arrayExpressionWrapper[E]) setRoot(root Expression) {
	b.Expression.setRoot(root)
	b.arrayInterfaceImpl.root = root
}

func newArrayExpressionWrap[E Expression](expression Expression) Array[E] {
	arrayExpressionWrapper := &arrayExpressionWrapper[E]{Expression: expression}
	arrayExpressionWrapper.arrayInterfaceImpl.root = arrayExpressionWrapper
	expression.setRoot(arrayExpressionWrapper)
	return arrayExpressionWrapper
}

// ArrayExp is array expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as array expression.
// Does not add sql cast to generated sql builder output.
func ArrayExp[E Expression](expression Expression) Array[E] {
	return newArrayExpressionWrap[E](expression)
}

// CastToArrayElemType casts exp to array element type
func CastToArrayElemType[E Expression](array Expression, exp Expression) E {
	var i Expression
	switch array.(type) {
	case Array[BoolExpression]:
		i = BoolExp(exp)
	case Array[StringExpression]:
		i = StringExp(exp)
	case Array[IntegerExpression]:
		i = IntExp(exp)
	case Array[FloatExpression]:
		i = FloatExp(exp)
	case Array[BlobExpression]:
		i = BlobExp(exp)
	case Array[DateExpression]:
		i = DateExp(exp)
	case Array[TimestampExpression]:
		i = TimestampExp(exp)
	case Array[TimestampzExpression]:
		i = TimestampzExp(exp)
	case Array[TimeExpression]:
		i = TimeExp(exp)
	case Array[TimezExpression]:
		i = TimezExp(exp)
	case Array[IntervalExpression]:
		i = IntervalExp(exp)
	}

	return i.(E)
}

// ARRAY constructor builds an array value using list of expressions.
func ARRAY[E Expression](elems ...E) Array[E] {
	return ArrayExp[E](AtomicCustomExpression(Token("ARRAY["), ListSerializer{
		Serializers: ToSerializerList(elems),
		Separator:   ",",
	}, Token("]")))
}
