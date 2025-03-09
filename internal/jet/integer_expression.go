package jet

// IntegerExpression interface
type IntegerExpression interface {
	Expression
	numericExpression

	EQ(rhs IntegerExpression) BoolExpression
	NOT_EQ(rhs IntegerExpression) BoolExpression
	IS_DISTINCT_FROM(rhs IntegerExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs IntegerExpression) BoolExpression

	LT(rhs IntegerExpression) BoolExpression
	LT_EQ(rhs IntegerExpression) BoolExpression
	GT(rhs IntegerExpression) BoolExpression
	GT_EQ(rhs IntegerExpression) BoolExpression
	BETWEEN(min, max IntegerExpression) BoolExpression
	NOT_BETWEEN(min, max IntegerExpression) BoolExpression

	ADD(rhs IntegerExpression) IntegerExpression
	SUB(rhs IntegerExpression) IntegerExpression
	MUL(rhs IntegerExpression) IntegerExpression
	DIV(rhs IntegerExpression) IntegerExpression
	MOD(rhs IntegerExpression) IntegerExpression
	POW(rhs IntegerExpression) IntegerExpression

	BIT_AND(rhs IntegerExpression) IntegerExpression
	BIT_OR(rhs IntegerExpression) IntegerExpression
	BIT_XOR(rhs IntegerExpression) IntegerExpression
	BIT_SHIFT_LEFT(shift IntegerExpression) IntegerExpression
	BIT_SHIFT_RIGHT(shift IntegerExpression) IntegerExpression
}

// additional integer expression subtypes, used in range expressions.
type (
	Int4Expression IntegerExpression
	Int8Expression IntegerExpression
)

type integerInterfaceImpl struct {
	numericExpressionImpl
	root IntegerExpression
}

func (i *integerInterfaceImpl) EQ(rhs IntegerExpression) BoolExpression {
	return Eq(i.root, rhs)
}

func (i *integerInterfaceImpl) NOT_EQ(rhs IntegerExpression) BoolExpression {
	return NotEq(i.root, rhs)
}

func (i *integerInterfaceImpl) IS_DISTINCT_FROM(rhs IntegerExpression) BoolExpression {
	return IsDistinctFrom(i.root, rhs)
}

func (i *integerInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs IntegerExpression) BoolExpression {
	return IsNotDistinctFrom(i.root, rhs)
}

func (i *integerInterfaceImpl) GT(rhs IntegerExpression) BoolExpression {
	return Gt(i.root, rhs)
}

func (i *integerInterfaceImpl) GT_EQ(rhs IntegerExpression) BoolExpression {
	return GtEq(i.root, rhs)
}

func (i *integerInterfaceImpl) LT(rhs IntegerExpression) BoolExpression {
	return Lt(i.root, rhs)
}

func (i *integerInterfaceImpl) LT_EQ(rhs IntegerExpression) BoolExpression {
	return LtEq(i.root, rhs)
}

func (i *integerInterfaceImpl) BETWEEN(min, max IntegerExpression) BoolExpression {
	return NewBetweenOperatorExpression(i.root, min, max, false)
}

func (i *integerInterfaceImpl) NOT_BETWEEN(min, max IntegerExpression) BoolExpression {
	return NewBetweenOperatorExpression(i.root, min, max, true)
}

func (i *integerInterfaceImpl) ADD(rhs IntegerExpression) IntegerExpression {
	return IntExp(Add(i.root, rhs))
}

func (i *integerInterfaceImpl) SUB(rhs IntegerExpression) IntegerExpression {
	return IntExp(Sub(i.root, rhs))
}

func (i *integerInterfaceImpl) MUL(rhs IntegerExpression) IntegerExpression {
	return IntExp(Mul(i.root, rhs))
}

func (i *integerInterfaceImpl) DIV(rhs IntegerExpression) IntegerExpression {
	return IntExp(Div(i.root, rhs))
}

func (i *integerInterfaceImpl) MOD(rhs IntegerExpression) IntegerExpression {
	return IntExp(Mod(i.root, rhs))
}

func (i *integerInterfaceImpl) POW(rhs IntegerExpression) IntegerExpression {
	return IntExp(POW(i.root, rhs))
}

func (i *integerInterfaceImpl) BIT_AND(rhs IntegerExpression) IntegerExpression {
	return newBinaryIntegerOperatorExpression(i.root, rhs, "&")
}

func (i *integerInterfaceImpl) BIT_OR(rhs IntegerExpression) IntegerExpression {
	return newBinaryIntegerOperatorExpression(i.root, rhs, "|")
}

func (i *integerInterfaceImpl) BIT_XOR(rhs IntegerExpression) IntegerExpression {
	return newBinaryIntegerOperatorExpression(i.root, rhs, "#")
}

func (i *integerInterfaceImpl) BIT_SHIFT_LEFT(intExpression IntegerExpression) IntegerExpression {
	return newBinaryIntegerOperatorExpression(i.root, intExpression, "<<")
}

func (i *integerInterfaceImpl) BIT_SHIFT_RIGHT(intExpression IntegerExpression) IntegerExpression {
	return newBinaryIntegerOperatorExpression(i.root, intExpression, ">>")
}

func newBinaryIntegerOperatorExpression(lhs, rhs IntegerExpression, operator string) IntegerExpression {
	return IntExp(NewBinaryOperatorExpression(lhs, rhs, operator))
}

func newPrefixIntegerOperatorExpression(expression IntegerExpression, operator string) IntegerExpression {
	return IntExp(newPrefixOperatorExpression(expression, operator))
}

type integerExpressionWrapper struct {
	integerInterfaceImpl

	Expression
}

func newIntExpressionWrap(expression Expression) IntegerExpression {
	intExpressionWrap := &integerExpressionWrapper{Expression: expression}
	intExpressionWrap.integerInterfaceImpl.root = intExpressionWrap
	expression.setRoot(intExpressionWrap)

	return intExpressionWrap
}

// IntExp is int expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as int expression.
// Does not add sql cast to generated sql builder output.
func IntExp(expression Expression) IntegerExpression {
	return newIntExpressionWrap(expression)
}
