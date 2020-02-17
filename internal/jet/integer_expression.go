package jet

// IntegerExpression interface
type IntegerExpression interface {
	Expression
	numericExpression

	// Check if expression is equal to rhs
	EQ(rhs IntegerExpression) BoolExpression
	// Check if expression is not equal to rhs
	NOT_EQ(rhs IntegerExpression) BoolExpression
	// Check if expression is distinct from rhs
	IS_DISTINCT_FROM(rhs IntegerExpression) BoolExpression
	// Check if expression is not distinct from rhs
	IS_NOT_DISTINCT_FROM(rhs IntegerExpression) BoolExpression

	// Check if expression is less then rhs
	LT(rhs IntegerExpression) BoolExpression
	// Check if expression is less then equal rhs
	LT_EQ(rhs IntegerExpression) BoolExpression
	// Check if expression is greater then rhs
	GT(rhs IntegerExpression) BoolExpression
	// Check if expression is greater then equal rhs
	GT_EQ(rhs IntegerExpression) BoolExpression

	// expression + rhs
	ADD(rhs IntegerExpression) IntegerExpression
	// expression - rhs
	SUB(rhs IntegerExpression) IntegerExpression
	// expression * rhs
	MUL(rhs IntegerExpression) IntegerExpression
	// expression / rhs
	DIV(rhs IntegerExpression) IntegerExpression
	// expression % rhs
	MOD(rhs IntegerExpression) IntegerExpression
	// expression ^ rhs
	POW(rhs IntegerExpression) IntegerExpression

	// expression & rhs
	BIT_AND(rhs IntegerExpression) IntegerExpression
	// expression | rhs
	BIT_OR(rhs IntegerExpression) IntegerExpression
	// expression # rhs
	BIT_XOR(rhs IntegerExpression) IntegerExpression
	// expression << rhs
	BIT_SHIFT_LEFT(shift IntegerExpression) IntegerExpression
	// expression >> rhs
	BIT_SHIFT_RIGHT(shift IntegerExpression) IntegerExpression
}

type integerInterfaceImpl struct {
	numericExpressionImpl
	parent IntegerExpression
}

func (i *integerInterfaceImpl) EQ(rhs IntegerExpression) BoolExpression {
	return Eq(i.parent, rhs)
}

func (i *integerInterfaceImpl) NOT_EQ(rhs IntegerExpression) BoolExpression {
	return NotEq(i.parent, rhs)
}

func (i *integerInterfaceImpl) IS_DISTINCT_FROM(rhs IntegerExpression) BoolExpression {
	return IsDistinctFrom(i.parent, rhs)
}

func (i *integerInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs IntegerExpression) BoolExpression {
	return IsNotDistinctFrom(i.parent, rhs)
}

func (i *integerInterfaceImpl) GT(rhs IntegerExpression) BoolExpression {
	return Gt(i.parent, rhs)
}

func (i *integerInterfaceImpl) GT_EQ(rhs IntegerExpression) BoolExpression {
	return GtEq(i.parent, rhs)
}

func (i *integerInterfaceImpl) LT(rhs IntegerExpression) BoolExpression {
	return Lt(i.parent, rhs)
}

func (i *integerInterfaceImpl) LT_EQ(rhs IntegerExpression) BoolExpression {
	return LtEq(i.parent, rhs)
}

func (i *integerInterfaceImpl) ADD(rhs IntegerExpression) IntegerExpression {
	return IntExp(Add(i.parent, rhs))
}

func (i *integerInterfaceImpl) SUB(rhs IntegerExpression) IntegerExpression {
	return IntExp(Sub(i.parent, rhs))
}

func (i *integerInterfaceImpl) MUL(rhs IntegerExpression) IntegerExpression {
	return IntExp(Mul(i.parent, rhs))
}

func (i *integerInterfaceImpl) DIV(rhs IntegerExpression) IntegerExpression {
	return IntExp(Div(i.parent, rhs))
}

func (i *integerInterfaceImpl) MOD(rhs IntegerExpression) IntegerExpression {
	return IntExp(Mod(i.parent, rhs))
}

func (i *integerInterfaceImpl) POW(rhs IntegerExpression) IntegerExpression {
	return IntExp(POW(i.parent, rhs))
}

func (i *integerInterfaceImpl) BIT_AND(rhs IntegerExpression) IntegerExpression {
	return newBinaryIntegerOperatorExpression(i.parent, rhs, "&")
}

func (i *integerInterfaceImpl) BIT_OR(rhs IntegerExpression) IntegerExpression {
	return newBinaryIntegerOperatorExpression(i.parent, rhs, "|")
}

func (i *integerInterfaceImpl) BIT_XOR(rhs IntegerExpression) IntegerExpression {
	return newBinaryIntegerOperatorExpression(i.parent, rhs, "#")
}

func (i *integerInterfaceImpl) BIT_SHIFT_LEFT(intExpression IntegerExpression) IntegerExpression {
	return newBinaryIntegerOperatorExpression(i.parent, intExpression, "<<")
}

func (i *integerInterfaceImpl) BIT_SHIFT_RIGHT(intExpression IntegerExpression) IntegerExpression {
	return newBinaryIntegerOperatorExpression(i.parent, intExpression, ">>")
}

//---------------------------------------------------//
func newBinaryIntegerOperatorExpression(lhs, rhs IntegerExpression, operator string) IntegerExpression {
	return IntExp(NewBinaryOperatorExpression(lhs, rhs, operator))
}

//---------------------------------------------------//
func newPrefixIntegerOperatorExpression(expression IntegerExpression, operator string) IntegerExpression {
	return IntExp(newPrefixOperatorExpression(expression, operator))
}

//---------------------------------------------------//
type integerExpressionWrapper struct {
	integerInterfaceImpl

	Expression
}

func newIntExpressionWrap(expression Expression) IntegerExpression {
	intExpressionWrap := integerExpressionWrapper{Expression: expression}

	intExpressionWrap.integerInterfaceImpl.parent = &intExpressionWrap

	return &intExpressionWrap
}

// IntExp is int expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as int expression.
// Does not add sql cast to generated sql builder output.
func IntExp(expression Expression) IntegerExpression {
	return newIntExpressionWrap(expression)
}
