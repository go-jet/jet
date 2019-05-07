package sqlbuilder

type boolExpression interface {
	expression

	Eq(expression boolExpression) boolExpression
	NotEq(expression boolExpression) boolExpression
	GtEq(rhs expression) boolExpression
	LtEq(rhs expression) boolExpression

	AND(expression boolExpression) boolExpression
	OR(expression boolExpression) boolExpression
	IS_TRUE() boolExpression
	IS_FALSE() boolExpression
}

type boolInterfaceImpl struct {
	parent boolExpression
}

func (b *boolInterfaceImpl) Eq(expression boolExpression) boolExpression {
	return Eq(b.parent, expression)
}

func (b *boolInterfaceImpl) NotEq(expression boolExpression) boolExpression {
	return NotEq(b.parent, expression)
}

func (b *boolInterfaceImpl) GtEq(rhs expression) boolExpression {
	return GtEq(b.parent, rhs)
}

func (b *boolInterfaceImpl) LtEq(rhs expression) boolExpression {
	return LtEq(b.parent, rhs)
}

func (b *boolInterfaceImpl) AND(expression boolExpression) boolExpression {
	return And(b.parent, expression)
}

func (b *boolInterfaceImpl) OR(expression boolExpression) boolExpression {
	return Or(b.parent, expression)
}
func (b *boolInterfaceImpl) IS_TRUE() boolExpression {
	return IsTrue(b.parent)
}

func (b *boolInterfaceImpl) IS_FALSE() boolExpression {
	return nil
}

//---------------------------------------------------//
type boolLiteralExpression struct {
	boolInterfaceImpl
	literalExpression
}

func newBoolLiteralExpression(value bool) boolExpression {
	boolLiteralExpression := boolLiteralExpression{}

	boolLiteralExpression.literalExpression = *Literal(value)
	boolLiteralExpression.boolInterfaceImpl.parent = &boolLiteralExpression

	return &boolLiteralExpression
}

//---------------------------------------------------//
type binaryBoolExpression struct {
	expressionInterfaceImpl
	boolInterfaceImpl

	binaryExpression
}

func newBinaryBoolExpression(lhs, rhs expression, operator string) boolExpression {
	boolExpression := binaryBoolExpression{}

	boolExpression.binaryExpression = newBinaryExpression(lhs, rhs, operator)
	boolExpression.expressionInterfaceImpl.parent = &boolExpression
	boolExpression.boolInterfaceImpl.parent = &boolExpression

	return &boolExpression
}

//---------------------------------------------------//
type prefixBoolExpression struct {
	expressionInterfaceImpl
	boolInterfaceImpl

	prefixExpression
}

func newPrefixBoolExpression(expression expression, operator string) boolExpression {
	boolExpression := prefixBoolExpression{}
	boolExpression.prefixExpression = newPrefixExpression(expression, operator)

	boolExpression.expressionInterfaceImpl.parent = &boolExpression
	boolExpression.boolInterfaceImpl.parent = &boolExpression

	return &boolExpression
}

func EXISTS(subQuery selectStatement) boolExpression {
	return newPrefixBoolExpression(subQuery, "EXISTS")
}

// Returns a representation of "a=b"
func Eq(lhs, rhs expression) boolExpression {
	return newBinaryBoolExpression(lhs, rhs, "=")
}

// Returns a representation of "a=b", where b is a literal
func EqL(lhs expression, val interface{}) boolExpression {
	return Eq(lhs, Literal(val))
}

// Returns a representation of "a!=b"
func NotEq(lhs, rhs expression) boolExpression {
	return newBinaryBoolExpression(lhs, rhs, "!=")
}

// Returns a representation of "a!=b", where b is a literal
func NeqL(lhs expression, val interface{}) boolExpression {
	return NotEq(lhs, Literal(val))
}

// Returns a representation of "a<b"
func Lt(lhs expression, rhs expression) boolExpression {
	return newBinaryBoolExpression(lhs, rhs, "<")
}

// Returns a representation of "a<b", where b is a literal
func LtL(lhs expression, val interface{}) boolExpression {
	return Lt(lhs, Literal(val))
}

// Returns a representation of "a<=b"
func LtEq(lhs, rhs expression) boolExpression {
	return newBinaryBoolExpression(lhs, rhs, "<=")
}

// Returns a representation of "a<=b", where b is a literal
func LteL(lhs expression, val interface{}) boolExpression {
	return LtEq(lhs, Literal(val))
}

// Returns a representation of "a>b"
func Gt(lhs, rhs expression) boolExpression {
	return newBinaryBoolExpression(lhs, rhs, ">")
}

// Returns a representation of "a>b", where b is a literal
func GtL(lhs expression, val interface{}) boolExpression {
	return Gt(lhs, Literal(val))
}

// Returns a representation of "a>=b"
func GtEq(lhs, rhs expression) boolExpression {
	return newBinaryBoolExpression(lhs, rhs, ">=")
}

// Returns a representation of "a>=b", where b is a literal
func GteL(lhs expression, val interface{}) boolExpression {
	return GtEq(lhs, Literal(val))
}

// Returns a representation of "not expr"
func Not(expr boolExpression) boolExpression {
	return newPrefixBoolExpression(expr, "NOT")
}

func IsTrue(expr boolExpression) boolExpression {
	return newPrefixBoolExpression(expr, "IS TRUE")
}

func And(lhs, rhs expression) boolExpression {
	return newBinaryBoolExpression(lhs, rhs, "AND")
}

// Returns a representation of "c[0] OR ... OR c[n-1]" for c in clauses
func Or(lhs, rhs expression) boolExpression {
	return newBinaryBoolExpression(lhs, rhs, "OR")
}

func Like(lhs, rhs expression) boolExpression {
	return newBinaryBoolExpression(lhs, rhs, "LIKE")
}

func LikeL(lhs expression, val string) boolExpression {
	return Like(lhs, Literal(val))
}

func Regexp(lhs, rhs expression) boolExpression {
	return newBinaryBoolExpression(lhs, rhs, "REGEXP")
}

func RegexpL(lhs expression, val string) boolExpression {
	return Regexp(lhs, Literal(val))
}
