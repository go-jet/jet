package sqlbuilder

type BoolExpression interface {
	Expression

	Eq(expression BoolExpression) BoolExpression
	NotEq(expression BoolExpression) BoolExpression
	GtEq(rhs Expression) BoolExpression
	LtEq(rhs Expression) BoolExpression

	AND(expression BoolExpression) BoolExpression
	OR(expression BoolExpression) BoolExpression
	IS_TRUE() BoolExpression
	IS_FALSE() BoolExpression
}

type boolInterfaceImpl struct {
	parent BoolExpression
}

func (b *boolInterfaceImpl) Eq(expression BoolExpression) BoolExpression {
	return Eq(b.parent, expression)
}

func (b *boolInterfaceImpl) NotEq(expression BoolExpression) BoolExpression {
	return NotEq(b.parent, expression)
}

func (b *boolInterfaceImpl) GtEq(rhs Expression) BoolExpression {
	return GtEq(b.parent, rhs)
}

func (b *boolInterfaceImpl) LtEq(rhs Expression) BoolExpression {
	return LtEq(b.parent, rhs)
}

func (b *boolInterfaceImpl) AND(expression BoolExpression) BoolExpression {
	return And(b.parent, expression)
}

func (b *boolInterfaceImpl) OR(expression BoolExpression) BoolExpression {
	return Or(b.parent, expression)
}
func (b *boolInterfaceImpl) IS_TRUE() BoolExpression {
	return IsTrue(b.parent)
}

func (b *boolInterfaceImpl) IS_FALSE() BoolExpression {
	return nil
}

//---------------------------------------------------//
type boolLiteralExpression struct {
	boolInterfaceImpl
	literalExpression
}

func newBoolLiteralExpression(value bool) BoolExpression {
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

func newBinaryBoolExpression(lhs, rhs Expression, operator string) BoolExpression {
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

func newPrefixBoolExpression(expression Expression, operator string) BoolExpression {
	boolExpression := prefixBoolExpression{}
	boolExpression.prefixExpression = newPrefixExpression(expression, operator)

	boolExpression.expressionInterfaceImpl.parent = &boolExpression
	boolExpression.boolInterfaceImpl.parent = &boolExpression

	return &boolExpression
}

func EXISTS(subQuery SelectStatement) BoolExpression {
	return newPrefixBoolExpression(subQuery, "EXISTS")
}

// Returns a representation of "a=b"
func Eq(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, "=")
}

// Returns a representation of "a=b", where b is a literal
func EqL(lhs Expression, val interface{}) BoolExpression {
	return Eq(lhs, Literal(val))
}

// Returns a representation of "a!=b"
func NotEq(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, "!=")
}

// Returns a representation of "a!=b", where b is a literal
func NeqL(lhs Expression, val interface{}) BoolExpression {
	return NotEq(lhs, Literal(val))
}

// Returns a representation of "a<b"
func Lt(lhs Expression, rhs Expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, "<")
}

// Returns a representation of "a<b", where b is a literal
func LtL(lhs Expression, val interface{}) BoolExpression {
	return Lt(lhs, Literal(val))
}

// Returns a representation of "a<=b"
func LtEq(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, "<=")
}

// Returns a representation of "a<=b", where b is a literal
func LteL(lhs Expression, val interface{}) BoolExpression {
	return LtEq(lhs, Literal(val))
}

// Returns a representation of "a>b"
func Gt(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, ">")
}

// Returns a representation of "a>b", where b is a literal
func GtL(lhs Expression, val interface{}) BoolExpression {
	return Gt(lhs, Literal(val))
}

// Returns a representation of "a>=b"
func GtEq(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, ">=")
}

// Returns a representation of "a>=b", where b is a literal
func GteL(lhs Expression, val interface{}) BoolExpression {
	return GtEq(lhs, Literal(val))
}

// Returns a representation of "not expr"
func Not(expr BoolExpression) BoolExpression {
	return newPrefixBoolExpression(expr, "NOT")
}

func IsTrue(expr BoolExpression) BoolExpression {
	return newPrefixBoolExpression(expr, "IS TRUE")
}

func And(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, "AND")
}

// Returns a representation of "c[0] OR ... OR c[n-1]" for c in clauses
func Or(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, "OR")
}

func Like(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, "LIKE")
}

func LikeL(lhs Expression, val string) BoolExpression {
	return Like(lhs, Literal(val))
}

func Regexp(lhs, rhs Expression) BoolExpression {
	return newBinaryBoolExpression(lhs, rhs, "REGEXP")
}

func RegexpL(lhs Expression, val string) BoolExpression {
	return Regexp(lhs, Literal(val))
}
