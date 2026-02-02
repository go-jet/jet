package jet

// StringExpression interface
type StringExpression interface {
	Expression
	isStringOrBlob()

	EQ(rhs StringExpression) BoolExpression
	NOT_EQ(rhs StringExpression) BoolExpression
	IS_DISTINCT_FROM(rhs StringExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs StringExpression) BoolExpression

	LT(rhs StringExpression) BoolExpression
	LT_EQ(rhs StringExpression) BoolExpression
	GT(rhs StringExpression) BoolExpression
	GT_EQ(rhs StringExpression) BoolExpression
	BETWEEN(min, max StringExpression) BoolExpression
	NOT_BETWEEN(min, max StringExpression) BoolExpression

	CONCAT(rhs Expression) StringExpression

	LIKE(pattern StringExpression) BoolExpression
	NOT_LIKE(pattern StringExpression) BoolExpression

	REGEXP_LIKE(pattern StringExpression, caseSensitive ...bool) BoolExpression
	NOT_REGEXP_LIKE(pattern StringExpression, caseSensitive ...bool) BoolExpression
}

type stringInterfaceImpl struct {
	root StringExpression
}

func (s *stringInterfaceImpl) isStringOrBlob() {}

func (s *stringInterfaceImpl) EQ(rhs StringExpression) BoolExpression {
	return Eq(s.root, rhs)
}

func (s *stringInterfaceImpl) NOT_EQ(rhs StringExpression) BoolExpression {
	return NotEq(s.root, rhs)
}

func (s *stringInterfaceImpl) IS_DISTINCT_FROM(rhs StringExpression) BoolExpression {
	return IsDistinctFrom(s.root, rhs)
}

func (s *stringInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs StringExpression) BoolExpression {
	return IsNotDistinctFrom(s.root, rhs)
}

func (s *stringInterfaceImpl) GT(rhs StringExpression) BoolExpression {
	return Gt(s.root, rhs)
}

func (s *stringInterfaceImpl) GT_EQ(rhs StringExpression) BoolExpression {
	return GtEq(s.root, rhs)
}

func (s *stringInterfaceImpl) LT(rhs StringExpression) BoolExpression {
	return Lt(s.root, rhs)
}

func (s *stringInterfaceImpl) LT_EQ(rhs StringExpression) BoolExpression {
	return LtEq(s.root, rhs)
}

func (s *stringInterfaceImpl) BETWEEN(min, max StringExpression) BoolExpression {
	return NewBetweenOperatorExpression(s.root, min, max, false)
}

func (s *stringInterfaceImpl) NOT_BETWEEN(min, max StringExpression) BoolExpression {
	return NewBetweenOperatorExpression(s.root, min, max, true)
}

func (s *stringInterfaceImpl) CONCAT(rhs Expression) StringExpression {
	return newBinaryStringOperatorExpression(s.root, rhs, StringConcatOperator)
}

func (s *stringInterfaceImpl) LIKE(pattern StringExpression) BoolExpression {
	return newBinaryBoolOperatorExpression(s.root, pattern, "LIKE")
}

func (s *stringInterfaceImpl) NOT_LIKE(pattern StringExpression) BoolExpression {
	return newBinaryBoolOperatorExpression(s.root, pattern, "NOT LIKE")
}

func (s *stringInterfaceImpl) REGEXP_LIKE(pattern StringExpression, caseSensitive ...bool) BoolExpression {
	return BoolExp(newExpression(&regexpLikeSerializer{
		str:           s.root,
		pattern:       pattern,
		caseSensitive: len(caseSensitive) > 0 && caseSensitive[0],
	}))
}

func (s *stringInterfaceImpl) NOT_REGEXP_LIKE(pattern StringExpression, caseSensitive ...bool) BoolExpression {
	return BoolExp(newExpression(&regexpLikeSerializer{
		not:           true,
		str:           s.root,
		pattern:       pattern,
		caseSensitive: len(caseSensitive) > 0 && caseSensitive[0],
	}))
}

type regexpLikeSerializer struct {
	not           bool
	str           StringExpression
	pattern       StringExpression
	caseSensitive bool
}

func (r *regexpLikeSerializer) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	optionalWrap(out, options, func(out *SQLBuilder, options []SerializeOption) {
		out.Dialect.RegexpLike(r.str, r.not, r.pattern, r.caseSensitive)(statement, out, options...)
	})
}

// ---------------------------------------------------//
func newBinaryStringOperatorExpression(lhs, rhs Expression, operator string) StringExpression {
	return StringExp(NewBinaryOperatorExpression(lhs, rhs, operator))
}

//---------------------------------------------------//

type stringExpressionWrapper struct {
	stringInterfaceImpl
	Expression
}

func newStringExpressionWrap(expression Expression) StringExpression {
	stringExpressionWrap := &stringExpressionWrapper{Expression: expression}
	stringExpressionWrap.stringInterfaceImpl.root = stringExpressionWrap
	expression.setRoot(stringExpressionWrap)
	return stringExpressionWrap
}

// StringExp is string expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as string expression.
// Does not add sql cast to generated sql builder output.
func StringExp(expression Expression) StringExpression {
	return newStringExpressionWrap(expression)
}
