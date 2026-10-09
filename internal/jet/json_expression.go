package jet

// JsonExpression is an expression of the PostgreSQL json type. The jsonb type is
// modelled separately (JsonbExpression) so that jsonb-only operators are only
// available on jsonb expressions at compile time; converting between the two
// requires an explicit cast.
type JsonExpression interface {
	Expression
	isJson()

	// Arrow returns a representation of "lhs -> rhs" (extract JSON object field by key).
	Arrow(rhs Expression) JsonExpression
	// ArrowText returns a representation of "lhs ->> rhs" (extract JSON object field by key as text).
	ArrowText(rhs Expression) StringExpression
	// HashArrow returns a representation of "lhs #> rhs" (extract JSON sub-object at the specified path).
	HashArrow(rhs Expression) JsonExpression
	// HashArrowText returns a representation of "lhs #>> rhs" (extract JSON sub-object at the specified path as text).
	HashArrowText(rhs Expression) StringExpression
	// Concat returns a representation of "lhs || rhs".
	Concat(rhs JsonExpression) JsonExpression

	EQ(rhs JsonExpression) BoolExpression
	NOT_EQ(rhs JsonExpression) BoolExpression
	IS_DISTINCT_FROM(rhs JsonExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs JsonExpression) BoolExpression
}

type jsonInterfaceImpl struct {
	root JsonExpression
}

func (j *jsonInterfaceImpl) isJson() {}

func (j *jsonInterfaceImpl) Arrow(rhs Expression) JsonExpression {
	return newBinaryJsonOperatorExpression(j.root, rhs, "->")
}

func (j *jsonInterfaceImpl) ArrowText(rhs Expression) StringExpression {
	return StringExp(NewBinaryOperatorExpression(j.root, rhs, "->>"))
}

func (j *jsonInterfaceImpl) HashArrow(rhs Expression) JsonExpression {
	return newBinaryJsonOperatorExpression(j.root, rhs, "#>")
}

func (j *jsonInterfaceImpl) HashArrowText(rhs Expression) StringExpression {
	return StringExp(NewBinaryOperatorExpression(j.root, rhs, "#>>"))
}

func (j *jsonInterfaceImpl) Concat(rhs JsonExpression) JsonExpression {
	return newBinaryJsonOperatorExpression(j.root, rhs, "||")
}

func (j *jsonInterfaceImpl) EQ(rhs JsonExpression) BoolExpression {
	return Eq(j.root, rhs)
}

func (j *jsonInterfaceImpl) NOT_EQ(rhs JsonExpression) BoolExpression {
	return NotEq(j.root, rhs)
}

func (j *jsonInterfaceImpl) IS_DISTINCT_FROM(rhs JsonExpression) BoolExpression {
	return IsDistinctFrom(j.root, rhs)
}

func (j *jsonInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs JsonExpression) BoolExpression {
	return IsNotDistinctFrom(j.root, rhs)
}

func newBinaryJsonOperatorExpression(lhs, rhs Expression, operator string) JsonExpression {
	root := NewBinaryOperatorExpression(lhs, rhs, operator)
	jsonExp := &jsonExpressionWrapper{
		jsonInterfaceImpl: jsonInterfaceImpl{},
		Expression:        root,
	}
	jsonExp.jsonInterfaceImpl.root = jsonExp
	root.setRoot(jsonExp)
	return jsonExp
}

// ---------------------------------------------------//

type jsonExpressionWrapper struct {
	jsonInterfaceImpl
	Expression
}

func newJsonExpressionWrap(expression Expression) JsonExpression {
	jsonExpressionWrap := &jsonExpressionWrapper{
		jsonInterfaceImpl: jsonInterfaceImpl{},
		Expression:        expression,
	}
	jsonExpressionWrap.jsonInterfaceImpl.root = jsonExpressionWrap
	expression.setRoot(jsonExpressionWrap)
	return jsonExpressionWrap
}

// JsonExp is a json expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as json expression.
// Does not add sql cast to generated sql builder output.
func JsonExp(expression Expression) JsonExpression {
	return newJsonExpressionWrap(expression)
}

//----------------- jsonb -----------------//

// JsonbExpression is an expression of the PostgreSQL jsonb type. jsonb-only
// operators (containment, existence, deletion) are only available here, so they
// cannot be applied to a json expression without an explicit cast.
type JsonbExpression interface {
	Expression
	isJsonb()

	// Arrow returns a representation of "lhs -> rhs" (extract JSON object field by key).
	Arrow(rhs Expression) JsonbExpression
	// ArrowText returns a representation of "lhs ->> rhs" (extract JSON object field by key as text).
	ArrowText(rhs Expression) StringExpression
	// HashArrow returns a representation of "lhs #> rhs" (extract JSON sub-object at the specified path).
	HashArrow(rhs Expression) JsonbExpression
	// HashArrowText returns a representation of "lhs #>> rhs" (extract JSON sub-object at the specified path as text).
	HashArrowText(rhs Expression) StringExpression
	// Concat returns a representation of "lhs || rhs".
	Concat(rhs JsonbExpression) JsonbExpression

	// Contains returns a representation of "lhs @> rhs" (lhs contains rhs).
	Contains(rhs JsonbExpression) BoolExpression
	// IsContainedBy returns a representation of "lhs <@ rhs" (lhs is contained by rhs).
	IsContainedBy(rhs JsonbExpression) BoolExpression
	// Exists returns a representation of "lhs ? rhs" (string exists as a top-level key).
	Exists(rhs StringExpression) BoolExpression
	// ExistsAny returns a representation of "lhs ?| rhs" (any of the strings exist as top-level keys).
	ExistsAny(rhs StringExpression) BoolExpression
	// ExistsAll returns a representation of "lhs ?& rhs" (all of the strings exist as top-level keys).
	ExistsAll(rhs StringExpression) BoolExpression
	// Remove returns a representation of "lhs - rhs" (deletes key or element).
	Remove(rhs Expression) JsonbExpression
	// RemovePath returns a representation of "lhs #- rhs" (delete path).
	RemovePath(rhs Expression) JsonbExpression

	EQ(rhs JsonbExpression) BoolExpression
	NOT_EQ(rhs JsonbExpression) BoolExpression
	IS_DISTINCT_FROM(rhs JsonbExpression) BoolExpression
	IS_NOT_DISTINCT_FROM(rhs JsonbExpression) BoolExpression
}

type jsonbInterfaceImpl struct {
	root JsonbExpression
}

func (j *jsonbInterfaceImpl) isJsonb() {}

func (j *jsonbInterfaceImpl) Arrow(rhs Expression) JsonbExpression {
	return newBinaryJsonbOperatorExpression(j.root, rhs, "->")
}

func (j *jsonbInterfaceImpl) ArrowText(rhs Expression) StringExpression {
	return StringExp(NewBinaryOperatorExpression(j.root, rhs, "->>"))
}

func (j *jsonbInterfaceImpl) HashArrow(rhs Expression) JsonbExpression {
	return newBinaryJsonbOperatorExpression(j.root, rhs, "#>")
}

func (j *jsonbInterfaceImpl) HashArrowText(rhs Expression) StringExpression {
	return StringExp(NewBinaryOperatorExpression(j.root, rhs, "#>>"))
}

func (j *jsonbInterfaceImpl) Concat(rhs JsonbExpression) JsonbExpression {
	return newBinaryJsonbOperatorExpression(j.root, rhs, "||")
}

func (j *jsonbInterfaceImpl) Contains(rhs JsonbExpression) BoolExpression {
	return newBinaryBoolOperatorExpression(j.root, rhs, "@>")
}

func (j *jsonbInterfaceImpl) IsContainedBy(rhs JsonbExpression) BoolExpression {
	return newBinaryBoolOperatorExpression(j.root, rhs, "<@")
}

func (j *jsonbInterfaceImpl) Exists(rhs StringExpression) BoolExpression {
	return newBinaryBoolOperatorExpression(j.root, rhs, "?")
}

func (j *jsonbInterfaceImpl) ExistsAny(rhs StringExpression) BoolExpression {
	return newBinaryBoolOperatorExpression(j.root, rhs, "?|")
}

func (j *jsonbInterfaceImpl) ExistsAll(rhs StringExpression) BoolExpression {
	return newBinaryBoolOperatorExpression(j.root, rhs, "?&")
}

func (j *jsonbInterfaceImpl) Remove(rhs Expression) JsonbExpression {
	return newBinaryJsonbOperatorExpression(j.root, rhs, "-")
}

func (j *jsonbInterfaceImpl) RemovePath(rhs Expression) JsonbExpression {
	return newBinaryJsonbOperatorExpression(j.root, rhs, "#-")
}

func (j *jsonbInterfaceImpl) EQ(rhs JsonbExpression) BoolExpression {
	return Eq(j.root, rhs)
}

func (j *jsonbInterfaceImpl) NOT_EQ(rhs JsonbExpression) BoolExpression {
	return NotEq(j.root, rhs)
}

func (j *jsonbInterfaceImpl) IS_DISTINCT_FROM(rhs JsonbExpression) BoolExpression {
	return IsDistinctFrom(j.root, rhs)
}

func (j *jsonbInterfaceImpl) IS_NOT_DISTINCT_FROM(rhs JsonbExpression) BoolExpression {
	return IsNotDistinctFrom(j.root, rhs)
}

func newBinaryJsonbOperatorExpression(lhs, rhs Expression, operator string) JsonbExpression {
	root := NewBinaryOperatorExpression(lhs, rhs, operator)
	jsonbExp := &jsonbExpressionWrapper{
		jsonbInterfaceImpl: jsonbInterfaceImpl{},
		Expression:         root,
	}
	jsonbExp.jsonbInterfaceImpl.root = jsonbExp
	root.setRoot(jsonbExp)
	return jsonbExp
}

// ---------------------------------------------------//

type jsonbExpressionWrapper struct {
	jsonbInterfaceImpl
	Expression
}

func newJsonbExpressionWrap(expression Expression) JsonbExpression {
	jsonbExpressionWrap := &jsonbExpressionWrapper{
		jsonbInterfaceImpl: jsonbInterfaceImpl{},
		Expression:         expression,
	}
	jsonbExpressionWrap.jsonbInterfaceImpl.root = jsonbExpressionWrap
	expression.setRoot(jsonbExpressionWrap)
	return jsonbExpressionWrap
}

// JsonbExp is a jsonb expression wrapper around arbitrary expression.
// Allows go compiler to see any expression as jsonb expression.
// Does not add sql cast to generated sql builder output.
func JsonbExp(expression Expression) JsonbExpression {
	return newJsonbExpressionWrap(expression)
}
