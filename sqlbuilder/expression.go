package sqlbuilder

import (
	"github.com/dropbox/godropbox/errors"
)

// An expression
type Expression interface {
	Clause
	Projection

	IN(subQuery SelectStatement) BoolExpression
	NOT_IN(subQuery SelectStatement) BoolExpression

	AS(alias string) Projection
	IS_DISTINCT_FROM(expression Expression) BoolExpression
	IS_NULL() BoolExpression
	ASC() OrderByClause
	DESC() OrderByClause
}

type expressionInterfaceImpl struct {
	parent Expression
}

func (e *expressionInterfaceImpl) IN(subQuery SelectStatement) BoolExpression {
	return newBinaryBoolExpression(e.parent, subQuery, "IN")
}

func (e *expressionInterfaceImpl) NOT_IN(subQuery SelectStatement) BoolExpression {
	return newBinaryBoolExpression(e.parent, subQuery, "NOT_IN")
}

func (e *expressionInterfaceImpl) AS(alias string) Projection {
	return NewAlias(e.parent, alias)
}

func (e *expressionInterfaceImpl) IS_DISTINCT_FROM(expression Expression) BoolExpression {
	return newBinaryBoolExpression(e.parent, expression, "IS DISTINCT FROM")
}

func (e *expressionInterfaceImpl) IS_NULL() BoolExpression {
	return nil
}

func (e *expressionInterfaceImpl) ASC() OrderByClause {
	return &orderByClause{expression: e.parent, ascent: true}
}

func (e *expressionInterfaceImpl) DESC() OrderByClause {
	return &orderByClause{expression: e.parent, ascent: false}
}

func (e *expressionInterfaceImpl) SerializeForProjection(out *queryData) error {
	return e.parent.Serialize(out, FOR_PROJECTION)
}

// Representation of binary operations (e.g. comparisons, arithmetic)
type binaryExpression struct {
	lhs, rhs Expression
	operator string
}

func newBinaryExpression(lhs, rhs Expression, operator string, parent ...Expression) binaryExpression {
	binaryExpression := binaryExpression{
		lhs:      lhs,
		rhs:      rhs,
		operator: operator,
	}

	return binaryExpression
}

func isSimpleOperand(expression Expression) bool {
	if _, ok := expression.(*literalExpression); ok {
		return true
	}
	if _, ok := expression.(Column); ok {
		return true
	}
	if _, ok := expression.(*numericFunc); ok {
		return true
	}

	return false
}

func (c *binaryExpression) Serialize(out *queryData, options ...serializeOption) error {
	if c.lhs == nil {
		return errors.Newf("nil lhs.")
	}
	if c.rhs == nil {
		return errors.Newf("nil rhs.")
	}

	wrap := !isSimpleOperand(c.lhs) && !isSimpleOperand(c.rhs)

	if wrap {
		out.WriteString("(")
	}

	if err := c.lhs.Serialize(out); err != nil {
		return err
	}

	out.WriteString(" " + c.operator + " ")

	if err := c.rhs.Serialize(out); err != nil {
		return err
	}

	if wrap {
		out.WriteString(")")
	}

	return nil
}

// A not expression which negates a expression value
type prefixExpression struct {
	expression Expression
	operator   string
}

func newPrefixExpression(expression Expression, operator string) prefixExpression {
	prefixExpression := prefixExpression{
		expression: expression,
		operator:   operator,
	}

	return prefixExpression
}

func (p *prefixExpression) Serialize(out *queryData, options ...serializeOption) error {
	out.WriteString(p.operator + " ")

	if p.expression == nil {
		return errors.Newf("nil prefix expression.")
	}
	if err := p.expression.Serialize(out); err != nil {
		return err
	}

	return nil
}
