package jet

import (
	"errors"
)

// Expression is common interface for all expressions.
// Can be Bool, Int, Float, String, Date, Time, Timez, Timestamp or Timestampz expressions.
type Expression interface {
	Serializer
	Projection
	GroupByClause
	OrderByClause

	// Test expression whether it is a NULL value.
	IS_NULL() BoolExpression
	// Test expression whether it is a non-NULL value.
	IS_NOT_NULL() BoolExpression

	// Check if this expressions matches any in expressions list
	IN(expressions ...Expression) BoolExpression
	// Check if this expressions is different of all expressions in expressions list
	NOT_IN(expressions ...Expression) BoolExpression

	// The temporary alias name to assign to the expression
	AS(alias string) Projection

	// Expression will be used to sort query result in ascending order
	ASC() OrderByClause
	// Expression will be used to sort query result in ascending order
	DESC() OrderByClause
}

type ExpressionInterfaceImpl struct {
	Parent Expression
}

func (e *ExpressionInterfaceImpl) fromImpl(subQuery SelectTable) Projection {
	return e.Parent
}

func (e *ExpressionInterfaceImpl) IS_NULL() BoolExpression {
	return newPostifxBoolExpression(e.Parent, "IS NULL")
}

func (e *ExpressionInterfaceImpl) IS_NOT_NULL() BoolExpression {
	return newPostifxBoolExpression(e.Parent, "IS NOT NULL")
}

func (e *ExpressionInterfaceImpl) IN(expressions ...Expression) BoolExpression {
	return newBinaryBoolOperator(e.Parent, WRAP(expressions...), "IN")
}

func (e *ExpressionInterfaceImpl) NOT_IN(expressions ...Expression) BoolExpression {
	return newBinaryBoolOperator(e.Parent, WRAP(expressions...), "NOT IN")
}

func (e *ExpressionInterfaceImpl) AS(alias string) Projection {
	return newAlias(e.Parent, alias)
}

func (e *ExpressionInterfaceImpl) ASC() OrderByClause {
	return newOrderByClause(e.Parent, true)
}

func (e *ExpressionInterfaceImpl) DESC() OrderByClause {
	return newOrderByClause(e.Parent, false)
}

func (e *ExpressionInterfaceImpl) serializeForGroupBy(statement StatementType, out *SqlBuilder) error {
	return e.Parent.serialize(statement, out, noWrap)
}

func (e *ExpressionInterfaceImpl) serializeForProjection(statement StatementType, out *SqlBuilder) error {
	return e.Parent.serialize(statement, out, noWrap)
}

func (e *ExpressionInterfaceImpl) serializeForOrderBy(statement StatementType, out *SqlBuilder) error {
	return e.Parent.serialize(statement, out, noWrap)
}

// Representation of binary operations (e.g. comparisons, arithmetic)
type binaryOpExpression struct {
	lhs, rhs Expression
	operator string
}

func newBinaryExpression(lhs, rhs Expression, operator string) binaryOpExpression {
	binaryExpression := binaryOpExpression{
		lhs:      lhs,
		rhs:      rhs,
		operator: operator,
	}

	return binaryExpression
}

func (c *binaryOpExpression) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) (err error) {
	if c == nil {
		return errors.New("jet: binary Expression is nil")
	}
	if c.lhs == nil {
		return errors.New("jet: nil lhs")
	}
	if c.rhs == nil {
		return errors.New("jet: nil rhs")
	}

	wrap := !contains(options, noWrap)

	if wrap {
		out.WriteString("(")
	}

	if serializeOverride := out.Dialect.SerializeOverride(c.operator); serializeOverride != nil {

		serializeOverrideFunc := serializeOverride(c.lhs, c.rhs)
		err = serializeOverrideFunc(statement, out, options...)

	} else {
		if err := c.lhs.serialize(statement, out); err != nil {
			return err
		}

		out.WriteString(c.operator)

		if err := c.rhs.serialize(statement, out); err != nil {
			return err
		}
	}

	if wrap {
		out.WriteString(")")
	}

	return err
}

// A prefix operator Expression
type prefixOpExpression struct {
	expression Expression
	operator   string
}

func newPrefixExpression(expression Expression, operator string) prefixOpExpression {
	prefixExpression := prefixOpExpression{
		expression: expression,
		operator:   operator,
	}

	return prefixExpression
}

func (p *prefixOpExpression) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) error {
	if p == nil {
		return errors.New("jet: Prefix Expression is nil")
	}

	out.WriteString("(")
	out.WriteString(p.operator)

	if p.expression == nil {
		return errors.New("jet: nil prefix Expression")
	}
	if err := p.expression.serialize(statement, out); err != nil {
		return err
	}

	out.WriteString(")")

	return nil
}

// A postifx operator Expression
type postfixOpExpression struct {
	expression Expression
	operator   string
}

func newPostfixOpExpression(expression Expression, operator string) postfixOpExpression {
	postfixOpExpression := postfixOpExpression{
		expression: expression,
		operator:   operator,
	}

	return postfixOpExpression
}

func (p *postfixOpExpression) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) error {
	if p == nil {
		return errors.New("jet: Postifx operator Expression is nil")
	}

	if p.expression == nil {
		return errors.New("jet: nil prefix Expression")
	}
	if err := p.expression.serialize(statement, out); err != nil {
		return err
	}

	out.WriteString(p.operator)

	return nil
}
