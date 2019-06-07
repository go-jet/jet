package sqlbuilder

import (
	"errors"
	"fmt"
)

// An Expression
type Expression interface {
	clause
	projection
	groupByClause
	OrderByClause

	IS_NULL() BoolExpression
	IS_NOT_NULL() BoolExpression

	IN(expressions ...Expression) BoolExpression
	NOT_IN(expressions ...Expression) BoolExpression

	AS(alias string) projection

	ASC() OrderByClause
	DESC() OrderByClause

	TO(dbType string) Expression
	TO_BOOL() BoolExpression
	TO_SMALLINT() IntegerExpression
	TO_INTEGER() IntegerExpression
	TO_BIGINT() IntegerExpression
	TO_NUMERIC(precision int, scale ...int) FloatExpression
	TO_REAL() FloatExpression
	TO_DOUBLE() FloatExpression
	TO_TEXT() StringExpression
	TO_DATE() DateExpression
	TO_TIME() TimeExpression
	TO_TIMEZ() TimezExpression
	TO_TIMESTAMP() TimestampExpression
	TO_TIMESTAMPZ() TimestampzExpression
}

type expressionInterfaceImpl struct {
	parent Expression
}

func (e *expressionInterfaceImpl) IS_NULL() BoolExpression {
	return newPostifxBoolExpression(e.parent, "IS NULL")
}

func (e *expressionInterfaceImpl) IS_NOT_NULL() BoolExpression {
	return newPostifxBoolExpression(e.parent, "IS NOT NULL")
}

func (e *expressionInterfaceImpl) IN(expressions ...Expression) BoolExpression {
	return newBinaryBoolExpression(e.parent, WRAP(expressions...), "IN")
}

func (e *expressionInterfaceImpl) NOT_IN(expressions ...Expression) BoolExpression {
	return newBinaryBoolExpression(e.parent, WRAP(expressions...), "NOT IN")
}

func (e *expressionInterfaceImpl) AS(alias string) projection {
	return NewAlias(e.parent, alias)
}

func (e *expressionInterfaceImpl) ASC() OrderByClause {
	return &orderByClauseImpl{expression: e.parent, ascent: true}
}

func (e *expressionInterfaceImpl) DESC() OrderByClause {
	return &orderByClauseImpl{expression: e.parent, ascent: false}
}

func (e *expressionInterfaceImpl) TO(dbType string) Expression {
	return newCast(e.parent, dbType)
}

func (e *expressionInterfaceImpl) TO_BOOL() BoolExpression {
	return newBoolCast(e.parent)
}

func (e *expressionInterfaceImpl) TO_SMALLINT() IntegerExpression {
	return newIntegerCast(e.parent, "smallint")
}

func (e *expressionInterfaceImpl) TO_INTEGER() IntegerExpression {
	return newIntegerCast(e.parent, "integer")
}

func (e *expressionInterfaceImpl) TO_BIGINT() IntegerExpression {
	return newIntegerCast(e.parent, "bigint")
}

func (e *expressionInterfaceImpl) TO_NUMERIC(precision int, scale ...int) FloatExpression {
	var castType string
	if len(scale) > 0 {
		castType = fmt.Sprintf("numeric(%d, %d)", precision, scale[0])
	} else {
		castType = fmt.Sprintf("numeric(%d)", precision)
	}
	return newFloatCast(e.parent, castType)
}

func (e *expressionInterfaceImpl) TO_REAL() FloatExpression {
	return newFloatCast(e.parent, "real")
}

func (e *expressionInterfaceImpl) TO_DOUBLE() FloatExpression {
	return newFloatCast(e.parent, "double precision")
}

func (e *expressionInterfaceImpl) TO_TEXT() StringExpression {
	return newTextCast(e.parent)
}

func (e *expressionInterfaceImpl) TO_DATE() DateExpression {
	return newDateCast(e.parent)
}

func (e *expressionInterfaceImpl) TO_TIME() TimeExpression {
	return newTimeCast(e.parent)
}

func (e *expressionInterfaceImpl) TO_TIMEZ() TimezExpression {
	return newTimezCast(e.parent)
}

func (e *expressionInterfaceImpl) TO_TIMESTAMP() TimestampExpression {
	return newTimestampCast(e.parent)
}

func (e *expressionInterfaceImpl) TO_TIMESTAMPZ() TimestampzExpression {
	return newTimestampzCast(e.parent)
}

func (e *expressionInterfaceImpl) serializeForGroupBy(statement statementType, out *queryData) error {
	return e.parent.serialize(statement, out, NO_WRAP)
}

func (e *expressionInterfaceImpl) serializeForProjection(statement statementType, out *queryData) error {
	return e.parent.serialize(statement, out, NO_WRAP)
}

func (e *expressionInterfaceImpl) serializeAsOrderBy(statement statementType, out *queryData) error {
	return e.parent.serialize(statement, out, NO_WRAP)
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

func (c *binaryOpExpression) serialize(statement statementType, out *queryData, options ...serializeOption) error {
	if c == nil {
		return errors.New("Binary Expression is nil.")
	}
	if c.lhs == nil {
		return errors.New("nil lhs.")
	}
	if c.rhs == nil {
		return errors.New("nil rhs.")
	}

	wrap := !contains(options, NO_WRAP)

	if wrap {
		out.writeString("(")
	}

	if err := c.lhs.serialize(statement, out); err != nil {
		return err
	}

	out.writeString(" " + c.operator + " ")

	if err := c.rhs.serialize(statement, out); err != nil {
		return err
	}

	if wrap {
		out.writeString(")")
	}

	return nil
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

func (p *prefixOpExpression) serialize(statement statementType, out *queryData, options ...serializeOption) error {
	if p == nil {
		return errors.New("Prefix Expression is nil.")
	}

	out.writeString(p.operator + " ")

	if p.expression == nil {
		return errors.New("nil prefix Expression.")
	}
	if err := p.expression.serialize(statement, out); err != nil {
		return err
	}

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

func (p *postfixOpExpression) serialize(statement statementType, out *queryData, options ...serializeOption) error {
	if p == nil {
		return errors.New("Postifx operator Expression is nil.")
	}

	if p.expression == nil {
		return errors.New("nil prefix Expression.")
	}
	if err := p.expression.serialize(statement, out); err != nil {
		return err
	}

	out.writeString(p.operator)

	return nil
}
