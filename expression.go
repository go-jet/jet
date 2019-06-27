package jet

import (
	"errors"
	"fmt"
)

// Common expression interface
type Expression interface {
	clause
	projection
	groupByClause
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
	AS(alias string) projection

	// Expression will be used to sort query result in ascending order
	ASC() OrderByClause
	// Expression will be used to sort query result in ascending order
	DESC() OrderByClause

	// Cast expression to dbType
	TO(dbType string) Expression
	// Cast expression to bool type
	TO_BOOL() BoolExpression
	// Cast expression to smallint type
	TO_SMALLINT() IntegerExpression
	// Cast expression to integer type
	TO_INTEGER() IntegerExpression
	// Cast expression to bigint type
	TO_BIGINT() IntegerExpression
	// Cast expression to numeric type, using precision and optionally scale
	TO_NUMERIC(precision int, scale ...int) FloatExpression
	// Cast expression to real type
	TO_REAL() FloatExpression
	// Cast expression to double precision type
	TO_DOUBLE() FloatExpression
	// Cast expression to text type
	TO_TEXT() StringExpression
	// Cast expression to date type
	TO_DATE() DateExpression
	// Cast expression to time type
	TO_TIME() TimeExpression
	// Cast expression to time with time timezone type
	TO_TIMEZ() TimezExpression
	// Cast expression to timestamp type
	TO_TIMESTAMP() TimestampExpression
	// Cast expression to timestamp with timezone type
	TO_TIMESTAMPZ() TimestampzExpression
}

type expressionInterfaceImpl struct {
	parent Expression
}

func (e *expressionInterfaceImpl) from(subQuery ExpressionTable) projection {
	return e.parent
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
	return newAlias(e.parent, alias)
}

func (e *expressionInterfaceImpl) ASC() OrderByClause {
	return newOrderByClause(e.parent, true)
}

func (e *expressionInterfaceImpl) DESC() OrderByClause {
	return newOrderByClause(e.parent, false)
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
	return e.parent.serialize(statement, out, noWrap)
}

func (e *expressionInterfaceImpl) serializeForProjection(statement statementType, out *queryData) error {
	return e.parent.serialize(statement, out, noWrap)
}

func (e *expressionInterfaceImpl) serializeForOrderBy(statement statementType, out *queryData) error {
	return e.parent.serialize(statement, out, noWrap)
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
		return errors.New("binary Expression is nil")
	}
	if c.lhs == nil {
		return errors.New("nil lhs")
	}
	if c.rhs == nil {
		return errors.New("nil rhs")
	}

	wrap := !contains(options, noWrap)

	if wrap {
		out.writeString("(")
	}

	if err := c.lhs.serialize(statement, out); err != nil {
		return err
	}

	out.writeString(c.operator)

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
