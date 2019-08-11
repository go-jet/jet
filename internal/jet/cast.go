package jet

import "strconv"

type Cast interface {
	AS(castType string) Expression

	AS_CHAR(lenght ...int) StringExpression
	// Cast expression AS date type
	AS_DATE() DateExpression
	// Cast expression AS numeric type, using precision and optionally scale
	AS_DECIMAL() FloatExpression
	// Cast expression AS time type
	AS_TIME() TimeExpression
}

type CastImpl struct {
	expression Expression
}

func NewCastImpl(expression Expression) CastImpl {
	castImpl := CastImpl{
		expression: expression,
	}

	return castImpl
}

func (b *CastImpl) AS(castType string) Expression {
	castExp := &castExpression{
		expression: b.expression,
		cast:       string(castType),
	}

	castExp.ExpressionInterfaceImpl.Parent = castExp

	return castExp
}

func (b *CastImpl) AS_CHAR(lenght ...int) StringExpression {
	if len(lenght) > 0 {
		return StringExp(b.AS("CHAR(" + strconv.Itoa(lenght[0]) + ")"))
	}

	return StringExp(b.AS("CHAR"))
}

// Cast expression AS date type
func (b *CastImpl) AS_DATE() DateExpression {
	return DateExp(b.AS("DATE"))
}

// Cast expression AS date type
func (b *CastImpl) AS_DECIMAL() FloatExpression {
	return FloatExp(b.AS("DECIMAL"))
}

// Cast expression AS date type
func (b *CastImpl) AS_TIME() TimeExpression {
	return TimeExp(b.AS("TIME"))
}

type castExpression struct {
	ExpressionInterfaceImpl

	expression Expression
	cast       string
}

func (b *castExpression) accept(visitor visitor) {
	b.expression.accept(visitor)
}

func (b *castExpression) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) error {

	expression := b.expression
	castType := b.cast

	if castOverride := out.Dialect.SerializeOverride("CAST"); castOverride != nil {
		return castOverride(expression, String(castType))(statement, out, options...)
	}

	out.WriteString("CAST(")
	err := expression.serialize(statement, out, options...)
	if err != nil {
		return err
	}

	out.WriteString("AS")
	out.WriteString(castType + ")")

	return err
}
