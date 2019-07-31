package jet

type CastType string

type Cast interface {
	As(castType CastType) Expression
}

type CastImpl struct {
	expression Expression
}

func NewCastImpl(expression Expression) Cast {
	castImpl := CastImpl{
		expression: expression,
	}

	return &castImpl
}

func (b *CastImpl) As(castType CastType) Expression {
	castExp := &castExpression{
		expression: b.expression,
		cast:       string(castType),
	}

	castExp.expressionInterfaceImpl.parent = castExp

	return castExp
}

type castExpression struct {
	expressionInterfaceImpl

	expression Expression
	cast       string
}

func (b *castExpression) accept(visitor visitor) {
	b.expression.accept(visitor)
}

func (b *castExpression) serialize(statement statementType, out *sqlBuilder, options ...serializeOption) error {

	expression := b.expression
	castType := b.cast

	if castOverride := out.dialect.CastOverride; castOverride != nil {
		return castOverride(expression, castType)(statement, out, options...)
	}

	out.writeString("CAST(")
	err := expression.serialize(statement, out, options...)
	if err != nil {
		return err
	}

	out.writeString("AS")
	out.writeString(castType + ")")

	return err
}
