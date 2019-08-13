package jet

type Cast interface {
	AS(castType string) Expression
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

func (b *CastImpl) AS(castType string) Expression {
	castExp := &castExpression{
		expression: b.expression,
		cast:       string(castType),
	}

	castExp.ExpressionInterfaceImpl.Parent = castExp

	return castExp
}

type castExpression struct {
	ExpressionInterfaceImpl

	expression Expression
	cast       string
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
