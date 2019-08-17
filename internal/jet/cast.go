package jet

// Cast interface
type Cast interface {
	AS(castType string) Expression
}

type castImpl struct {
	expression Expression
}

// NewCastImpl creates new generic cast
func NewCastImpl(expression Expression) Cast {
	castImpl := castImpl{
		expression: expression,
	}

	return &castImpl
}

func (b *castImpl) AS(castType string) Expression {
	castExp := &castExpression{
		expression: b.expression,
		cast:       string(castType),
	}

	castExp.expressionInterfaceImpl.Parent = castExp

	return castExp
}

type castExpression struct {
	expressionInterfaceImpl

	expression Expression
	cast       string
}

func (b *castExpression) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {

	expression := b.expression
	castType := b.cast

	if castOverride := out.Dialect.OperatorSerializeOverride("CAST"); castOverride != nil {
		castOverride(expression, String(castType))(statement, out, options...)
		return
	}

	out.WriteString("CAST(")
	expression.serialize(statement, out, options...)
	out.WriteString("AS")
	out.WriteString(castType + ")")
}
