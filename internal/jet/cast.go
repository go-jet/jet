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

	castExp.ExpressionInterfaceImpl.Parent = castExp

	return castExp
}

type castExpression struct {
	ExpressionInterfaceImpl

	expression Expression
	cast       string
}

func (b *castExpression) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {

	expression := b.expression
	castType := b.cast

	if castOverride := out.Dialect.OperatorSerializeOverride("CAST"); castOverride != nil {
		castOverride(expression, String(castType))(statement, out, FallTrough(options)...)
		return
	}

	out.WriteString("CAST(")
	expression.serialize(statement, out, FallTrough(options)...)
	out.WriteString("AS")
	out.WriteString(castType + ")")
}
