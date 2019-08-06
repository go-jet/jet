package mysql

import (
	"github.com/go-jet/jet/internal/jet"
)

type cast interface {
	jet.Cast

	AS_DATETIME() DateTimeExpression
	AS_SIGNED() IntegerExpression
	AS_UNSIGNED() IntegerExpression
	AS_BINARY() StringExpression
}

type castImpl struct {
	jet.CastImpl
}

func CAST(expr jet.Expression) cast {
	castImpl := &castImpl{}

	castImpl.CastImpl = jet.NewCastImpl(expr)

	return castImpl
}

func (c *castImpl) AS_DATETIME() DateTimeExpression {
	return DateTimeExp(c.AS("DATETIME"))
}

func (c *castImpl) AS_SIGNED() IntegerExpression {
	return IntExp(c.AS("SIGNED"))
}

func (c *castImpl) AS_UNSIGNED() IntegerExpression {
	return IntExp(c.AS("UNSIGNED"))
}

func (c *castImpl) AS_BINARY() StringExpression {
	return StringExp(c.AS("BINARY"))
}
