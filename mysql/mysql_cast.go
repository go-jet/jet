package mysql

import (
	"github.com/go-jet/jet"
)

type cast interface {
	AS_DATE() DateExpression
	AS_TIME() TimeExpression
	AS_DATETIME() DateTimeExpression
	AS_CHAR() StringExpression
	AS_SIGNED() IntegerExpression
	AS_UNSIGNED() IntegerExpression
	AS_BINARY() StringExpression
}

type castImpl struct {
	jet.Cast
}

func CAST(expr jet.Expression) cast {
	castImpl := &castImpl{}

	castImpl.Cast = jet.NewCastImpl(expr)

	return castImpl
}

func (c *castImpl) AS_DATE() DateExpression {
	return jet.DateExp(c.As("DATE"))
}

func (c *castImpl) AS_DATETIME() DateTimeExpression {
	return jet.TimestampExp(c.As("DATETIME"))
}

func (c *castImpl) AS_TIME() TimeExpression {
	return jet.TimeExp(c.As("TIME"))
}

func (c *castImpl) AS_CHAR() StringExpression {
	return jet.StringExp(c.As("CHAR"))
}

func (c *castImpl) AS_SIGNED() IntegerExpression {
	return jet.IntExp(c.As("SIGNED"))
}

func (c *castImpl) AS_UNSIGNED() IntegerExpression {
	return jet.IntExp(c.As("UNSIGNED"))
}

func (c *castImpl) AS_BINARY() StringExpression {
	return jet.StringExp(c.As("BINARY"))
}
