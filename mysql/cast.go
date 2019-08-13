package mysql

import (
	"github.com/go-jet/jet/internal/jet"
	"strconv"
)

type cast interface {
	// Cast expressions as castType type
	AS(castType string) Expression
	// Cast expression as char with optional length
	AS_CHAR(lenght ...int) StringExpression
	// Cast expression AS date type
	AS_DATE() DateExpression
	// Cast expression AS numeric type, using precision and optionally scale
	AS_DECIMAL() FloatExpression
	// Cast expression AS time type
	AS_TIME() TimeExpression
	// Cast expression as datetime type
	AS_DATETIME() DateTimeExpression
	// Cast expressions as signed integer type
	AS_SIGNED() IntegerExpression
	// Cast expression as unsigned integer type
	AS_UNSIGNED() IntegerExpression
	// Cast expression as binary type
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

func (c *castImpl) AS(castType string) Expression {
	return c.Cast.AS(castType)
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

func (b *castImpl) AS_CHAR(lenght ...int) StringExpression {
	if len(lenght) > 0 {
		return StringExp(b.AS("CHAR(" + strconv.Itoa(lenght[0]) + ")"))
	}

	return StringExp(b.AS("CHAR"))
}

// Cast expression AS date type
func (b *castImpl) AS_DATE() DateExpression {
	return DateExp(b.AS("DATE"))
}

// Cast expression AS date type
func (b *castImpl) AS_DECIMAL() FloatExpression {
	return FloatExp(b.AS("DECIMAL"))
}

// Cast expression AS date type
func (b *castImpl) AS_TIME() TimeExpression {
	return TimeExp(b.AS("TIME"))
}

func (c *castImpl) AS_BINARY() StringExpression {
	return StringExp(c.AS("BINARY"))
}
