package mysql

import (
	"github.com/go-jet/jet/v2/internal/jet"
	"strconv"
)

type cast interface {
	// Cast expressions as castType type
	AS(castType string) Expression
	// Cast expression as char with optional length
	AS_CHAR(length ...int) StringExpression
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

// CAST function converts a expr (of any type) into latter specified datatype.
func CAST(expr Expression) cast {
	castImpl := &castImpl{}

	castImpl.Cast = jet.NewCastImpl(expr)

	return castImpl
}

// AS casts expressions to castType
func (c *castImpl) AS(castType string) Expression {
	return c.Cast.AS(castType)
}

// AS_DATETIME cast expression to DATETIME type
func (c *castImpl) AS_DATETIME() DateTimeExpression {
	return DateTimeExp(c.AS("DATETIME"))
}

// AS_SIGNED casts expression to SIGNED type
func (c *castImpl) AS_SIGNED() IntegerExpression {
	return IntExp(c.AS("SIGNED"))
}

// AS_UNSIGNED casts expression to UNSIGNED type
func (c *castImpl) AS_UNSIGNED() IntegerExpression {
	return IntExp(c.AS("UNSIGNED"))
}

// AS_CHAR casts expression to CHAR type with optional length
func (c *castImpl) AS_CHAR(length ...int) StringExpression {
	if len(length) > 0 {
		return StringExp(c.AS("CHAR(" + strconv.Itoa(length[0]) + ")"))
	}

	return StringExp(c.AS("CHAR"))
}

// AS_DATE casts expression AS DATE type
func (c *castImpl) AS_DATE() DateExpression {
	return DateExp(c.AS("DATE"))
}

// AS_DECIMAL casts expression AS DECIMAL type
func (c *castImpl) AS_DECIMAL() FloatExpression {
	return FloatExp(c.AS("DECIMAL"))
}

// AS_TIME casts expression AS TIME type
func (c *castImpl) AS_TIME() TimeExpression {
	return TimeExp(c.AS("TIME"))
}

// AS_BINARY casts expression as BINARY type
func (c *castImpl) AS_BINARY() StringExpression {
	return StringExp(c.AS("BINARY"))
}
