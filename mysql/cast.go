package mysql

import (
	"github.com/go-jet/jet/v2/internal/jet"
	"strconv"
)

type cast interface {
	// AS casts expressions as castType type
	AS(castType string) Expression
	// AS_CHAR casts expression as char with optional length
	AS_CHAR(length ...int) StringExpression
	// AS_DATE casts expression AS date type
	AS_DATE() DateExpression
	// AS_FLOAT casts expressions as float type
	AS_FLOAT() FloatExpression
	// AS_DOUBLE casts expressions as double type
	AS_DOUBLE() FloatExpression
	// AS_DECIMAL casts expression AS numeric type
	AS_DECIMAL() FloatExpression
	// AS_TIME casts expression AS time type
	AS_TIME() TimeExpression
	// AS_DATETIME casts expression as datetime type
	AS_DATETIME() DateTimeExpression
	// AS_SIGNED casts expressions as signed integer type
	AS_SIGNED() IntegerExpression
	// AS_UNSIGNED casts expression as unsigned integer type
	AS_UNSIGNED() IntegerExpression
	// AS_BINARY casts expression as binary type
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

func (c *castImpl) AS_FLOAT() FloatExpression {
	return FloatExp(c.AS("FLOAT"))
}

func (c *castImpl) AS_DOUBLE() FloatExpression {
	return FloatExp(c.AS("DOUBLE"))
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
