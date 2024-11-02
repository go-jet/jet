package mysql

import (
	"github.com/go-jet/jet/v2/internal/jet"
	"strconv"
)

type cast struct {
	jet.Cast
}

// CAST function converts a expr (of any type) into latter specified datatype.
func CAST(expr Expression) *cast {
	ret := &cast{}
	ret.Cast = jet.NewCastImpl(expr)

	return ret
}

// AS casts expressions to castType
func (c *cast) AS(castType string) Expression {
	return c.Cast.AS(castType)
}

// AS_DATETIME cast expression to DATETIME type
func (c *cast) AS_DATETIME() DateTimeExpression {
	return DateTimeExp(c.AS("DATETIME"))
}

// AS_SIGNED casts expression to SIGNED type
func (c *cast) AS_SIGNED() IntegerExpression {
	return IntExp(c.AS("SIGNED"))
}

// AS_UNSIGNED casts expression to UNSIGNED type
func (c *cast) AS_UNSIGNED() IntegerExpression {
	return IntExp(c.AS("UNSIGNED"))
}

// AS_CHAR casts expression to CHAR type with optional length
func (c *cast) AS_CHAR(length ...int) StringExpression {
	if len(length) > 0 {
		return StringExp(c.AS("CHAR(" + strconv.Itoa(length[0]) + ")"))
	}

	return StringExp(c.AS("CHAR"))
}

// AS_DATE casts expression AS DATE type
func (c *cast) AS_DATE() DateExpression {
	return DateExp(c.AS("DATE"))
}

func (c *cast) AS_FLOAT() FloatExpression {
	return FloatExp(c.AS("FLOAT"))
}

func (c *cast) AS_DOUBLE() FloatExpression {
	return FloatExp(c.AS("DOUBLE"))
}

// AS_DECIMAL casts expression AS DECIMAL type
func (c *cast) AS_DECIMAL() FloatExpression {
	return FloatExp(c.AS("DECIMAL"))
}

// AS_TIME casts expression AS TIME type
func (c *cast) AS_TIME() TimeExpression {
	return TimeExp(c.AS("TIME"))
}

// AS_BINARY casts expression as BINARY type
func (c *cast) AS_BINARY() StringExpression {
	return StringExp(c.AS("BINARY"))
}
