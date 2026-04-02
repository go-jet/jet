package cubrid

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

// AS_INTEGER casts expression to INTEGER type
func (c *cast) AS_INTEGER() IntegerExpression {
	return IntExp(c.AS("INTEGER"))
}

// AS_BIGINT casts expression to BIGINT type
func (c *cast) AS_BIGINT() IntegerExpression {
	return IntExp(c.AS("BIGINT"))
}

// AS_SMALLINT casts expression to SMALLINT type
func (c *cast) AS_SMALLINT() IntegerExpression {
	return IntExp(c.AS("SMALLINT"))
}

// AS_CHAR casts expression to CHAR type with optional length
func (c *cast) AS_CHAR(length ...int) StringExpression {
	if len(length) > 0 {
		return StringExp(c.AS("CHAR(" + strconv.Itoa(length[0]) + ")"))
	}
	return StringExp(c.AS("CHAR"))
}

// AS_VARCHAR casts expression to VARCHAR type with optional length
func (c *cast) AS_VARCHAR(length ...int) StringExpression {
	if len(length) > 0 {
		return StringExp(c.AS("VARCHAR(" + strconv.Itoa(length[0]) + ")"))
	}
	return StringExp(c.AS("VARCHAR"))
}

// AS_DATE casts expression AS DATE type
func (c *cast) AS_DATE() DateExpression {
	return DateExp(c.AS("DATE"))
}

// AS_FLOAT casts expression AS FLOAT type
func (c *cast) AS_FLOAT() FloatExpression {
	return FloatExp(c.AS("FLOAT"))
}

// AS_DOUBLE casts expression AS DOUBLE type
func (c *cast) AS_DOUBLE() FloatExpression {
	return FloatExp(c.AS("DOUBLE"))
}

// AS_NUMERIC casts expression AS NUMERIC type
func (c *cast) AS_NUMERIC() FloatExpression {
	return FloatExp(c.AS("NUMERIC"))
}

// AS_TIME casts expression AS TIME type
func (c *cast) AS_TIME() TimeExpression {
	return TimeExp(c.AS("TIME"))
}

// AS_TIMESTAMP casts expression AS TIMESTAMP type
func (c *cast) AS_TIMESTAMP() TimestampExpression {
	return TimestampExp(c.AS("TIMESTAMP"))
}
