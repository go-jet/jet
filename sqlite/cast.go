package sqlite

import (
	"github.com/go-jet/jet/v2/internal/jet"
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

// AS_TEXT cast expression to TEXT type
func (c *cast) AS_TEXT() StringExpression {
	return StringExp(c.AS("TEXT"))
}

// AS_NUMERIC cast expression to NUMERIC type
func (c *cast) AS_NUMERIC() FloatExpression {
	return FloatExp(c.AS("NUMERIC"))
}

// AS_INTEGER cast expression to INTEGER type
func (c *cast) AS_INTEGER() IntegerExpression {
	return IntExp(c.AS("INTEGER"))
}

// AS_REAL cast expression to REAL type
func (c *cast) AS_REAL() FloatExpression {
	return FloatExp(c.AS("REAL"))
}

// AS_BLOB cast expression to BLOB type
func (c *cast) AS_BLOB() StringExpression {
	return StringExp(c.AS("BLOB"))
}
