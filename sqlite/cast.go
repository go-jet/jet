package sqlite

import (
	"github.com/go-jet/jet/v2/internal/jet"
)

type cast interface {
	AS(castType string) Expression
	AS_TEXT() StringExpression
	AS_NUMERIC() FloatExpression
	AS_INTEGER() IntegerExpression
	AS_REAL() FloatExpression
	AS_BLOB() StringExpression
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

// AS_TEXT cast expression to TEXT type
func (c *castImpl) AS_TEXT() StringExpression {
	return StringExp(c.AS("TEXT"))
}

// AS_NUMERIC cast expression to NUMERIC type
func (c *castImpl) AS_NUMERIC() FloatExpression {
	return FloatExp(c.AS("NUMERIC"))
}

// AS_INTEGER cast expression to INTEGER type
func (c *castImpl) AS_INTEGER() IntegerExpression {
	return IntExp(c.AS("INTEGER"))
}

// AS_REAL cast expression to REAL type
func (c *castImpl) AS_REAL() FloatExpression {
	return FloatExp(c.AS("REAL"))
}

// AS_BLOB cast expression to BLOB type
func (c *castImpl) AS_BLOB() StringExpression {
	return StringExp(c.AS("BLOB"))
}
