package postgres

import (
	"fmt"
	"github.com/go-jet/jet/internal/jet"
)

type cast interface {
	jet.Cast
	// Cast expression AS bool type
	AS_BOOL() BoolExpression

	// Cast expression AS smallint type
	AS_SMALLINT() IntegerExpression
	// Cast expression AS integer type
	AS_INTEGER() IntegerExpression
	// Cast expression AS bigint type
	AS_BIGINT() IntegerExpression
	// Cast expression AS numeric type, using precision and optionally scale
	AS_NUMERIC(precisionAndScale ...int) FloatExpression

	// Cast expression AS real type
	AS_REAL() FloatExpression
	// Cast expression AS double precision type
	AS_DOUBLE() FloatExpression
	// Cast expression AS text type
	AS_TEXT() StringExpression

	AS_BYTEA() StringExpression

	// Cast expression AS time with time timezone type
	AS_TIMEZ() TimezExpression
	// Cast expression AS timestamp type
	AS_TIMESTAMP() TimestampExpression
	// Cast expression AS timestamp with timezone type
	AS_TIMESTAMPZ() TimestampzExpression
}

type castImpl struct {
	jet.Cast
}

func CAST(expr Expression) cast {
	castImpl := &castImpl{}

	castImpl.Cast = jet.NewCastImpl(expr)

	return castImpl
}

func (b *castImpl) AS_BOOL() BoolExpression {
	return jet.BoolExp(b.AS("boolean"))
}

func (b *castImpl) AS_SMALLINT() IntegerExpression {
	return jet.IntExp(b.AS("smallint"))
}

// Cast expression AS integer type
func (b *castImpl) AS_INTEGER() IntegerExpression {
	return jet.IntExp(b.AS("integer"))
}

// Cast expression AS bigint type
func (b *castImpl) AS_BIGINT() IntegerExpression {
	return jet.IntExp(b.AS("bigint"))
}

// Cast expression AS numeric type, using precision and optionally scale
func (b *castImpl) AS_NUMERIC(precisionAndScale ...int) FloatExpression {
	var castArgs string

	var argLen = len(precisionAndScale)
	if argLen >= 2 {
		castArgs = fmt.Sprintf("(%d, %d)", precisionAndScale[0], precisionAndScale[1])
	} else if argLen == 1 {
		castArgs = fmt.Sprintf("(%d)", precisionAndScale[0])
	}

	return jet.FloatExp(b.AS("numeric" + castArgs))
}

// Cast expression AS real type
func (b *castImpl) AS_REAL() FloatExpression {
	return jet.FloatExp(b.AS("real"))
}

// Cast expression AS double precision type
func (b *castImpl) AS_DOUBLE() FloatExpression {
	return jet.FloatExp(b.AS("double precision"))
}

// Cast expression AS text type
func (b *castImpl) AS_TEXT() StringExpression {
	return jet.StringExp(b.AS("text"))
}

// Cast expression AS text type
func (b *castImpl) AS_BYTEA() StringExpression {
	return jet.StringExp(b.AS("bytea"))
}

// Cast expression AS date type
func (b *castImpl) AS_TIME() jet.TimeExpression {
	return TimeExp(b.AS("time without time zone"))
}

// Cast expression AS time with time timezone type
func (b *castImpl) AS_TIMEZ() TimezExpression {
	return jet.TimezExp(b.AS("time with time zone"))
}

// Cast expression AS timestamp type
func (b *castImpl) AS_TIMESTAMP() TimestampExpression {
	return jet.TimestampExp(b.AS("timestamp without time zone"))
}

// Cast expression AS timestamp with timezone type
func (b *castImpl) AS_TIMESTAMPZ() TimestampzExpression {
	return jet.TimestampzExp(b.AS("timestamp with time zone"))
}
