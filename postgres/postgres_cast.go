package postgres

import (
	"fmt"
	"github.com/go-jet/jet"
)

type cast interface {
	// Cast expression AS bool type
	AS_BOOL() BoolExpression
	// Cast expression AS smallint type
	AS_SMALLINT() IntegerExpression
	// Cast expression AS integer type
	AS_INTEGER() IntegerExpression
	// Cast expression AS bigint type
	AS_BIGINT() IntegerExpression
	// Cast expression AS numeric type, using precision and optionally scale
	AS_NUMERIC(precision int, scale ...int) FloatExpression
	// Cast expression AS numeric type, using precision and optionally scale
	AS_DECIMAL() FloatExpression
	// Cast expression AS real type
	AS_REAL() FloatExpression
	// Cast expression AS double precision type
	AS_DOUBLE() FloatExpression
	// Cast expression AS text type
	AS_TEXT() StringExpression
	// Cast expression AS date type
	AS_DATE() DateExpression
	// Cast expression AS time type
	AS_TIME() TimeExpression
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

func CAST(expr jet.Expression) cast {
	castImpl := &castImpl{}

	castImpl.Cast = jet.NewCastImpl(expr)

	return castImpl
}

func (b *castImpl) AS_BOOL() BoolExpression {
	return jet.BoolExp(b.As("boolean"))
}

func (b *castImpl) AS_SMALLINT() IntegerExpression {
	return jet.IntExp(b.As("smallint"))
}

// Cast expression AS integer type
func (b *castImpl) AS_INTEGER() IntegerExpression {
	return jet.IntExp(b.As("integer"))
}

// Cast expression AS bigint type
func (b *castImpl) AS_BIGINT() IntegerExpression {
	return jet.IntExp(b.As("bigint"))
}

// Cast expression AS numeric type, using precision and optionally scale
func (b *castImpl) AS_NUMERIC(precision int, scale ...int) FloatExpression {
	var castType string

	if len(scale) > 0 {
		castType = fmt.Sprintf("numeric(%d, %d)", precision, scale[0])
	} else {
		castType = fmt.Sprintf("numeric(%d)", precision)
	}

	return jet.FloatExp(b.As(jet.CastType(castType)))
}

func (b *castImpl) AS_DECIMAL() FloatExpression {
	return jet.FloatExp(b.As("decimal"))
}

// Cast expression AS real type
func (b *castImpl) AS_REAL() FloatExpression {
	return jet.FloatExp(b.As("real"))
}

// Cast expression AS double precision type
func (b *castImpl) AS_DOUBLE() FloatExpression {
	return jet.FloatExp(b.As("double precision"))
}

// Cast expression AS text type
func (b *castImpl) AS_TEXT() StringExpression {
	return jet.StringExp(b.As("text"))
}

// Cast expression AS date type
func (b *castImpl) AS_DATE() DateExpression {
	return jet.DateExp(b.As("date"))
}

// Cast expression AS time type
func (b *castImpl) AS_TIME() TimeExpression {
	return jet.TimeExp(b.As("time without time zone"))
}

// Cast expression AS time with time timezone type
func (b *castImpl) AS_TIMEZ() TimezExpression {
	return jet.TimezExp(b.As("time with time zone"))
}

// Cast expression AS timestamp type
func (b *castImpl) AS_TIMESTAMP() TimestampExpression {
	return jet.TimestampExp(b.As("timestamp without time zone"))
}

// Cast expression AS timestamp with timezone type
func (b *castImpl) AS_TIMESTAMPZ() TimestampzExpression {
	return jet.TimestampzExp(b.As("timestamp with time zone"))
}
