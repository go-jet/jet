package postgres

import (
	"fmt"
	"strconv"

	"github.com/go-jet/jet/v2/internal/jet"
)

type cast interface {
	AS(castType string) Expression
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
	// Cast expression AS char with optional length
	AS_CHAR(length ...int) StringExpression
	// Cast expression AS date type
	AS_DATE() DateExpression
	// Cast expression AS numeric type, using precision and optionally scale
	AS_DECIMAL() FloatExpression
	// Cast expression AS time type
	AS_TIME() TimeExpression
	// Cast expression AS text type
	AS_TEXT() StringExpression
	// Cast expression AS bytea type
	AS_BYTEA() StringExpression
	// Cast expression AS time with time timezone type
	AS_TIMEZ() TimezExpression
	// Cast expression AS timestamp type
	AS_TIMESTAMP() TimestampExpression
	// Cast expression AS timestamp with timezone type
	AS_TIMESTAMPZ() TimestampzExpression
	// Cast expression AS interval type
	AS_INTERVAL() IntervalExpression
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

// Cast expression as castType
func (b *castImpl) AS(castType string) Expression {
	return b.Cast.AS(castType)
}

// Cast expression as bool type
func (b *castImpl) AS_BOOL() BoolExpression {
	return BoolExp(b.AS("boolean"))
}

// Cast expression as smallint type
func (b *castImpl) AS_SMALLINT() IntegerExpression {
	return IntExp(b.AS("smallint"))
}

// Cast expression AS integer type
func (b *castImpl) AS_INTEGER() IntegerExpression {
	return IntExp(b.AS("integer"))
}

// Cast expression AS bigint type
func (b *castImpl) AS_BIGINT() IntegerExpression {
	return IntExp(b.AS("bigint"))
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

	return FloatExp(b.AS("numeric" + castArgs))
}

// Cast expression AS real type
func (b *castImpl) AS_REAL() FloatExpression {
	return FloatExp(b.AS("real"))
}

// Cast expression AS double precision type
func (b *castImpl) AS_DOUBLE() FloatExpression {
	return FloatExp(b.AS("double precision"))
}

// Cast expression AS text type
func (b *castImpl) AS_TEXT() StringExpression {
	return StringExp(b.AS("text"))
}

func (b *castImpl) AS_CHAR(length ...int) StringExpression {
	if len(length) > 0 {
		return StringExp(b.AS("char(" + strconv.Itoa(length[0]) + ")"))
	}

	return StringExp(b.AS("char"))
}

// Cast expression AS date type
func (b *castImpl) AS_DATE() DateExpression {
	return DateExp(b.AS("date"))
}

// Cast expression AS date type
func (b *castImpl) AS_DECIMAL() FloatExpression {
	return FloatExp(b.AS("decimal"))
}

// Cast expression AS text type
func (b *castImpl) AS_BYTEA() StringExpression {
	return StringExp(b.AS("bytea"))
}

// Cast expression AS date type
func (b *castImpl) AS_TIME() TimeExpression {
	return TimeExp(b.AS("time without time zone"))
}

// Cast expression AS time with time timezone type
func (b *castImpl) AS_TIMEZ() TimezExpression {
	return TimezExp(b.AS("time with time zone"))
}

// Cast expression AS timestamp type
func (b *castImpl) AS_TIMESTAMP() TimestampExpression {
	return TimestampExp(b.AS("timestamp without time zone"))
}

// Cast expression AS timestamp with timezone type
func (b *castImpl) AS_TIMESTAMPZ() TimestampzExpression {
	return TimestampzExp(b.AS("timestamp with time zone"))
}

// Cast expression AS interval type
func (b *castImpl) AS_INTERVAL() IntervalExpression {
	return IntervalExp(b.AS("interval"))
}
