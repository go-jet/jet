package postgres

import (
	"fmt"
	"strconv"

	"github.com/go-jet/jet/v2/internal/jet"
)

type cast struct {
	jet.Cast
}

// CAST function converts an expr (of any type) into later specified datatype.
func CAST(expr Expression) *cast {
	ret := &cast{}
	ret.Cast = jet.NewCastImpl(expr)

	return ret
}

// AS casts expression as castType
func (b *cast) AS(castType string) Expression {
	return b.Cast.AS(castType)
}

// AS_BOOL casts expression as bool type
func (b *cast) AS_BOOL() BoolExpression {
	return BoolExp(b.AS("boolean"))
}

// AS_SMALLINT casts expression as smallint type
func (b *cast) AS_SMALLINT() IntegerExpression {
	return IntExp(b.AS("smallint"))
}

// AS_INTEGER casts expression AS integer type
func (b *cast) AS_INTEGER() IntegerExpression {
	return IntExp(b.AS("integer"))
}

// AS_BIGINT casts expression AS bigint type
func (b *cast) AS_BIGINT() IntegerExpression {
	return IntExp(b.AS("bigint"))
}

// AS_NUMERIC casts expression as numeric type, using precision and optionally scale
func (b *cast) AS_NUMERIC(precisionAndScale ...int) FloatExpression {
	var castArgs string

	var argLen = len(precisionAndScale)
	if argLen >= 2 {
		castArgs = fmt.Sprintf("(%d, %d)", precisionAndScale[0], precisionAndScale[1])
	} else if argLen == 1 {
		castArgs = fmt.Sprintf("(%d)", precisionAndScale[0])
	}

	return FloatExp(b.AS("numeric" + castArgs))
}

// AS_REAL casts expression AS real type
func (b *cast) AS_REAL() FloatExpression {
	return FloatExp(b.AS("real"))
}

// AS_DOUBLE casts expression AS double precision type
func (b *cast) AS_DOUBLE() FloatExpression {
	return FloatExp(b.AS("double precision"))
}

// AS_TEXT casts expression AS text type
func (b *cast) AS_TEXT() StringExpression {
	return StringExp(b.AS("text"))
}

// AS_CHAR casts expression AS a character type
func (b *cast) AS_CHAR(length ...int) StringExpression {
	if len(length) > 0 {
		return StringExp(b.AS("char(" + strconv.Itoa(length[0]) + ")"))
	}

	return StringExp(b.AS("char"))
}

// AS_VARCHAR casts expression AS a character varying type
func (b *cast) AS_VARCHAR(length ...int) StringExpression {
	if len(length) > 0 {
		return StringExp(b.AS("varchar(" + strconv.Itoa(length[0]) + ")"))
	}

	return StringExp(b.AS("varchar"))
}

// AS_DATE casts expression AS date type
func (b *cast) AS_DATE() DateExpression {
	return DateExp(b.AS("date"))
}

// AS_DECIMAL casts expression AS date type
func (b *cast) AS_DECIMAL() FloatExpression {
	return FloatExp(b.AS("decimal"))
}

// AS_BYTEA casts expression AS text type
func (b *cast) AS_BYTEA() StringExpression {
	return StringExp(b.AS("bytea"))
}

// AS_TIME casts expression AS date type
func (b *cast) AS_TIME() TimeExpression {
	return TimeExp(b.AS("time without time zone"))
}

// AS_TIMEZ casts expression AS time with time timezone type
func (b *cast) AS_TIMEZ() TimezExpression {
	return TimezExp(b.AS("time with time zone"))
}

// AS_TIMESTAMP casts expression AS timestamp type
func (b *cast) AS_TIMESTAMP() TimestampExpression {
	return TimestampExp(b.AS("timestamp without time zone"))
}

// AS_TIMESTAMPZ casts expression AS timestamp with timezone type
func (b *cast) AS_TIMESTAMPZ() TimestampzExpression {
	return TimestampzExp(b.AS("timestamp with time zone"))
}

// AS_INTERVAL casts expression AS interval type
func (b *cast) AS_INTERVAL() IntervalExpression {
	return IntervalExp(b.AS("interval"))
}
