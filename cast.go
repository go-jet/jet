package jet

import "fmt"

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
	Expression
	castType string
}

// CAST wraps expression for casting.
// For instance: CAST(table.column).AS_BOOL()
func CAST(expression Expression) cast {
	return &castImpl{
		Expression: expression,
	}
}

func (b *castImpl) accept(visitor visitor) {
	visitor.visit(b)

	b.Expression.accept(visitor)
}

func (b *castImpl) serialize(statement statementType, out *sqlBuilder, options ...serializeOption) error {

	if castOverride := out.dialect.CastOverride; castOverride != nil {
		return castOverride(b.Expression, b.castType)(statement, out, options...)
	}

	out.writeString("CAST(")
	err := b.Expression.serialize(statement, out, options...)
	if err != nil {
		return err
	}

	out.writeString("AS")
	out.writeString(b.castType + ")")

	return err
}

func (b *castImpl) AS_BOOL() BoolExpression {
	b.castType = "boolean"
	return BoolExp(b)
}

func (b *castImpl) AS_SMALLINT() IntegerExpression {
	b.castType = "smallint"
	return IntExp(b)
}

// Cast expression AS integer type
func (b *castImpl) AS_INTEGER() IntegerExpression {
	b.castType = "integer"
	return IntExp(b)
}

// Cast expression AS bigint type
func (b *castImpl) AS_BIGINT() IntegerExpression {
	b.castType = "bigint"
	return IntExp(b)
}

// Cast expression AS numeric type, using precision and optionally scale
func (b *castImpl) AS_NUMERIC(precision int, scale ...int) FloatExpression {

	if len(scale) > 0 {
		b.castType = fmt.Sprintf("numeric(%d, %d)", precision, scale[0])
	} else {
		b.castType = fmt.Sprintf("numeric(%d)", precision)
	}

	return FloatExp(b)
}

func (b *castImpl) AS_DECIMAL() FloatExpression {
	b.castType = "decimal"
	return FloatExp(b)
}

// Cast expression AS real type
func (b *castImpl) AS_REAL() FloatExpression {
	b.castType = "real"
	return FloatExp(b)
}

// Cast expression AS double precision type
func (b *castImpl) AS_DOUBLE() FloatExpression {
	b.castType = "double precision"
	return FloatExp(b)
}

// Cast expression AS text type
func (b *castImpl) AS_TEXT() StringExpression {
	b.castType = "text"
	return StringExp(b)
}

// Cast expression AS date type
func (b *castImpl) AS_DATE() DateExpression {
	b.castType = "date"
	return DateExp(b)
}

// Cast expression AS time type
func (b *castImpl) AS_TIME() TimeExpression {
	b.castType = "time without time zone"
	return TimeExp(b)
}

// Cast expression AS time with time timezone type
func (b *castImpl) AS_TIMEZ() TimezExpression {
	b.castType = "time with time zone"
	return TimezExp(b)
}

// Cast expression AS timestamp type
func (b *castImpl) AS_TIMESTAMP() TimestampExpression {
	b.castType = "timestamp without time zone"
	return TimestampExp(b)
}

// Cast expression AS timestamp with timezone type
func (b *castImpl) AS_TIMESTAMPZ() TimestampzExpression {
	b.castType = "timestamp with time zone"
	return TimestampzExp(b)
}
