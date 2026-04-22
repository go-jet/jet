package cubrid

import "github.com/go-jet/jet/v2/internal/jet"

var (
	AND = jet.AND
	OR  = jet.OR
)

// ROW function creates a tuple value.
func ROW(expressions ...Expression) RowExpression { return jet.ROW(Dialect, expressions...) }

// Mathematical functions
var (
	ABSf  = jet.ABSf
	ABSi  = jet.ABSi
	POW   = jet.POW
	POWER = jet.POWER
	SQRT  = jet.SQRT
	CEIL  = jet.CEIL
	FLOOR = jet.FLOOR
	ROUND = jet.ROUND
	SIGN  = jet.SIGN
	LN    = jet.LN
	LOG   = jet.LOG
	MOD   = jet.Mod
)

// TRUNC calculates trunc of float expression
var TRUNC = func(floatExpression jet.FloatExpression, precision jet.IntegerExpression) jet.FloatExpression {
	return jet.NewFloatFunc("TRUNC", floatExpression, precision)
}

// Aggregate functions
var (
	AVG     = jet.AVG
	BIT_AND = jet.BIT_AND
	BIT_OR  = jet.BIT_OR
	COUNT   = jet.COUNT
	MAX     = jet.MAX
	MAXi    = jet.MAXi
	MAXf    = jet.MAXf
	MIN     = jet.MIN
	MINi    = jet.MINi
	MINf    = jet.MINf
	SUM     = jet.SUM
	SUMi    = jet.SUMi
	SUMf    = jet.SUMf
)

// Window functions
var (
	ROW_NUMBER   = jet.ROW_NUMBER
	RANK         = jet.RANK
	DENSE_RANK   = jet.DENSE_RANK
	PERCENT_RANK = jet.PERCENT_RANK
	CUME_DIST    = jet.CUME_DIST
	NTILE        = jet.NTILE
	LAG          = jet.LAG
	LEAD         = jet.LEAD
	FIRST_VALUE  = jet.FIRST_VALUE
	LAST_VALUE   = jet.LAST_VALUE
	NTH_VALUE    = jet.NTH_VALUE
)

// String functions
var (
	BIT_LENGTH   = jet.BIT_LENGTH
	CHAR_LENGTH  = jet.CHAR_LENGTH
	OCTET_LENGTH = jet.OCTET_LENGTH
	LOWER        = jet.LOWER
	UPPER        = jet.UPPER
	LTRIM        = jet.LTRIM
	RTRIM        = jet.RTRIM
	CONCAT       = jet.CONCAT
	CONCAT_WS    = jet.CONCAT_WS
	LEFT         = jet.LEFT
	RIGHT        = jet.RIGHT
	MD5          = jet.MD5
	REPEAT       = jet.REPEAT
	REPLACE      = jet.REPLACE
	REVERSE      = jet.REVERSE
	SUBSTR       = jet.SUBSTR
	REGEXP_LIKE  = jet.REGEXP_LIKE
)

// LENGTH returns number of characters in string
func LENGTH(str jet.StringOrBlobExpression) jet.IntegerExpression { return jet.LENGTH(str) }

// LPAD fills up the string to length by prepending fill characters
func LPAD(str jet.StringExpression, length jet.IntegerExpression, text jet.StringExpression) jet.StringExpression {
	return jet.LPAD(str, length, text)
}

// RPAD fills up the string to length by appending fill characters
func RPAD(str jet.StringExpression, length jet.IntegerExpression, text jet.StringExpression) jet.StringExpression {
	return jet.RPAD(str, length, text)
}

// Date/time functions
var CURRENT_DATE = jet.CURRENT_DATE

// CURRENT_TIME returns current time
func CURRENT_TIME(precision ...int) TimeExpression {
	return TimeExp(jet.CURRENT_TIME(precision...))
}

// CURRENT_TIMESTAMP returns current timestamp
func CURRENT_TIMESTAMP(precision ...int) TimestampExpression {
	return TimestampExp(jet.CURRENT_TIMESTAMP(precision...))
}

// NOW returns current datetime
func NOW() DateTimeExpression { return jet.NewTimestampFunc("NOW") }

// unitType is CUBRID-specific unit type for EXTRACT function
type unitType string

const (
	YEAR        unitType = "YEAR"
	MONTH       unitType = "MONTH"
	DAY         unitType = "DAY"
	HOUR        unitType = "HOUR"
	MINUTE      unitType = "MINUTE"
	SECOND      unitType = "SECOND"
	MILLISECOND unitType = "MILLISECOND"
)

// EXTRACT function retrieves subfields from date/time values
func EXTRACT(field unitType, from Expression) IntegerExpression {
	return IntExp(jet.EXTRACT(string(field), from))
}

// Conditional expressions
var (
	EXISTS   = jet.EXISTS
	CASE     = jet.CASE
	COALESCE = jet.COALESCE
	NULLIF   = jet.NULLIF
	GREATEST = jet.GREATEST
	LEAST    = jet.LEAST
)

// Group By operators
var (
	WITH_ROLLUP = jet.WITH_ROLLUP
	GROUPING    = jet.GROUPING
)
