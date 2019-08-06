package postgres

import "github.com/go-jet/jet/internal/jet"

var ROW = jet.ROW

// ------------------ Mathematical functions ---------------//

var ABSf = jet.ABSf
var ABSi = jet.ABSi
var POW = jet.POW
var POWER = jet.POWER
var SQRT = jet.SQRT
var CBRT = jet.CBRT
var CEIL = jet.CEIL
var FLOOR = jet.FLOOR
var ROUND = jet.ROUND
var SIGN = jet.SIGN
var TRUNC = jet.TRUNC
var LN = jet.LN
var LOG = jet.LOG

// ----------------- Aggregate functions  -------------------//

var AVG = jet.AVG
var BIT_AND = jet.BIT_AND
var BIT_OR = jet.BIT_OR
var BOOL_AND = jet.BOOL_AND
var BOOL_OR = jet.BOOL_OR
var COUNT = jet.COUNT
var EVERY = jet.EVERY
var MAXf = jet.MAXf
var MAXi = jet.MAXi
var MINf = jet.MINf
var MINi = jet.MINi
var SUMf = jet.SUMf
var SUMi = jet.SUMi

//--------------------- String functions ------------------//

var REGEXP_LIKE = jet.REGEXP_LIKE
var BIT_LENGTH = jet.BIT_LENGTH
var CHAR_LENGTH = jet.CHAR_LENGTH
var OCTET_LENGTH = jet.OCTET_LENGTH
var LOWER = jet.LOWER
var UPPER = jet.UPPER
var BTRIM = jet.BTRIM
var LTRIM = jet.LTRIM
var RTRIM = jet.RTRIM
var CHR = jet.CHR

var CONCAT = func(expressions ...Expression) StringExpression {
	return jet.CONCAT(explicitCasts(expressions...)...)
}

func CONCAT_WS(expressions ...Expression) StringExpression {
	return jet.CONCAT_WS(explicitCasts(expressions...)...)
}

var CONVERT = jet.CONVERT
var CONVERT_FROM = jet.CONVERT_FROM
var CONVERT_TO = jet.CONVERT_TO
var ENCODE = jet.ENCODE
var DECODE = jet.DECODE

func FORMAT(formatStr StringExpression, formatArgs ...Expression) StringExpression {
	return jet.FORMAT(formatStr, explicitCasts(formatArgs...)...)
}

var INITCAP = jet.INITCAP
var LEFT = jet.LEFT
var RIGHT = jet.RIGHT
var LENGTH = jet.LENGTH
var LPAD = jet.LPAD
var RPAD = jet.RPAD
var MD5 = jet.MD5
var REPEAT = jet.REPEAT
var REPLACE = jet.REPLACE
var REVERSE = jet.REVERSE
var STRPOS = jet.STRPOS
var SUBSTR = jet.SUBSTR
var TO_ASCII = jet.TO_ASCII
var TO_HEX = jet.TO_HEX

//----------Data Type Formatting Functions ----------------------//

var TO_CHAR = jet.TO_CHAR
var TO_DATE = jet.TO_DATE
var TO_NUMBER = jet.TO_NUMBER
var TO_TIMESTAMP = jet.TO_TIMESTAMP

//----------------- Date/Time Functions and Operators ------------//

var CURRENT_DATE = jet.CURRENT_DATE
var CURRENT_TIME = jet.CURRENT_TIME
var CURRENT_TIMESTAMP = jet.CURRENT_TIMESTAMP
var LOCALTIME = jet.LOCALTIME
var LOCALTIMESTAMP = jet.LOCALTIMESTAMP
var NOW = jet.NOW

// --------------- Conditional Expressions Functions -------------//

var COALESCE = jet.COALESCE
var NULLIF = jet.NULLIF
var GREATEST = jet.GREATEST
var LEAST = jet.LEAST
var EXISTS = jet.EXISTS
var CASE = jet.CASE

func explicitCasts(expressions ...Expression) []jet.Expression {
	ret := []jet.Expression{}

	for _, exp := range expressions {
		ret = append(ret, explicitCast(exp))
	}

	return ret
}

func explicitCast(expresion Expression) jet.Expression {
	if _, ok := expresion.(jet.LiteralExpression); !ok {
		return expresion
	}

	switch expresion.(type) {
	case jet.BoolExpression:
		return CAST(expresion).AS_BOOL()
	case jet.IntegerExpression:
		return CAST(expresion).AS_INTEGER()
	case jet.FloatExpression:
		return CAST(expresion).AS_NUMERIC()
	case jet.StringExpression:
		return CAST(expresion).AS_TEXT()
	}

	return expresion
}
