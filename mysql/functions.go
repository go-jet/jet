package mysql

import "github.com/go-jet/jet/internal/jet"

// ------------------ Mathematical functions ---------------//

var POW = jet.POW
var LN = jet.LN
var LOG = jet.LOG

var ABSf = jet.ABSf
var ABSi = jet.ABSi
var POWER = jet.POWER
var SQRT = jet.SQRT

func CBRT(number jet.NumericExpression) jet.FloatExpression {
	return POWER(number, Float(1.0).DIV(Float(3.0)))
}

var CEIL = jet.CEIL
var FLOOR = jet.FLOOR
var ROUND = jet.ROUND
var SIGN = jet.SIGN
var TRUNC = TRUNCATE

var TRUNCATE = func(floatExpression jet.FloatExpression, precision jet.IntegerExpression) jet.FloatExpression {
	return jet.NewFloatFunc("TRUNCATE", floatExpression, precision)
}

//var MINUSi = jet.MINUSi
//var MINUSf = jet.MINUSf
var BIT_NOT = jet.BIT_NOT

// ----------------- Aggregate functions  -------------------//

var BIT_AND = jet.BIT_AND
var BIT_OR = jet.BIT_OR
var BOOL_AND = jet.BOOL_AND
var BOOL_OR = jet.BOOL_OR
var EVERY = jet.EVERY
var MAXi = jet.MAXi
var MINi = jet.MINi
var SUMi = jet.SUMi

var SUMf = jet.SUMf
var AVG = jet.AVG
var MAXf = jet.MAXf
var MINf = jet.MINf
var COUNT = jet.COUNT

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
var CONCAT = jet.CONCAT
var CONCAT_WS = jet.CONCAT_WS
var CONVERT = jet.CONVERT
var CONVERT_FROM = jet.CONVERT_FROM
var CONVERT_TO = jet.CONVERT_TO
var ENCODE = jet.ENCODE
var DECODE = jet.DECODE
var FORMAT = jet.FORMAT
var INITCAP = jet.INITCAP
var LEFT = jet.LEFT
var RIGHT = jet.RIGHT

func LENGTH(str jet.StringExpression) jet.StringExpression {
	return jet.LENGTH(str)
}

func LPAD(str jet.StringExpression, length jet.IntegerExpression, text jet.StringExpression) jet.StringExpression {
	return jet.LPAD(str, length, text)
}
func RPAD(str jet.StringExpression, length jet.IntegerExpression, text jet.StringExpression) jet.StringExpression {
	return jet.RPAD(str, length, text)
}

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

func NOW(fsp ...int) DateTimeExpression {
	if len(fsp) > 0 {
		return jet.NewTimestampFunc("NOW", Int(int64(fsp[0]), true))
	}
	return jet.NewTimestampFunc("NOW")
}

func TIMESTAMP(str StringExpression) TimestampExpression {
	return jet.NewTimestampFunc("TIMESTAMP", str)
}

func UNIX_TIMESTAMP(str StringExpression) TimestampExpression {
	return jet.NewTimestampFunc("UNIX_TIMESTAMP", str)
}

// --------------- Conditional Expressions Functions -------------//
var COALESCE = jet.COALESCE
var NULLIF = jet.NULLIF
var GREATEST = jet.GREATEST
var LEAST = jet.LEAST
var EXISTS = jet.EXISTS
var CASE = jet.CASE
