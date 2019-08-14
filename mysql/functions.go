package mysql

import "github.com/go-jet/jet/internal/jet"

// ROW is construct one table row from list of expressions.
var ROW = jet.ROW

// ------------------ Mathematical functions ---------------//

// ABSf calculates absolute value from float expression
var ABSf = jet.ABSf

// ABSi calculates absolute value from int expression
var ABSi = jet.ABSi

// POW calculates power of base with exponent
var POW = jet.POW

// POWER calculates power of base with exponent
var POWER = jet.POWER

// SQRT calculates square root of numeric expression
var SQRT = jet.SQRT

// CBRT calculates cube root of numeric expression
func CBRT(number jet.NumericExpression) jet.FloatExpression {
	return POWER(number, Float(1.0).DIV(Float(3.0)))
}

// CEIL calculates ceil of float expression
var CEIL = jet.CEIL

// FLOOR calculates floor of float expression
var FLOOR = jet.FLOOR

// ROUND calculates round of a float expressions with optional precision
var ROUND = jet.ROUND

// SIGN returns sign of float expression
var SIGN = jet.SIGN

// TRUNC calculates trunc of float expression with precision
var TRUNC = TRUNCATE

// TRUNCATE calculates trunc of float expression with precision
var TRUNCATE = func(floatExpression jet.FloatExpression, precision jet.IntegerExpression) jet.FloatExpression {
	return jet.NewFloatFunc("TRUNCATE", floatExpression, precision)
}

// LN calculates natural algorithm of float expression
var LN = jet.LN

// LOG calculates logarithm of float expression
var LOG = jet.LOG

// ----------------- Aggregate functions  -------------------//

// AVG is aggregate function used to calculate avg value from numeric expression
var AVG = jet.AVG

// BIT_AND is aggregate function used to calculates the bitwise AND of all non-null input values, or null if none.
var BIT_AND = jet.BIT_AND

// BIT_OR is aggregate function used to calculates the bitwise OR of all non-null input values, or null if none.
var BIT_OR = jet.BIT_OR

// COUNT is aggregate function. Returns number of input rows for which the value of expression is not null.
var COUNT = jet.COUNT

// MAXi is aggregate function. Returns maximum value of int expression across all input values
var MAXi = jet.MAXi

// MAXf is aggregate function. Returns maximum value of float expression across all input values
var MAXf = jet.MAXf

// MINi is aggregate function. Returns minimum value of int expression across all input values
var MINi = jet.MINi

// MINf is aggregate function. Returns minimum value of float expression across all input values
var MINf = jet.MINf

// SUMi is aggregate function. Returns sum of expression across all integer expression.
var SUMi = jet.SUMi

// SUMi is aggregate function. Returns sum of expression across all integer expression.
var SUMf = jet.SUMf

//--------------------- String functions ------------------//

// BIT_LENGTH returns number of bits in string expression
var BIT_LENGTH = jet.BIT_LENGTH

// CHAR_LENGTH returns number of characters in string expression
var CHAR_LENGTH = jet.CHAR_LENGTH

// OCTET_LENGTH returns number of bytes in string expression
var OCTET_LENGTH = jet.OCTET_LENGTH

// LOWER returns string expression in lower case
var LOWER = jet.LOWER

// UPPER returns string expression in upper case
var UPPER = jet.UPPER

// LTRIM removes the longest string containing only characters
// from characters (a space by default) from the start of string
var LTRIM = jet.LTRIM

// RTRIM removes the longest string containing only characters
// from characters (a space by default) from the end of string
var RTRIM = jet.RTRIM

// CONCAT adds two or more expressions together
var CONCAT = jet.CONCAT

// CONCAT_WS adds two or more expressions together with a separator.
var CONCAT_WS = jet.CONCAT_WS

// FORMAT formats a number to a format like "#,###,###.##", rounded to a specified number of decimal places, then it returns the result as a string.
var FORMAT = jet.FORMAT

// LEFT returns first n characters in the string.
// When n is negative, return all but last |n| characters.
var LEFT = jet.LEFT

// RIGHT returns last n characters in the string.
// When n is negative, return all but first |n| characters.
var RIGHT = jet.RIGHT

// LENGTH returns number of characters in string with a given encoding
func LENGTH(str jet.StringExpression) jet.StringExpression {
	return jet.LENGTH(str)
}

// LPAD fills up the string to length length by prepending the characters
// fill (a space by default). If the string is already longer than length
// then it is truncated (on the right).
func LPAD(str jet.StringExpression, length jet.IntegerExpression, text jet.StringExpression) jet.StringExpression {
	return jet.LPAD(str, length, text)
}

// RPAD fills up the string to length length by appending the characters
// fill (a space by default). If the string is already longer than length then it is truncated.
func RPAD(str jet.StringExpression, length jet.IntegerExpression, text jet.StringExpression) jet.StringExpression {
	return jet.RPAD(str, length, text)
}

// MD5 calculates the MD5 hash of string, returning the result in hexadecimal
var MD5 = jet.MD5

// REPEAT repeats string the specified number of times
var REPEAT = jet.REPEAT

// REPLACE replaces all occurrences in string of substring from with substring to
var REPLACE = jet.REPLACE

// REVERSE returns reversed string.
var REVERSE = jet.REVERSE

// SUBSTR extracts substring
var SUBSTR = jet.SUBSTR

// REGEXP_LIKE Returns 1 if the string expr matches the regular expression specified by the pattern pat, 0 otherwise.
var REGEXP_LIKE = jet.REGEXP_LIKE

//----------------- Date/Time Functions and Operators ------------//
// CURRENT_DATE returns current date
var CURRENT_DATE = jet.CURRENT_DATE

// CURRENT_TIME returns current time with time zone
func CURRENT_TIME(precision ...int) TimeExpression {
	return TimeExp(jet.CURRENT_TIME(precision...))
}

// CURRENT_TIMESTAMP returns current timestamp with time zone
func CURRENT_TIMESTAMP(precision ...int) TimestampExpression {
	return TimestampExp(jet.CURRENT_TIMESTAMP(precision...))
}

// NOW returns current datetime
func NOW(fsp ...int) DateTimeExpression {
	if len(fsp) > 0 {
		return jet.NewTimestampFunc("NOW", Int(int64(fsp[0]), true))
	}
	return jet.NewTimestampFunc("NOW")
}

// TIMESTAMP return a datetime value based on the arguments:
func TIMESTAMP(str StringExpression) TimestampExpression {
	return jet.NewTimestampFunc("TIMESTAMP", str)
}

// UNIX_TIMESTAMP returns unix timestamp
func UNIX_TIMESTAMP(str StringExpression) TimestampExpression {
	return jet.NewTimestampFunc("UNIX_TIMESTAMP", str)
}

//----------- Comparison operators ---------------//

// EXISTS checks for existence of the rows in subQuery
var EXISTS = jet.EXISTS

// CASE create CASE operator with optional list of expressions
var CASE = jet.CASE

//----------------- Bit operators ---------------//

var BIT_NOT = jet.BIT_NOT
