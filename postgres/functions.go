package postgres

import "github.com/go-jet/jet/v2/internal/jet"

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
var CBRT = jet.CBRT

// CEIL calculates ceil of float expression
var CEIL = jet.CEIL

// FLOOR calculates floor of float expression
var FLOOR = jet.FLOOR

// ROUND calculates round of a float expressions with optional precision
var ROUND = jet.ROUND

// SIGN returns sign of float expression
var SIGN = jet.SIGN

// TRUNC calculates trunc of float expression with optional precision
var TRUNC = jet.TRUNC

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

// BOOL_AND is aggregate function. Returns true if all input values are true, otherwise false
var BOOL_AND = jet.BOOL_AND

// BOOL_OR is aggregate function. Returns true if at least one input value is true, otherwise false
var BOOL_OR = jet.BOOL_OR

// COUNT is aggregate function. Returns number of input rows for which the value of expression is not null.
var COUNT = jet.COUNT

// EVERY is aggregate function. Returns true if all input values are true, otherwise false
var EVERY = jet.EVERY

// MAX is aggregate function. Returns maximum value of expression across all input values
var MAX = jet.MAX

// MAXf is aggregate function. Returns maximum value of float expression across all input values
var MAXf = jet.MAXf

// MAXi is aggregate function. Returns maximum value of int expression across all input values
var MAXi = jet.MAXi

// MIN is aggregate function. Returns minimum value of expression across all input values.
var MIN = jet.MIN

// MINf is aggregate function. Returns minimum value of float expression across all input values
var MINf = jet.MINf

// MINi is aggregate function. Returns minimum value of int expression across all input values
var MINi = jet.MINi

// SUM is aggregate function. Returns sum of all expressions
var SUM = jet.SUM

// SUMf is aggregate function. Returns sum of expression across all float expressions
var SUMf = jet.SUMf

// SUMi is aggregate function. Returns sum of expression across all integer expression.
var SUMi = jet.SUMi

// -------------------- Window functions -----------------------//

// ROW_NUMBER returns number of the current row within its partition, counting from 1
var ROW_NUMBER = jet.ROW_NUMBER

// RANK of the current row with gaps; same as row_number of its first peer
var RANK = jet.RANK

// DENSE_RANK returns rank of the current row without gaps; this function counts peer groups
var DENSE_RANK = jet.DENSE_RANK

// PERCENT_RANK calculates relative rank of the current row: (rank - 1) / (total partition rows - 1)
var PERCENT_RANK = jet.PERCENT_RANK

// CUME_DIST calculates cumulative distribution: (number of partition rows preceding or peer with current row) / total partition rows
var CUME_DIST = jet.CUME_DIST

// NTILE returns integer ranging from 1 to the argument value, dividing the partition as equally as possible
var NTILE = jet.NTILE

// LAG returns value evaluated at the row that is offset rows before the current row within the partition;
// if there is no such row, instead return default (which must be of the same type as value).
// Both offset and default are evaluated with respect to the current row.
// If omitted, offset defaults to 1 and default to null
var LAG = jet.LAG

// LEAD returns value evaluated at the row that is offset rows after the current row within the partition;
// if there is no such row, instead return default (which must be of the same type as value).
// Both offset and default are evaluated with respect to the current row.
// If omitted, offset defaults to 1 and default to null
var LEAD = jet.LEAD

// FIRST_VALUE returns value evaluated at the row that is the first row of the window frame
var FIRST_VALUE = jet.FIRST_VALUE

// LAST_VALUE returns value evaluated at the row that is the last row of the window frame
var LAST_VALUE = jet.LAST_VALUE

// NTH_VALUE returns value evaluated at the row that is the nth row of the window frame (counting from 1); null if no such row
var NTH_VALUE = jet.NTH_VALUE

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

// BTRIM removes the longest string consisting only of characters
// in characters (a space by default) from the start and end of string
var BTRIM = jet.BTRIM

// LTRIM removes the longest string containing only characters
// from characters (a space by default) from the start of string
var LTRIM = jet.LTRIM

// RTRIM removes the longest string containing only characters
// from characters (a space by default) from the end of string
var RTRIM = jet.RTRIM

// CHR returns character with the given code.
var CHR = jet.CHR

// CONCAT adds two or more expressions together
var CONCAT = func(expressions ...Expression) StringExpression {
	return jet.CONCAT(explicitLiteralCasts(expressions...)...)
}

// CONCAT_WS adds two or more expressions together with a separator.
func CONCAT_WS(separator Expression, expressions ...Expression) StringExpression {
	return jet.CONCAT_WS(explicitLiteralCast(separator), explicitLiteralCasts(expressions...)...)
}

// CONVERT converts string to dest_encoding. The original encoding is
// specified by src_encoding. The string must be valid in this encoding.
var CONVERT = jet.CONVERT

// CONVERT_FROM converts string to the database encoding. The original
// encoding is specified by src_encoding. The string must be valid in this encoding.
var CONVERT_FROM = jet.CONVERT_FROM

// CONVERT_TO converts string to dest_encoding.
var CONVERT_TO = jet.CONVERT_TO

// ENCODE encodes binary data into a textual representation.
// Supported formats are: base64, hex, escape. escape converts zero bytes and
// high-bit-set bytes to octal sequences (\nnn) and doubles backslashes.
var ENCODE = jet.ENCODE

// DECODE decodes binary data from textual representation in string.
// Options for format are same as in encode.
var DECODE = jet.DECODE

// FORMAT formats a number to a format like "#,###,###.##", rounded to a specified number of decimal places, then it returns the result as a string.
func FORMAT(formatStr StringExpression, formatArgs ...Expression) StringExpression {
	return jet.FORMAT(formatStr, explicitLiteralCasts(formatArgs...)...)
}

// INITCAP converts the first letter of each word to upper case
// and the rest to lower case. Words are sequences of alphanumeric
// characters separated by non-alphanumeric characters.
var INITCAP = jet.INITCAP

// LEFT returns first n characters in the string.
// When n is negative, return all but last |n| characters.
var LEFT = jet.LEFT

// RIGHT returns last n characters in the string.
// When n is negative, return all but first |n| characters.
var RIGHT = jet.RIGHT

// LENGTH returns number of characters in string with a given encoding
var LENGTH = jet.LENGTH

// LPAD fills up the string to length length by prepending the characters
// fill (a space by default). If the string is already longer than length
// then it is truncated (on the right).
var LPAD = jet.LPAD

// RPAD fills up the string to length length by appending the characters
// fill (a space by default). If the string is already longer than length then it is truncated.
var RPAD = jet.RPAD

// MD5 calculates the MD5 hash of string, returning the result in hexadecimal
var MD5 = jet.MD5

// REPEAT repeats string the specified number of times
var REPEAT = jet.REPEAT

// REPLACE replaces all occurrences in string of substring from with substring to
var REPLACE = jet.REPLACE

// REVERSE returns reversed string.
var REVERSE = jet.REVERSE

// STRPOS returns location of specified substring (same as position(substring in string),
// but note the reversed argument order)
var STRPOS = jet.STRPOS

// SUBSTR extracts substring
var SUBSTR = jet.SUBSTR

// TO_ASCII convert string to ASCII from another encoding
var TO_ASCII = jet.TO_ASCII

// TO_HEX converts number to its equivalent hexadecimal representation
var TO_HEX = jet.TO_HEX

//----------Data Type Formatting Functions ----------------------//

// TO_CHAR converts expression to string with format
var TO_CHAR = jet.TO_CHAR

// TO_DATE converts string to date using format
var TO_DATE = jet.TO_DATE

// TO_NUMBER converts string to numeric using format
var TO_NUMBER = jet.TO_NUMBER

// TO_TIMESTAMP converts string to time stamp with time zone using format
var TO_TIMESTAMP = jet.TO_TIMESTAMP

//----------------- Date/Time Functions and Operators ------------//

// CURRENT_DATE returns current date
var CURRENT_DATE = jet.CURRENT_DATE

// CURRENT_TIME returns current time with time zone
var CURRENT_TIME = jet.CURRENT_TIME

// CURRENT_TIMESTAMP returns current timestamp with time zone
var CURRENT_TIMESTAMP = jet.CURRENT_TIMESTAMP

// LOCALTIME returns local time of day using optional precision
var LOCALTIME = jet.LOCALTIME

// LOCALTIMESTAMP returns current date and time using optional precision
var LOCALTIMESTAMP = jet.LOCALTIMESTAMP

// NOW returns current date and time
var NOW = jet.NOW

// --------------- Conditional Expressions Functions -------------//

// COALESCE function returns the first of its arguments that is not null.
var COALESCE = jet.COALESCE

// NULLIF function returns a null value if value1 equals value2; otherwise it returns value1.
var NULLIF = jet.NULLIF

// GREATEST selects the largest  value from a list of expressions
var GREATEST = jet.GREATEST

// LEAST selects the smallest  value from a list of expressions
var LEAST = jet.LEAST

// EXISTS checks for existence of the rows in subQuery
var EXISTS = jet.EXISTS

// CASE create CASE operator with optional list of expressions
var CASE = jet.CASE

func explicitLiteralCasts(expressions ...Expression) []jet.Expression {
	ret := []jet.Expression{}

	for _, exp := range expressions {
		ret = append(ret, explicitLiteralCast(exp))
	}

	return ret
}

func explicitLiteralCast(expresion Expression) jet.Expression {
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
