package sqlite

import (
	"fmt"
	"github.com/go-jet/jet/v2/internal/jet"
	"time"
)

// This functions can be used, instead of its method counterparts, to have a better indentation of a complex condition
// in the Go code and in the generated SQL.
var (
	// AND function adds AND operator between expressions.
	AND = jet.AND
	// OR function adds OR operator between expressions.
	OR = jet.OR
)

// ROW function is used to create a tuple value that consists of a set of expressions or column values.
func ROW(expressions ...Expression) RowExpression {
	return jet.WRAP(Dialect, expressions...)
}

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
//var BIT_AND = jet.BIT_AND

// BIT_OR is aggregate function used to calculates the bitwise OR of all non-null input values, or null if none.
//var BIT_OR = jet.BIT_OR

// COUNT is aggregate function. Returns number of input rows for which the value of expression is not null.
var COUNT = jet.COUNT

// MAX is aggregate function. Returns maximum value of expression across all input values
var MAX = jet.MAX

// MAXi is aggregate function. Returns maximum value of int expression across all input values
var MAXi = jet.MAXi

// MAXf is aggregate function. Returns maximum value of float expression across all input values
var MAXf = jet.MAXf

// MIN is aggregate function. Returns minimum value of int expression across all input values
var MIN = jet.MIN

// MINi is aggregate function. Returns minimum value of int expression across all input values
var MINi = jet.MINi

// MINf is aggregate function. Returns minimum value of float expression across all input values
var MINf = jet.MINf

// SUM is aggregate function. Returns sum of all expressions
var SUM = jet.SUM

// SUMi is aggregate function. Returns sum of integer expression.
var SUMi = jet.SUMi

// SUMf is aggregate function. Returns sum of float expression.
var SUMf = jet.SUMf

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
//var BIT_LENGTH = jet.BIT_LENGTH
//
//// CHAR_LENGTH returns number of characters in string expression
//var CHAR_LENGTH = jet.CHAR_LENGTH
//
//// OCTET_LENGTH returns number of bytes in string expression
//var OCTET_LENGTH = jet.OCTET_LENGTH

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
//var CONCAT = jet.CONCAT

// CONCAT_WS adds two or more expressions together with a separator.
//var CONCAT_WS = jet.CONCAT_WS

// FORMAT formats a number to a format like "#,###,###.##", rounded to a specified number of decimal places, then it returns the result as a string.
//var FORMAT = jet.FORMAT

// LEFTSTR returns first n characters in the string.
// When n is negative, return all but last |n| characters.
//func LEFTSTR(str StringExpression, n IntegerExpression) StringExpression {
//	return jet.NewStringFunc("LEFTSTR", str, n)
//}
//
//// RIGHT returns last n characters in the string.
//// When n is negative, return all but first |n| characters.
//func RIGHTSTR(str StringExpression, n IntegerExpression) StringExpression {
//	return jet.NewStringFunc("RIGHTSTR", str, n)
//}

// LENGTH returns number of characters in string with a given encoding
func LENGTH(str jet.StringExpression) jet.StringExpression {
	return jet.LENGTH(str)
}

// LPAD fills up the string to length length by prepending the characters
// fill (a space by default). If the string is already longer than length
// then it is truncated (on the right).
//func LPAD(str jet.StringExpression, length jet.IntegerExpression, text jet.StringExpression) jet.StringExpression {
//	return jet.LPAD(str, length, text)
//}

// RPAD fills up the string to length length by appending the characters
// fill (a space by default). If the string is already longer than length then it is truncated.
//func RPAD(str jet.StringExpression, length jet.IntegerExpression, text jet.StringExpression) jet.StringExpression {
//	return jet.RPAD(str, length, text)
//}

// MD5 calculates the MD5 hash of string, returning the result in hexadecimal
//var MD5 = jet.MD5

// REPEAT repeats string the specified number of times
//var REPEAT = jet.REPEAT

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
func CURRENT_TIME() TimeExpression {
	return TimeExp(jet.CURRENT_TIME())
}

// CURRENT_TIMESTAMP returns current timestamp with time zone
func CURRENT_TIMESTAMP() TimestampExpression {
	return TimestampExp(jet.CURRENT_TIMESTAMP())
}

//// NOW returns current datetime
//func NOW() DateTimeExpression {
//	//if len(fsp) > 0 {
//	//	return jet.NewTimestampFunc("NOW", jet.FixedLiteral(int64(fsp[0])))
//	//}
//	//return jet.NewTimestampFunc("NOW")
//	return DATETIME(jet.FixedLiteral("now"))
//}

// time-value modifiers
var (
	YEARS   = modifier("YEARS")
	MONTHS  = modifier("MONTHS")
	DAYS    = modifier("DAYS")
	HOURS   = modifier("HOURS")
	MINUTES = modifier("MINUTES")
	SECONDS = modifier("SECONDS")

	START_OF_YEAR  = String("start of year")
	START_OF_MONTH = String("start of month")
	UNIXEPOCH      = String("unixepoch")
	LOCALTIME      = String("localtime")
	UTC            = String("UTC")

	WEEKDAY = func(value int) Expression {
		return String(fmt.Sprintf("WEEKDAY %d", value))
	}
)

func modifier(modifierName string) func(value float64) Expression {
	return func(value float64) Expression {
		return String(fmt.Sprintf("%g %s", value, modifierName))
	}
}

// DATE function creates new date from time-value and zero or more time modifiers
func DATE(timeValue interface{}, modifiers ...Expression) DateExpression {
	exprList := getFuncExprList(timeValue, modifiers...)

	return jet.NewDateFunc("DATE", exprList...)
}

// TIME function creates new time from time-value and zero or more time modifiers
func TIME(timeValue interface{}, modifiers ...Expression) TimeExpression {
	exprList := getFuncExprList(timeValue, modifiers...)

	return jet.NewTimeFunc("TIME", exprList...)
}

// DATETIME function creates new DateTime from time-value and zero or more time modifiers
func DATETIME(timeValue interface{}, modifiers ...Expression) DateTimeExpression {
	exprList := getFuncExprList(timeValue, modifiers...)

	return jet.NewTimestampFunc("DATETIME", exprList...)
}

// JULIANDAY returns the number of days since noon in Greenwich on November 24, 4714 B.C
func JULIANDAY(timeValue interface{}, modifiers ...Expression) FloatExpression {
	exprList := getFuncExprList(timeValue, modifiers...)
	return jet.NewFloatFunc("JULIANDAY", exprList...)
}

// STRFTIME routine returns the date formatted according to the format string specified as the first argument.
func STRFTIME(format StringExpression, timeValue interface{}, modifiers ...Expression) StringExpression {
	exprList := append([]Expression{format}, getFuncExprList(timeValue, modifiers...)...)
	return jet.NewStringFunc("strftime", exprList...)
}

func getFuncExprList(timeValue interface{}, modifiers ...Expression) []Expression {
	return append([]Expression{getTimeValueExpression(timeValue)}, modifiers...)
}

func getTimeValueExpression(timeValue interface{}) Expression {
	switch t := timeValue.(type) {
	case string:
		return String(t)
	case Expression:
		return t
	case time.Time, int64:
		return jet.Literal(t)
	}

	panic(fmt.Sprintf("jet: Invalid time value %T(%q)", timeValue, timeValue))
}

// TIMESTAMP return a datetime value based on the arguments:
func TIMESTAMP(str StringExpression) TimestampExpression {
	return jet.NewTimestampFunc("TIMESTAMP", str)
}

// UNIX_TIMESTAMP returns unix timestamp
func UNIX_TIMESTAMP(str StringExpression) TimestampExpression {
	return jet.NewTimestampFunc("UNIX_TIMESTAMP", str)
}

// --------------- Conditional Expressions Functions -------------//

// COALESCE function returns the first of its arguments that is not null.
var COALESCE = jet.COALESCE

// NULLIF function returns a null value if value1 equals value2; otherwise it returns value1.
var NULLIF = jet.NULLIF

// EXISTS checks for existence of the rows in subQuery
var EXISTS = jet.EXISTS

// CASE create CASE operator with optional list of expressions
var CASE = jet.CASE
