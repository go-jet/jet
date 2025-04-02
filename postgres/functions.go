package postgres

import (
	"github.com/go-jet/jet/v2/internal/jet"
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
	return jet.ROW(Dialect, expressions...)
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

// Character encodings for CONVERT, CONVERT_FROM and CONVERT_TO functions
var (
	UTF8       = StringExp(jet.FixedLiteral("UTF8"))
	LATIN1     = StringExp(jet.FixedLiteral("LATIN1"))
	LATIN2     = StringExp(jet.FixedLiteral("LATIN2"))
	LATIN3     = StringExp(jet.FixedLiteral("LATIN3"))
	LATIN4     = StringExp(jet.FixedLiteral("LATIN4"))
	WIN1252    = StringExp(jet.FixedLiteral("WIN1252"))
	ISO_8859_5 = StringExp(jet.FixedLiteral("ISO_8859_5"))
	ISO_8859_6 = StringExp(jet.FixedLiteral("ISO_8859_6"))
	ISO_8859_7 = StringExp(jet.FixedLiteral("ISO_8859_7"))
	ISO_8859_8 = StringExp(jet.FixedLiteral("ISO_8859_8"))
	KOI8R      = StringExp(jet.FixedLiteral("KOI8R"))
	KOI8U      = StringExp(jet.FixedLiteral("KOI8U"))
)

// CONVERT converts string to dest_encoding. The original encoding is
// specified by src_encoding. The string must be valid in this encoding.
func CONVERT(str ByteaExpression, srcEncoding StringExpression, destEncoding StringExpression) ByteaExpression {
	return jet.CONVERT(str, srcEncoding, destEncoding)
}

// CONVERT_FROM converts string to the database encoding. The original
// encoding is specified by src_encoding. The string must be valid in this encoding.
var CONVERT_FROM = jet.CONVERT_FROM

// CONVERT_TO converts string to dest_encoding.
var CONVERT_TO = jet.CONVERT_TO

// ENCODE/DECODE textual formats
var (
	Base64 = StringExp(jet.FixedLiteral("base64"))
	Escape = StringExp(jet.FixedLiteral("escape"))
	Hex    = StringExp(jet.FixedLiteral("hex"))
)

// ENCODE encodes binary data into a textual representation.
// Supported formats are: base64, hex, escape. escape converts zero bytes and
// high-bit-set bytes to octal sequences (\nnn) and doubles backslashes.
var ENCODE = jet.ENCODE

// DECODE decodes binary data from textual representation in string.
// Options for format are same as in encode.
var DECODE = jet.DECODE

// FORMAT formats the arguments according to a format string. This function is similar to the C function sprintf.
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

// BIT_COUNT returns the number of bits set in the binary string (also known as “popcount”).
var BIT_COUNT = jet.BIT_COUNT

// GET_BIT extracts n'th bit from binary string.
func GET_BIT(bytes ByteaExpression, n IntegerExpression) IntegerExpression {
	return IntExp(Func("GET_BIT", bytes, n))
}

// GET_BYTE extracts n'th byte from binary string.
func GET_BYTE(bytes ByteaExpression, n IntegerExpression) IntegerExpression {
	return IntExp(Func("GET_BYTE", bytes, n))
}

// SET_BIT sets n'th bit in binary string to newvalue.
func SET_BIT(bytes ByteaExpression, n IntegerExpression, newValue IntegerExpression) ByteaExpression {
	return ByteaExp(Func("SET_BIT", bytes, n, newValue))
}

// SET_BYTE sets n'th byte in binary string to newvalue.
func SET_BYTE(bytes ByteaExpression, n IntegerExpression, newValue IntegerExpression) ByteaExpression {
	return ByteaExp(Func("SET_BYTE", bytes, n, newValue))
}

// SHA224 computes the SHA-224 hash of the binary string.
func SHA224(bytes ByteaExpression) ByteaExpression {
	return ByteaExp(Func("SHA224", bytes))
}

// SHA256 computes the SHA-256 hash of the binary string.
func SHA256(bytes ByteaExpression) ByteaExpression {
	return ByteaExp(Func("SHA256", bytes))
}

// SHA384 computes the SHA-384 hash of the binary string.
func SHA384(bytes ByteaExpression) ByteaExpression {
	return ByteaExp(Func("SHA384", bytes))
}

// SHA512 computes the SHA-512 hash of the binary string.
func SHA512(bytes ByteaExpression) ByteaExpression {
	return ByteaExp(Func("SHA512", bytes))
}

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

// LOWER_BOUND returns range expressions lower bound
func LOWER_BOUND[T Expression](expression jet.Range[T]) T {
	return jet.LOWER_BOUND[T](expression)
}

// UPPER_BOUND returns range expressions upper bound
func UPPER_BOUND[T Expression](expression jet.Range[T]) T {
	return jet.UPPER_BOUND[T](expression)
}

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

// Additional time unit types for EXTRACT function
const (
	DOW unit = MILLENNIUM + 1 + iota
	DOY
	EPOCH
	ISODOW
	ISOYEAR
	JULIAN
	QUARTER
	TIMEZONE
	TIMEZONE_HOUR
	TIMEZONE_MINUTE
)

// EXTRACT function retrieves subfields such as year or hour from date/time values
//
//	EXTRACT(DAY, User.CreatedAt)
func EXTRACT(field unit, from Expression) FloatExpression {
	return FloatExp(jet.EXTRACT(unitToString(field), from))
}

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

// DATE_TRUNC returns the truncated date and time using optional time zone.
// Use TimestampzExp if you need timestamp with time zone and IntervalExp if you need interval.
func DATE_TRUNC(field unit, source Expression, timezone ...string) TimestampExpression {
	if len(timezone) > 0 {
		return jet.NewTimestampFunc("DATE_TRUNC", jet.FixedLiteral(unitToString(field)), source, jet.FixedLiteral(timezone[0]))
	}

	return jet.NewTimestampFunc("DATE_TRUNC", jet.FixedLiteral(unitToString(field)), source)
}

// GENERATE_SERIES generates a series of values from start to stop, with a step size of step.
func GENERATE_SERIES(start Expression, stop Expression, step ...Expression) Expression {
	if len(step) > 0 {
		return jet.NewFunc("GENERATE_SERIES", []Expression{start, stop, step[0]}, nil)
	}

	return jet.NewFunc("GENERATE_SERIES", []Expression{start, stop}, nil)
}

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

// MODE computes the most frequent value of the aggregated argument
var MODE = jet.MODE

// PERCENTILE_CONT computes a value corresponding to the specified fraction within the ordered set of
// aggregated argument values. This will interpolate between adjacent input items if needed.
func PERCENTILE_CONT(fraction FloatExpression) *jet.OrderSetAggregateFunc {
	return jet.PERCENTILE_CONT(castFloatLiteral(fraction))
}

// PERCENTILE_DISC computes  the first value within the ordered set of aggregated argument values whose position
// in the ordering equals or exceeds the specified fraction. The aggregated argument must be of a sortable type.
func PERCENTILE_DISC(fraction FloatExpression) *jet.OrderSetAggregateFunc {
	return jet.PERCENTILE_DISC(castFloatLiteral(fraction))
}

func castFloatLiteral(fraction FloatExpression) FloatExpression {
	if _, ok := fraction.(jet.LiteralExpression); ok {
		return CAST(fraction).AS_DOUBLE() // to make postgres aware of the type
	}
	return fraction
}

// ----------------- Group By operators --------------------------//

// GROUPING_SETS operator allows grouping of the rows in a table by multiple sets of columns(or expressions) in a single query.
// This can be useful when we want to analyze data by different combinations of columns, without having to write separate
// queries for each combination. GROUPING_SETS sets of columns are constructed with WRAP method.
//
//	GROUPING_SETS(
//		WRAP(Inventory.FilmID, Inventory.StoreID),
//		WRAP(),
//	),
var GROUPING_SETS = jet.GROUPING_SETS

// WRAP surrounds a list of expressions or columns with parentheses, producing new row: (expression1, expression2, ...)
// The construct (a, b) is normally recognized in expressions as a row constructor. WRAP and ROW methods behave exactly the same,
// except when used in GROUPING_SETS and VALUES. In these contexts, WRAP must be used instead of ROW.
func WRAP(expressions ...Expression) RowExpression {
	return jet.WRAP(Dialect, expressions...)
}

// ROLLUP operator is used with the GROUP BY clause to generate all prefixes of a group of columns including the empty list.
// It creates extra rows in the result set that represent the subtotal values for each combination of columns.
var ROLLUP = jet.ROLLUP

// CUBE operator is used with the GROUP BY clause to generate subtotals for all possible combinations of a group of columns.
// It creates extra rows in the result set that represent the subtotal values for each combination of columns.
var CUBE = jet.CUBE

// GROUPING function is used to identify which columns are included in a grouping set or a subtotal row. It takes as input
// the name of a column and returns 1 if the column is not included in the current grouping set, and 0 otherwise.
// It can be also used with multiple parameters to check if a set of columns is included in the current grouping set. The result
// of the GROUPING function would then be an integer bit mask having 1’s for the arguments which have GROUPING(argument) as 1.
var GROUPING = jet.GROUPING

// range constructor functions
var (
	// DATE_RANGE constructor function to create a date range
	DATE_RANGE = jet.DateRange
	// NUM_RANGE constructor function to create a numeric range
	NUM_RANGE = jet.NumRange
	// TS_RANGE constructor function to create a timestamp range
	TS_RANGE = jet.TsRange
	// TSTZ_RANGE constructor function to create a timestampz range
	TSTZ_RANGE = jet.TstzRange
	// INT4_RANGE constructor function to create a int4 range
	INT4_RANGE = jet.Int4Range
	// INT8_RANGE constructor function to create a int8 range
	INT8_RANGE = jet.Int8Range
)
