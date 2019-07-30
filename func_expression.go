package jet

import "errors"

// ROW is construct one table row from list of expressions.
func ROW(expressions ...Expression) Expression {
	return newFunc("ROW", expressions, nil)
}

// ------------------ Mathematical functions ---------------//

// ABSf calculates absolute value from float expression
func ABSf(floatExpression FloatExpression) FloatExpression {
	return NewFloatFunc("ABS", floatExpression)
}

// ABSi calculates absolute value from int expression
func ABSi(integerExpression IntegerExpression) IntegerExpression {
	return newIntegerFunc("ABS", integerExpression)
}

func POWER(base, exponent NumericExpression) FloatExpression {
	return NewFloatFunc("POWER", base, exponent)
}

// SQRT calculates square root of numeric expression
func SQRT(numericExpression NumericExpression) FloatExpression {
	return NewFloatFunc("SQRT", numericExpression)
}

// CBRT calculates cube root of numeric expression
func CBRT(numericExpression NumericExpression) FloatExpression {
	return NewFloatFunc("CBRT", numericExpression)
}

// CEIL calculates ceil of float expression
func CEIL(floatExpression FloatExpression) FloatExpression {
	return NewFloatFunc("CEIL", floatExpression)
}

// FLOOR calculates floor of float expression
func FLOOR(floatExpression FloatExpression) FloatExpression {
	return NewFloatFunc("FLOOR", floatExpression)
}

// ROUND calculates round of a float expressions with optional precision
func ROUND(floatExpression FloatExpression, precision ...IntegerExpression) FloatExpression {
	if len(precision) > 0 {
		return NewFloatFunc("ROUND", floatExpression, precision[0])
	}
	return NewFloatFunc("ROUND", floatExpression)
}

// SIGN returns sign of float expression
func SIGN(floatExpression FloatExpression) FloatExpression {
	return NewFloatFunc("SIGN", floatExpression)
}

// TRUNC calculates trunc of float expression with optional precision
func TRUNC(floatExpression FloatExpression, precision ...IntegerExpression) FloatExpression {
	if len(precision) > 0 {
		return NewFloatFunc("TRUNC", floatExpression, precision[0])
	}
	return NewFloatFunc("TRUNC", floatExpression)
}

// LN calculates natural algorithm of float expression
func LN(floatExpression FloatExpression) FloatExpression {
	return NewFloatFunc("LN", floatExpression)
}

// LOG calculates logarithm of float expression
func LOG(floatExpression FloatExpression) FloatExpression {
	return NewFloatFunc("LOG", floatExpression)
}

// ----------------- Aggregate functions  -------------------//

// AVG is aggregate function used to calculate avg value from numeric expression
func AVG(numericExpression NumericExpression) FloatExpression {
	return NewFloatFunc("AVG", numericExpression)
}

// BIT_AND is aggregate function used to calculates the bitwise AND of all non-null input values, or null if none.
func BIT_AND(integerExpression IntegerExpression) IntegerExpression {
	return newIntegerFunc("BIT_AND", integerExpression)
}

// BIT_OR is aggregate function used to calculates the bitwise OR of all non-null input values, or null if none.
func BIT_OR(integerExpression IntegerExpression) IntegerExpression {
	return newIntegerFunc("BIT_OR", integerExpression)
}

// BOOL_AND is aggregate function. Returns true if all input values are true, otherwise false
func BOOL_AND(boolExpression BoolExpression) BoolExpression {
	return newBoolFunc("BOOL_AND", boolExpression)
}

// BOOL_OR is aggregate function. Returns true if at least one input value is true, otherwise false
func BOOL_OR(boolExpression BoolExpression) BoolExpression {
	return newBoolFunc("BOOL_OR", boolExpression)
}

// COUNT is aggregate function. Returns number of input rows for which the value of expression is not null.
func COUNT(expression Expression) IntegerExpression {
	return newIntegerFunc("COUNT", expression)
}

// EVERY is aggregate function. Returns true if all input values are true, otherwise false
func EVERY(boolExpression BoolExpression) BoolExpression {
	return newBoolFunc("EVERY", boolExpression)
}

// MAXf is aggregate function. Returns maximum value of float expression across all input values
func MAXf(floatExpression FloatExpression) FloatExpression {
	return NewFloatFunc("MAX", floatExpression)
}

// MAXi is aggregate function. Returns maximum value of int expression across all input values
func MAXi(integerExpression IntegerExpression) IntegerExpression {
	return newIntegerFunc("MAX", integerExpression)
}

// MINf is aggregate function. Returns minimum value of float expression across all input values
func MINf(floatExpression FloatExpression) FloatExpression {
	return NewFloatFunc("MIN", floatExpression)
}

// MINi is aggregate function. Returns minimum value of int expression across all input values
func MINi(integerExpression IntegerExpression) IntegerExpression {
	return newIntegerFunc("MIN", integerExpression)
}

// SUMf is aggregate function. Returns sum of expression across all float expressions
func SUMf(floatExpression FloatExpression) FloatExpression {
	return NewFloatFunc("SUM", floatExpression)
}

// SUMi is aggregate function. Returns sum of expression across all integer expression.
func SUMi(integerExpression IntegerExpression) IntegerExpression {
	return newIntegerFunc("SUM", integerExpression)
}

//------------ String functions ------------------//

// BIT_LENGTH returns number of bits in string expression
func BIT_LENGTH(stringExpression StringExpression) IntegerExpression {
	return newIntegerFunc("BIT_LENGTH", stringExpression)
}

// CHAR_LENGTH returns number of characters in string expression
func CHAR_LENGTH(stringExpression StringExpression) IntegerExpression {
	return newIntegerFunc("CHAR_LENGTH", stringExpression)
}

// OCTET_LENGTH returns number of bytes in string expression
func OCTET_LENGTH(stringExpression StringExpression) IntegerExpression {
	return newIntegerFunc("OCTET_LENGTH", stringExpression)
}

// LOWER returns string expression in lower case
func LOWER(stringExpression StringExpression) StringExpression {
	return newStringFunc("LOWER", stringExpression)
}

// UPPER returns string expression in upper case
func UPPER(stringExpression StringExpression) StringExpression {
	return newStringFunc("UPPER", stringExpression)
}

// BTRIM removes the longest string consisting only of characters
// in characters (a space by default) from the start and end of string
func BTRIM(stringExpression StringExpression, trimChars ...StringExpression) StringExpression {
	if len(trimChars) > 0 {
		return newStringFunc("BTRIM", stringExpression, trimChars[0])
	}
	return newStringFunc("BTRIM", stringExpression)
}

// LTRIM removes the longest string containing only characters
// from characters (a space by default) from the start of string
func LTRIM(str StringExpression, trimChars ...StringExpression) StringExpression {
	if len(trimChars) > 0 {
		return newStringFunc("LTRIM", str, trimChars[0])
	}
	return newStringFunc("LTRIM", str)
}

// RTRIM removes the longest string containing only characters
// from characters (a space by default) from the end of string
func RTRIM(str StringExpression, trimChars ...StringExpression) StringExpression {
	if len(trimChars) > 0 {
		return newStringFunc("RTRIM", str, trimChars[0])
	}
	return newStringFunc("RTRIM", str)
}

// CHR returns character with the given code.
func CHR(integerExpression IntegerExpression) StringExpression {
	return newStringFunc("CHR", integerExpression)
}

//
//func CONCAT(expressions ...Expression) StringExpression {
//	return newStringFunc("CONCAT", expressions...)
//}
//
//func CONCAT_WS(expressions ...Expression) StringExpression {
//	return newStringFunc("CONCAT_WS", expressions...)
//}

// CONVERT converts string to dest_encoding. The original encoding is
// specified by src_encoding. The string must be valid in this encoding.
func CONVERT(str StringExpression, srcEncoding StringExpression, destEncoding StringExpression) StringExpression {
	return newStringFunc("CONVERT", str, srcEncoding, destEncoding)
}

// CONVERT_FROM converts string to the database encoding. The original
// encoding is specified by src_encoding. The string must be valid in this encoding.
func CONVERT_FROM(str StringExpression, srcEncoding StringExpression) StringExpression {
	return newStringFunc("CONVERT_FROM", str, srcEncoding)
}

// CONVERT_TO converts string to dest_encoding.
func CONVERT_TO(str StringExpression, toEncoding StringExpression) StringExpression {
	return newStringFunc("CONVERT_TO", str, toEncoding)
}

// ENCODE encodes binary data into a textual representation.
// Supported formats are: base64, hex, escape. escape converts zero bytes and
// high-bit-set bytes to octal sequences (\nnn) and doubles backslashes.
func ENCODE(data StringExpression, format StringExpression) StringExpression {
	return newStringFunc("ENCODE", data, format)
}

// DECODE decodes binary data from textual representation in string.
// Options for format are same as in encode.
func DECODE(data StringExpression, format StringExpression) StringExpression {
	return newStringFunc("DECODE", data, format)
}

//func FORMAT(formatStr StringExpression, formatArgs ...expressions) StringExpression {
//	args := []expressions{formatStr}
//	args = append(args, formatArgs...)
//	return newStringFunc("FORMAT", args...)
//}

// INITCAP converts the first letter of each word to upper case
// and the rest to lower case. Words are sequences of alphanumeric
// characters separated by non-alphanumeric characters.
func INITCAP(str StringExpression) StringExpression {
	return newStringFunc("INITCAP", str)
}

// LEFT returns first n characters in the string.
// When n is negative, return all but last |n| characters.
func LEFT(str StringExpression, n IntegerExpression) StringExpression {
	return newStringFunc("LEFT", str, n)
}

// RIGHT returns last n characters in the string.
// When n is negative, return all but first |n| characters.
func RIGHT(str StringExpression, n IntegerExpression) StringExpression {
	return newStringFunc("RIGHT", str, n)
}

// LENGTH returns number of characters in string with a given encoding
func LENGTH(str StringExpression, encoding ...StringExpression) StringExpression {
	if len(encoding) > 0 {
		return newStringFunc("LENGTH", str, encoding[0])
	}
	return newStringFunc("LENGTH", str)
}

// LPAD fills up the string to length length by prepending the characters
// fill (a space by default). If the string is already longer than length
// then it is truncated (on the right).
func LPAD(str StringExpression, length IntegerExpression, text ...StringExpression) StringExpression {
	if len(text) > 0 {
		return newStringFunc("LPAD", str, length, text[0])
	}

	return newStringFunc("LPAD", str, length)
}

// RPAD fills up the string to length length by appending the characters
// fill (a space by default). If the string is already longer than length then it is truncated.
func RPAD(str StringExpression, length IntegerExpression, text ...StringExpression) StringExpression {
	if len(text) > 0 {
		return newStringFunc("RPAD", str, length, text[0])
	}

	return newStringFunc("RPAD", str, length)
}

// MD5 calculates the MD5 hash of string, returning the result in hexadecimal
func MD5(stringExpression StringExpression) StringExpression {
	return newStringFunc("MD5", stringExpression)
}

// REPEAT repeats string the specified number of times
func REPEAT(str StringExpression, n IntegerExpression) StringExpression {
	return newStringFunc("REPEAT", str, n)
}

// REPLACE replaces all occurrences in string of substring from with substring to
func REPLACE(text, from, to StringExpression) StringExpression {
	return newStringFunc("REPLACE", text, from, to)
}

// REVERSE returns reversed string.
func REVERSE(stringExpression StringExpression) StringExpression {
	return newStringFunc("REVERSE", stringExpression)
}

// STRPOS returns location of specified substring (same as position(substring in string),
// but note the reversed argument order)
func STRPOS(str, substring StringExpression) IntegerExpression {
	return newIntegerFunc("STRPOS", str, substring)
}

// SUBSTR extracts substring
func SUBSTR(str StringExpression, from IntegerExpression, count ...IntegerExpression) StringExpression {
	if len(count) > 0 {
		return newStringFunc("SUBSTR", str, from, count[0])
	}
	return newStringFunc("SUBSTR", str, from)
}

// TO_ASCII convert string to ASCII from another encoding
func TO_ASCII(str StringExpression, encoding ...StringExpression) StringExpression {
	if len(encoding) > 0 {
		return newStringFunc("TO_ASCII", str, encoding[0])
	}
	return newStringFunc("TO_ASCII", str)
}

// TO_HEX converts number to its equivalent hexadecimal representation
func TO_HEX(number IntegerExpression) StringExpression {
	return newStringFunc("TO_HEX", number)
}

//----------Data Type Formatting Functions ----------------------//

// TO_CHAR converts expression to string with format
func TO_CHAR(expression Expression, format StringExpression) StringExpression {
	return newStringFunc("TO_CHAR", expression, format)
}

// TO_DATE converts string to date using format
func TO_DATE(dateStr, format StringExpression) DateExpression {
	return newDateFunc("TO_DATE", dateStr, format)
}

// TO_NUMBER converts string to numeric using format
func TO_NUMBER(floatStr, format StringExpression) FloatExpression {
	return NewFloatFunc("TO_NUMBER", floatStr, format)
}

// TO_TIMESTAMP converts string to time stamp with time zone using format
func TO_TIMESTAMP(timestampzStr, format StringExpression) TimestampzExpression {
	return newTimestampzFunc("TO_TIMESTAMP", timestampzStr, format)
}

//----------------- Date/Time Functions and Operators ---------------//

// CURRENT_DATE returns current date
func CURRENT_DATE() DateExpression {
	dateFunc := newDateFunc("CURRENT_DATE")
	dateFunc.noBrackets = true
	return dateFunc
}

// CURRENT_TIME returns current time with time zone
func CURRENT_TIME(precision ...int) TimezExpression {
	var timezFunc *timezFunc

	if len(precision) > 0 {
		timezFunc = newTimezFunc("CURRENT_TIME", constLiteral(precision[0]))
	} else {
		timezFunc = newTimezFunc("CURRENT_TIME")
	}

	timezFunc.noBrackets = true

	return timezFunc
}

// CURRENT_TIMESTAMP returns current timestamp with time zone
func CURRENT_TIMESTAMP(precision ...int) TimestampzExpression {
	var timestampzFunc *timestampzFunc

	if len(precision) > 0 {
		timestampzFunc = newTimestampzFunc("CURRENT_TIMESTAMP", constLiteral(precision[0]))
	} else {
		timestampzFunc = newTimestampzFunc("CURRENT_TIMESTAMP")
	}

	timestampzFunc.noBrackets = true

	return timestampzFunc
}

// LOCALTIME returns local time of day using optional precision
func LOCALTIME(precision ...int) TimeExpression {
	var timeFunc *timeFunc

	if len(precision) > 0 {
		timeFunc = newTimeFunc("LOCALTIME", constLiteral(precision[0]))
	} else {
		timeFunc = newTimeFunc("LOCALTIME")
	}

	timeFunc.noBrackets = true

	return timeFunc
}

// LOCALTIMESTAMP returns current date and time using optional precision
func LOCALTIMESTAMP(precision ...int) TimestampExpression {
	var timestampFunc *timestampFunc

	if len(precision) > 0 {
		timestampFunc = newTimestampFunc("LOCALTIMESTAMP", constLiteral(precision[0]))
	} else {
		timestampFunc = newTimestampFunc("LOCALTIMESTAMP")
	}

	timestampFunc.noBrackets = true

	return timestampFunc
}

// NOW returns current date and time
func NOW() TimestampzExpression {
	return newTimestampzFunc("NOW")
}

// --------------- Conditional Expressions Functions -------------//

// COALESCE function returns the first of its arguments that is not null.
func COALESCE(value Expression, values ...Expression) Expression {
	var allValues = []Expression{value}
	allValues = append(allValues, values...)
	return newFunc("COALESCE", allValues, nil)
}

// NULLIF function returns a null value if value1 equals value2; otherwise it returns value1.
func NULLIF(value1, value2 Expression) Expression {
	return newFunc("NULLIF", []Expression{value1, value2}, nil)
}

// GREATEST selects the largest  value from a list of expressions
func GREATEST(value Expression, values ...Expression) Expression {
	var allValues = []Expression{value}
	allValues = append(allValues, values...)
	return newFunc("GREATEST", allValues, nil)
}

// LEAST selects the smallest  value from a list of expressions
func LEAST(value Expression, values ...Expression) Expression {
	var allValues = []Expression{value}
	allValues = append(allValues, values...)
	return newFunc("LEAST", allValues, nil)
}

//--------------------------------------------------------------------//

type funcExpressionImpl struct {
	expressionInterfaceImpl

	name        string
	expressions []Expression
	noBrackets  bool
}

func newFunc(name string, expressions []Expression, parent Expression) *funcExpressionImpl {
	funcExp := &funcExpressionImpl{
		name:        name,
		expressions: expressions,
	}

	if parent != nil {
		funcExp.expressionInterfaceImpl.parent = parent
	} else {
		funcExp.expressionInterfaceImpl.parent = funcExp
	}

	return funcExp
}

func (f *funcExpressionImpl) accept(visitor visitor) {
	visitor.visit(f)

	for _, exp := range f.expressions {
		exp.accept(visitor)
	}
}

func (f *funcExpressionImpl) serialize(statement statementType, out *sqlBuilder, options ...serializeOption) error {
	if f == nil {
		return errors.New("jet: Function expressions is nil. ")
	}

	addBrackets := !f.noBrackets || len(f.expressions) > 0

	if addBrackets {
		out.writeString(f.name + "(")
	} else {
		out.writeString(f.name)
	}

	err := serializeExpressionList(statement, f.expressions, ", ", out)
	if err != nil {
		return err
	}

	if addBrackets {
		out.writeString(")")
	}

	return nil
}

type boolFunc struct {
	funcExpressionImpl
	boolInterfaceImpl
}

func newBoolFunc(name string, expressions ...Expression) BoolExpression {
	boolFunc := &boolFunc{}

	boolFunc.funcExpressionImpl = *newFunc(name, expressions, boolFunc)
	boolFunc.boolInterfaceImpl.parent = boolFunc

	return boolFunc
}

type floatFunc struct {
	funcExpressionImpl
	floatInterfaceImpl
}

func NewFloatFunc(name string, expressions ...Expression) FloatExpression {
	floatFunc := &floatFunc{}

	floatFunc.funcExpressionImpl = *newFunc(name, expressions, floatFunc)
	floatFunc.floatInterfaceImpl.parent = floatFunc

	return floatFunc
}

type integerFunc struct {
	funcExpressionImpl
	integerInterfaceImpl
}

func newIntegerFunc(name string, expressions ...Expression) IntegerExpression {
	floatFunc := &integerFunc{}

	floatFunc.funcExpressionImpl = *newFunc(name, expressions, floatFunc)
	floatFunc.integerInterfaceImpl.parent = floatFunc

	return floatFunc
}

type stringFunc struct {
	funcExpressionImpl
	stringInterfaceImpl
}

func newStringFunc(name string, expressions ...Expression) StringExpression {
	stringFunc := &stringFunc{}

	stringFunc.funcExpressionImpl = *newFunc(name, expressions, stringFunc)
	stringFunc.stringInterfaceImpl.parent = stringFunc

	return stringFunc
}

type dateFunc struct {
	funcExpressionImpl
	dateInterfaceImpl
}

func newDateFunc(name string, expressions ...Expression) *dateFunc {
	dateFunc := &dateFunc{}

	dateFunc.funcExpressionImpl = *newFunc(name, expressions, dateFunc)
	dateFunc.dateInterfaceImpl.parent = dateFunc

	return dateFunc
}

type timeFunc struct {
	funcExpressionImpl
	timeInterfaceImpl
}

func newTimeFunc(name string, expressions ...Expression) *timeFunc {
	timeFun := &timeFunc{}

	timeFun.funcExpressionImpl = *newFunc(name, expressions, timeFun)
	timeFun.timeInterfaceImpl.parent = timeFun

	return timeFun
}

type timezFunc struct {
	funcExpressionImpl
	timezInterfaceImpl
}

func newTimezFunc(name string, expressions ...Expression) *timezFunc {
	timezFun := &timezFunc{}

	timezFun.funcExpressionImpl = *newFunc(name, expressions, timezFun)
	timezFun.timezInterfaceImpl.parent = timezFun

	return timezFun
}

type timestampFunc struct {
	funcExpressionImpl
	timestampInterfaceImpl
}

func newTimestampFunc(name string, expressions ...Expression) *timestampFunc {
	timestampFunc := &timestampFunc{}

	timestampFunc.funcExpressionImpl = *newFunc(name, expressions, timestampFunc)
	timestampFunc.timestampInterfaceImpl.parent = timestampFunc

	return timestampFunc
}

type timestampzFunc struct {
	funcExpressionImpl
	timestampzInterfaceImpl
}

func newTimestampzFunc(name string, expressions ...Expression) *timestampzFunc {
	timestampzFunc := &timestampzFunc{}

	timestampzFunc.funcExpressionImpl = *newFunc(name, expressions, timestampzFunc)
	timestampzFunc.timestampzInterfaceImpl.parent = timestampzFunc

	return timestampzFunc
}
