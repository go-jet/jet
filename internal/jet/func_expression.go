package jet

// AND function adds AND operator between expressions. This function can be used, instead of method AND,
// to have a better inlining of a complex condition in the Go code and in the generated SQL.
func AND(expressions ...BoolExpression) BoolExpression {
	return newBoolExpressionListOperator("AND", expressions...)
}

// OR function adds OR operator between expressions. This function can be used, instead of method OR,
// to have a better inlining of a complex condition in the Go code and in the generated SQL.
func OR(expressions ...BoolExpression) BoolExpression {
	return newBoolExpressionListOperator("OR", expressions...)
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

// POW calculates power of base with exponent
func POW(base, exponent NumericExpression) FloatExpression {
	return NewFloatFunc("POW", base, exponent)
}

// POWER calculates power of base with exponent
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
func AVG(numericExpression Expression) floatWindowExpression {
	return NewFloatWindowFunc("AVG", numericExpression)
}

// BIT_AND is aggregate function used to calculates the bitwise AND of all non-null input values, or null if none.
func BIT_AND(integerExpression IntegerExpression) integerWindowExpression {
	return newIntegerWindowFunc("BIT_AND", integerExpression)
}

// BIT_OR is aggregate function used to calculates the bitwise OR of all non-null input values, or null if none.
func BIT_OR(integerExpression IntegerExpression) integerWindowExpression {
	return newIntegerWindowFunc("BIT_OR", integerExpression)
}

// BOOL_AND is aggregate function. Returns true if all input values are true, otherwise false
func BOOL_AND(boolExpression BoolExpression) boolWindowExpression {
	return newBoolWindowFunc("BOOL_AND", boolExpression)
}

// BOOL_OR is aggregate function. Returns true if at least one input value is true, otherwise false
func BOOL_OR(boolExpression BoolExpression) boolWindowExpression {
	return newBoolWindowFunc("BOOL_OR", boolExpression)
}

// COUNT is aggregate function. Returns number of input rows for which the value of expression is not null.
func COUNT(expression Expression) integerWindowExpression {
	return newIntegerWindowFunc("COUNT", expression)
}

// EVERY is aggregate function. Returns true if all input values are true, otherwise false
func EVERY(boolExpression BoolExpression) boolWindowExpression {
	return newBoolWindowFunc("EVERY", boolExpression)
}

// MAX is aggregate function. Returns minimum value of expression across all input values.
func MAX(expression Expression) Expression {
	return newWindowFunc("MAX", expression)
}

// MAXf is aggregate function. Returns maximum value of float expression across all input values
func MAXf(floatExpression FloatExpression) floatWindowExpression {
	return NewFloatWindowFunc("MAX", floatExpression)
}

// MAXi is aggregate function. Returns maximum value of int expression across all input values
func MAXi(integerExpression IntegerExpression) integerWindowExpression {
	return newIntegerWindowFunc("MAX", integerExpression)
}

// MIN is aggregate function. Returns minimum value of expression across all input values.
func MIN(expression Expression) Expression {
	return newWindowFunc("MIN", expression)
}

// MINf is aggregate function. Returns minimum value of float expression across all input values
func MINf(floatExpression FloatExpression) floatWindowExpression {
	return NewFloatWindowFunc("MIN", floatExpression)
}

// MINi is aggregate function. Returns minimum value of int expression across all input values
func MINi(integerExpression IntegerExpression) integerWindowExpression {
	return newIntegerWindowFunc("MIN", integerExpression)
}

// SUM is aggregate function. Returns sum of all expressions
func SUM(expression Expression) Expression {
	return newWindowFunc("SUM", expression)
}

// SUMf is aggregate function. Returns sum of expression across all float expressions
func SUMf(floatExpression FloatExpression) floatWindowExpression {
	return NewFloatWindowFunc("SUM", floatExpression)
}

// SUMi is aggregate function. Returns sum of expression across all integer expression.
func SUMi(integerExpression IntegerExpression) integerWindowExpression {
	return newIntegerWindowFunc("SUM", integerExpression)
}

// ----------------- Window functions  -------------------//

// ROW_NUMBER returns number of the current row within its partition, counting from 1
func ROW_NUMBER() integerWindowExpression {
	return newIntegerWindowFunc("ROW_NUMBER")
}

// RANK of the current row with gaps; same as row_number of its first peer
func RANK() integerWindowExpression {
	return newIntegerWindowFunc("RANK")
}

// DENSE_RANK returns rank of the current row without gaps; this function counts peer groups
func DENSE_RANK() integerWindowExpression {
	return newIntegerWindowFunc("DENSE_RANK")
}

// PERCENT_RANK calculates relative rank of the current row: (rank - 1) / (total partition rows - 1)
func PERCENT_RANK() floatWindowExpression {
	return NewFloatWindowFunc("PERCENT_RANK")
}

// CUME_DIST calculates cumulative distribution: (number of partition rows preceding or peer with current row) / total partition rows
func CUME_DIST() floatWindowExpression {
	return NewFloatWindowFunc("CUME_DIST")
}

// NTILE returns integer ranging from 1 to the argument value, dividing the partition as equally as possible
func NTILE(numOfBuckets int64) integerWindowExpression {
	return newIntegerWindowFunc("NTILE", FixedLiteral(numOfBuckets))
}

// LAG returns value evaluated at the row that is offset rows before the current row within the partition;
// if there is no such row, instead return default (which must be of the same type as value).
// Both offset and default are evaluated with respect to the current row.
// If omitted, offset defaults to 1 and default to null
func LAG(expr Expression, offsetAndDefault ...interface{}) windowExpression {
	return leadLagImpl("LAG", expr, offsetAndDefault...)
}

// LEAD returns value evaluated at the row that is offset rows after the current row within the partition;
// if there is no such row, instead return default (which must be of the same type as value).
// Both offset and default are evaluated with respect to the current row.
// If omitted, offset defaults to 1 and default to null
func LEAD(expr Expression, offsetAndDefault ...interface{}) windowExpression {
	return leadLagImpl("LEAD", expr, offsetAndDefault...)
}

// FIRST_VALUE returns value evaluated at the row that is the first row of the window frame
func FIRST_VALUE(value Expression) windowExpression {
	return newWindowFunc("FIRST_VALUE", value)
}

// LAST_VALUE returns value evaluated at the row that is the last row of the window frame
func LAST_VALUE(value Expression) windowExpression {
	return newWindowFunc("LAST_VALUE", value)
}

// NTH_VALUE returns value evaluated at the row that is the nth row of the window frame (counting from 1); null if no such row
func NTH_VALUE(value Expression, nth int64) windowExpression {
	return newWindowFunc("NTH_VALUE", value, FixedLiteral(nth))
}

func leadLagImpl(name string, expr Expression, offsetAndDefault ...interface{}) windowExpression {
	params := []Expression{expr}

	if len(offsetAndDefault) >= 2 {
		offset, ok := offsetAndDefault[0].(int)
		if !ok {
			panic("jet: LAG offset should be an integer")
		}

		var defaultValue Expression

		defaultValue, ok = offsetAndDefault[1].(Expression)

		if !ok {
			defaultValue = literal(offsetAndDefault[1])
		}

		params = append(params, FixedLiteral(offset), defaultValue)
	}

	return newWindowFunc(name, params...)
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
	return NewStringFunc("LOWER", stringExpression)
}

// UPPER returns string expression in upper case
func UPPER(stringExpression StringExpression) StringExpression {
	return NewStringFunc("UPPER", stringExpression)
}

// BTRIM removes the longest string consisting only of characters
// in characters (a space by default) from the start and end of string
func BTRIM(stringExpression StringExpression, trimChars ...StringExpression) StringExpression {
	if len(trimChars) > 0 {
		return NewStringFunc("BTRIM", stringExpression, trimChars[0])
	}
	return NewStringFunc("BTRIM", stringExpression)
}

// LTRIM removes the longest string containing only characters
// from characters (a space by default) from the start of string
func LTRIM(str StringExpression, trimChars ...StringExpression) StringExpression {
	if len(trimChars) > 0 {
		return NewStringFunc("LTRIM", str, trimChars[0])
	}
	return NewStringFunc("LTRIM", str)
}

// RTRIM removes the longest string containing only characters
// from characters (a space by default) from the end of string
func RTRIM(str StringExpression, trimChars ...StringExpression) StringExpression {
	if len(trimChars) > 0 {
		return NewStringFunc("RTRIM", str, trimChars[0])
	}
	return NewStringFunc("RTRIM", str)
}

// CHR returns character with the given code.
func CHR(integerExpression IntegerExpression) StringExpression {
	return NewStringFunc("CHR", integerExpression)
}

// CONCAT adds two or more expressions together
func CONCAT(expressions ...Expression) StringExpression {
	return NewStringFunc("CONCAT", expressions...)
}

// CONCAT_WS adds two or more expressions together with a separator.
func CONCAT_WS(separator Expression, expressions ...Expression) StringExpression {
	return NewStringFunc("CONCAT_WS", append([]Expression{separator}, expressions...)...)
}

// CONVERT converts string to dest_encoding. The original encoding is
// specified by src_encoding. The string must be valid in this encoding.
func CONVERT(str StringExpression, srcEncoding StringExpression, destEncoding StringExpression) StringExpression {
	return NewStringFunc("CONVERT", str, srcEncoding, destEncoding)
}

// CONVERT_FROM converts string to the database encoding. The original
// encoding is specified by src_encoding. The string must be valid in this encoding.
func CONVERT_FROM(str StringExpression, srcEncoding StringExpression) StringExpression {
	return NewStringFunc("CONVERT_FROM", str, srcEncoding)
}

// CONVERT_TO converts string to dest_encoding.
func CONVERT_TO(str StringExpression, toEncoding StringExpression) StringExpression {
	return NewStringFunc("CONVERT_TO", str, toEncoding)
}

// ENCODE encodes binary data into a textual representation.
// Supported formats are: base64, hex, escape. escape converts zero bytes and
// high-bit-set bytes to octal sequences (\nnn) and doubles backslashes.
func ENCODE(data StringExpression, format StringExpression) StringExpression {
	return NewStringFunc("ENCODE", data, format)
}

// DECODE decodes binary data from textual representation in string.
// Options for format are same as in encode.
func DECODE(data StringExpression, format StringExpression) StringExpression {
	return NewStringFunc("DECODE", data, format)
}

// FORMAT formats a number to a format like "#,###,###.##", rounded to a specified number of decimal places, then it returns the result as a string.
func FORMAT(formatStr StringExpression, formatArgs ...Expression) StringExpression {
	args := []Expression{formatStr}
	args = append(args, formatArgs...)
	return NewStringFunc("FORMAT", args...)
}

// INITCAP converts the first letter of each word to upper case
// and the rest to lower case. Words are sequences of alphanumeric
// characters separated by non-alphanumeric characters.
func INITCAP(str StringExpression) StringExpression {
	return NewStringFunc("INITCAP", str)
}

// LEFT returns first n characters in the string.
// When n is negative, return all but last |n| characters.
func LEFT(str StringExpression, n IntegerExpression) StringExpression {
	return NewStringFunc("LEFT", str, n)
}

// RIGHT returns last n characters in the string.
// When n is negative, return all but first |n| characters.
func RIGHT(str StringExpression, n IntegerExpression) StringExpression {
	return NewStringFunc("RIGHT", str, n)
}

// LENGTH returns number of characters in string with a given encoding
func LENGTH(str StringExpression, encoding ...StringExpression) StringExpression {
	if len(encoding) > 0 {
		return NewStringFunc("LENGTH", str, encoding[0])
	}
	return NewStringFunc("LENGTH", str)
}

// LPAD fills up the string to length length by prepending the characters
// fill (a space by default). If the string is already longer than length
// then it is truncated (on the right).
func LPAD(str StringExpression, length IntegerExpression, text ...StringExpression) StringExpression {
	if len(text) > 0 {
		return NewStringFunc("LPAD", str, length, text[0])
	}

	return NewStringFunc("LPAD", str, length)
}

// RPAD fills up the string to length length by appending the characters
// fill (a space by default). If the string is already longer than length then it is truncated.
func RPAD(str StringExpression, length IntegerExpression, text ...StringExpression) StringExpression {
	if len(text) > 0 {
		return NewStringFunc("RPAD", str, length, text[0])
	}

	return NewStringFunc("RPAD", str, length)
}

// MD5 calculates the MD5 hash of string, returning the result in hexadecimal
func MD5(stringExpression StringExpression) StringExpression {
	return NewStringFunc("MD5", stringExpression)
}

// REPEAT repeats string the specified number of times
func REPEAT(str StringExpression, n IntegerExpression) StringExpression {
	return NewStringFunc("REPEAT", str, n)
}

// REPLACE replaces all occurrences in string of substring from with substring to
func REPLACE(text, from, to StringExpression) StringExpression {
	return NewStringFunc("REPLACE", text, from, to)
}

// REVERSE returns reversed string.
func REVERSE(stringExpression StringExpression) StringExpression {
	return NewStringFunc("REVERSE", stringExpression)
}

// STRPOS returns location of specified substring (same as position(substring in string),
// but note the reversed argument order)
func STRPOS(str, substring StringExpression) IntegerExpression {
	return newIntegerFunc("STRPOS", str, substring)
}

// SUBSTR extracts substring
func SUBSTR(str StringExpression, from IntegerExpression, count ...IntegerExpression) StringExpression {
	if len(count) > 0 {
		return NewStringFunc("SUBSTR", str, from, count[0])
	}
	return NewStringFunc("SUBSTR", str, from)
}

// TO_ASCII convert string to ASCII from another encoding
func TO_ASCII(str StringExpression, encoding ...StringExpression) StringExpression {
	if len(encoding) > 0 {
		return NewStringFunc("TO_ASCII", str, encoding[0])
	}
	return NewStringFunc("TO_ASCII", str)
}

// TO_HEX converts number to its equivalent hexadecimal representation
func TO_HEX(number IntegerExpression) StringExpression {
	return NewStringFunc("TO_HEX", number)
}

// REGEXP_LIKE Returns 1 if the string expr matches the regular expression specified by the pattern pat, 0 otherwise.
func REGEXP_LIKE(stringExp StringExpression, pattern StringExpression, matchType ...string) BoolExpression {
	if len(matchType) > 0 {
		return newBoolFunc("REGEXP_LIKE", stringExp, pattern, FixedLiteral(matchType[0]))
	}

	return newBoolFunc("REGEXP_LIKE", stringExp, pattern)
}

//----------Range Type Functions ----------------------//

// LOWER_BOUND returns range expressions lower bound. Returns null if range is empty or the requested bound is infinite.
func LOWER_BOUND[T Expression](rangeExpression Range[T]) T {
	return rangeTypeCaster[T](rangeExpression, NewFunc("LOWER", []Expression{rangeExpression}, nil))
}

// UPPER_BOUND returns range expressions upper bound. Returns null if range is empty or the requested bound is infinite.
func UPPER_BOUND[T Expression](rangeExpression Range[T]) T {
	return rangeTypeCaster[T](rangeExpression, NewFunc("UPPER", []Expression{rangeExpression}, nil))
}

func rangeTypeCaster[T Expression](rangeExpression Range[T], exp Expression) T {
	var i Expression
	switch rangeExpression.(type) {
	case Range[Int4Expression], Range[Int8Expression]:
		i = IntExp(exp)
	case Range[NumericExpression]:
		i = FloatExp(exp)
	case Range[DateExpression]:
		i = DateExp(exp)
	case Range[TimestampExpression]:
		i = TimestampExp(exp)
	case Range[TimestampzExpression]:
		i = TimestampzExp(exp)
	}
	return i.(T)
}

// IS_EMPTY returns true if range is empty
func IS_EMPTY[T Expression](rangeExpression Range[T]) BoolExpression {
	return newBoolFunc("ISEMPTY", rangeExpression)
}

// LOWER_INC returns true if lower bound is inclusive. Returns false for empty range.
func LOWER_INC[T Expression](rangeExpression Range[T]) BoolExpression {
	return newBoolFunc("LOWER_INC", rangeExpression)
}

// UPPER_INC returns true if upper bound is inclusive. Returns false for empty range.
func UPPER_INC[T Expression](rangeExpression Range[T]) BoolExpression {
	return newBoolFunc("UPPER_INC", rangeExpression)
}

// LOWER_INF returns true if upper bound is infinite. Returns false for empty range.
func LOWER_INF[T Expression](rangeExpression Range[T]) BoolExpression {
	return newBoolFunc("LOWER_INF", rangeExpression)
}

// UPPER_INF returns true if lower bound is infinite. Returns false for empty range.
func UPPER_INF[T Expression](rangeExpression Range[T]) BoolExpression {
	return newBoolFunc("UPPER_INF", rangeExpression)
}

//----------Data Type Formatting Functions ----------------------//

// TO_CHAR converts expression to string with format
func TO_CHAR(expression Expression, format StringExpression) StringExpression {
	return NewStringFunc("TO_CHAR", expression, format)
}

// TO_DATE converts string to date using format
func TO_DATE(dateStr, format StringExpression) DateExpression {
	return NewDateFunc("TO_DATE", dateStr, format)
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

// EXTRACT extracts time component from time expression
func EXTRACT(field string, from Expression) Expression {
	return CustomExpression(Token("EXTRACT("), Token(field), Token("FROM"), from, Token(")"))
}

// CURRENT_DATE returns current date
func CURRENT_DATE() DateExpression {
	dateFunc := NewDateFunc("CURRENT_DATE")
	dateFunc.noBrackets = true
	return dateFunc
}

// CURRENT_TIME returns current time with time zone
func CURRENT_TIME(precision ...int) TimezExpression {
	var timezFunc *timezFunc

	if len(precision) > 0 {
		timezFunc = newTimezFunc("CURRENT_TIME", FixedLiteral(precision[0]))
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
		timestampzFunc = newTimestampzFunc("CURRENT_TIMESTAMP", FixedLiteral(precision[0]))
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
		timeFunc = NewTimeFunc("LOCALTIME", FixedLiteral(precision[0]))
	} else {
		timeFunc = NewTimeFunc("LOCALTIME")
	}

	timeFunc.noBrackets = true

	return timeFunc
}

// LOCALTIMESTAMP returns current date and time using optional precision
func LOCALTIMESTAMP(precision ...int) TimestampExpression {
	var timestampFunc *timestampFunc

	if len(precision) > 0 {
		timestampFunc = NewTimestampFunc("LOCALTIMESTAMP", FixedLiteral(precision[0]))
	} else {
		timestampFunc = NewTimestampFunc("LOCALTIMESTAMP")
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
	return NewFunc("COALESCE", allValues, nil)
}

// NULLIF function returns a null value if value1 equals value2; otherwise it returns value1.
func NULLIF(value1, value2 Expression) Expression {
	return NewFunc("NULLIF", []Expression{value1, value2}, nil)
}

// GREATEST selects the largest  value from a list of expressions
func GREATEST(value Expression, values ...Expression) Expression {
	var allValues = []Expression{value}
	allValues = append(allValues, values...)
	return NewFunc("GREATEST", allValues, nil)
}

// LEAST selects the smallest  value from a list of expressions
func LEAST(value Expression, values ...Expression) Expression {
	var allValues = []Expression{value}
	allValues = append(allValues, values...)
	return NewFunc("LEAST", allValues, nil)
}

//--------------------------------------------------------------------//

type funcExpressionImpl struct {
	ExpressionInterfaceImpl

	name       string
	parameters parametersSerializer
	noBrackets bool
}

// NewFunc creates new function with name and expressions parameters
func NewFunc(name string, expressions []Expression, parent Expression) *funcExpressionImpl {
	funcExp := &funcExpressionImpl{
		name:       name,
		parameters: parametersSerializer(expressions),
	}

	if parent != nil {
		funcExp.ExpressionInterfaceImpl.Parent = parent
	} else {
		funcExp.ExpressionInterfaceImpl.Parent = funcExp
	}

	return funcExp
}

func (f *funcExpressionImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	if serializeOverride := out.Dialect.FunctionSerializeOverride(f.name); serializeOverride != nil {
		serializeOverrideFunc := serializeOverride(ExpressionListToSerializerList(f.parameters)...)
		serializeOverrideFunc(statement, out, FallTrough(options)...)
		return
	}

	addBrackets := !f.noBrackets || len(f.parameters) > 0

	if addBrackets {
		out.WriteString(f.name + "(")
	} else {
		out.WriteString(f.name)
	}

	f.parameters.serialize(statement, out, options...)

	if addBrackets {
		out.WriteString(")")
	}
}

type parametersSerializer []Expression

func (p parametersSerializer) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {

	for i, expression := range p {
		if i > 0 {
			out.WriteString(", ")
		}

		if _, isStatement := expression.(Statement); isStatement {
			expression.serialize(statement, out, options...)
		} else {
			expression.serialize(statement, out, append(options, NoWrap, Ident)...)
		}
	}
}

// NewFloatWindowFunc creates new float function with name and expressions
func newWindowFunc(name string, expressions ...Expression) windowExpression {
	newFun := NewFunc(name, expressions, nil)
	windowExpr := newWindowExpression(newFun)
	newFun.ExpressionInterfaceImpl.Parent = windowExpr

	return windowExpr
}

type boolFunc struct {
	funcExpressionImpl
	boolInterfaceImpl
}

func newBoolFunc(name string, expressions ...Expression) BoolExpression {
	boolFunc := &boolFunc{}

	boolFunc.funcExpressionImpl = *NewFunc(name, expressions, boolFunc)
	boolFunc.boolInterfaceImpl.parent = boolFunc
	boolFunc.ExpressionInterfaceImpl.Parent = boolFunc

	return boolFunc
}

// NewFloatWindowFunc creates new float function with name and expressions
func newBoolWindowFunc(name string, expressions ...Expression) boolWindowExpression {
	boolFunc := &boolFunc{}

	boolFunc.funcExpressionImpl = *NewFunc(name, expressions, boolFunc)
	intWindowFunc := newBoolWindowExpression(boolFunc)
	boolFunc.boolInterfaceImpl.parent = intWindowFunc
	boolFunc.ExpressionInterfaceImpl.Parent = intWindowFunc

	return intWindowFunc
}

type floatFunc struct {
	funcExpressionImpl
	floatInterfaceImpl
}

// NewFloatFunc creates new float function with name and expressions
func NewFloatFunc(name string, expressions ...Expression) FloatExpression {
	floatFunc := &floatFunc{}

	floatFunc.funcExpressionImpl = *NewFunc(name, expressions, floatFunc)
	floatFunc.floatInterfaceImpl.parent = floatFunc

	return floatFunc
}

// NewFloatWindowFunc creates new float function with name and expressions
func NewFloatWindowFunc(name string, expressions ...Expression) floatWindowExpression {
	floatFunc := &floatFunc{}

	floatFunc.funcExpressionImpl = *NewFunc(name, expressions, floatFunc)
	floatWindowFunc := newFloatWindowExpression(floatFunc)
	floatFunc.floatInterfaceImpl.parent = floatWindowFunc
	floatFunc.ExpressionInterfaceImpl.Parent = floatWindowFunc

	return floatWindowFunc
}

type integerFunc struct {
	funcExpressionImpl
	integerInterfaceImpl
}

func newIntegerFunc(name string, expressions ...Expression) IntegerExpression {
	intFunc := &integerFunc{}

	intFunc.funcExpressionImpl = *NewFunc(name, expressions, intFunc)
	intFunc.integerInterfaceImpl.parent = intFunc

	return intFunc
}

// NewFloatWindowFunc creates new float function with name and expressions
func newIntegerWindowFunc(name string, expressions ...Expression) integerWindowExpression {
	integerFunc := &integerFunc{}

	integerFunc.funcExpressionImpl = *NewFunc(name, expressions, integerFunc)
	intWindowFunc := newIntegerWindowExpression(integerFunc)
	integerFunc.integerInterfaceImpl.parent = intWindowFunc
	integerFunc.ExpressionInterfaceImpl.Parent = intWindowFunc

	return intWindowFunc
}

type stringFunc struct {
	funcExpressionImpl
	stringInterfaceImpl
}

// NewStringFunc creates new string function with name and expression parameters
func NewStringFunc(name string, expressions ...Expression) StringExpression {
	stringFunc := &stringFunc{}

	stringFunc.funcExpressionImpl = *NewFunc(name, expressions, stringFunc)
	stringFunc.stringInterfaceImpl.parent = stringFunc

	return stringFunc
}

type dateFunc struct {
	funcExpressionImpl
	dateInterfaceImpl
}

// NewDateFunc creates new date function with name and expression parameters
func NewDateFunc(name string, expressions ...Expression) *dateFunc {
	dateFunc := &dateFunc{}

	dateFunc.funcExpressionImpl = *NewFunc(name, expressions, dateFunc)
	dateFunc.dateInterfaceImpl.parent = dateFunc

	return dateFunc
}

type timeFunc struct {
	funcExpressionImpl
	timeInterfaceImpl
}

// NewTimeFunc creates new time function with name and expression parameters
func NewTimeFunc(name string, expressions ...Expression) *timeFunc {
	timeFun := &timeFunc{}

	timeFun.funcExpressionImpl = *NewFunc(name, expressions, timeFun)
	timeFun.timeInterfaceImpl.parent = timeFun

	return timeFun
}

type timezFunc struct {
	funcExpressionImpl
	timezInterfaceImpl
}

func newTimezFunc(name string, expressions ...Expression) *timezFunc {
	timezFun := &timezFunc{}

	timezFun.funcExpressionImpl = *NewFunc(name, expressions, timezFun)
	timezFun.timezInterfaceImpl.parent = timezFun

	return timezFun
}

type timestampFunc struct {
	funcExpressionImpl
	timestampInterfaceImpl
}

// NewTimestampFunc creates new timestamp function with name and expressions
func NewTimestampFunc(name string, expressions ...Expression) *timestampFunc {
	timestampFunc := &timestampFunc{}

	timestampFunc.funcExpressionImpl = *NewFunc(name, expressions, timestampFunc)
	timestampFunc.timestampInterfaceImpl.parent = timestampFunc

	return timestampFunc
}

type timestampzFunc struct {
	funcExpressionImpl
	timestampzInterfaceImpl
}

func newTimestampzFunc(name string, expressions ...Expression) *timestampzFunc {
	timestampzFunc := &timestampzFunc{}

	timestampzFunc.funcExpressionImpl = *NewFunc(name, expressions, timestampzFunc)
	timestampzFunc.timestampzInterfaceImpl.parent = timestampzFunc

	return timestampzFunc
}

// Func can be used to call custom or unsupported database functions.
func Func(name string, expressions ...Expression) Expression {
	return NewFunc(name, expressions, nil)
}

func NumRange(lowNum, highNum NumericExpression, bounds ...StringExpression) Range[NumericExpression] {
	return NumRangeExp(NewFunc("numrange", rangeFuncParamCombiner(lowNum, highNum, bounds...), nil))
}

func Int4Range(lowNum, highNum IntegerExpression, bounds ...StringExpression) Range[Int4Expression] {
	return Int4RangeExp(NewFunc("int4range", rangeFuncParamCombiner(lowNum, highNum, bounds...), nil))
}

func Int8Range(lowNum, highNum Int8Expression, bounds ...StringExpression) Range[Int8Expression] {
	return Int8RangeExp(NewFunc("int8range", rangeFuncParamCombiner(lowNum, highNum, bounds...), nil))
}

func TsRange(lowTs, highTs TimestampExpression, bounds ...StringExpression) Range[TimestampExpression] {
	return TsRangeExp(NewFunc("tsrange", rangeFuncParamCombiner(lowTs, highTs, bounds...), nil))
}

func TstzRange(lowTs, highTs TimestampzExpression, bounds ...StringExpression) Range[TimestampzExpression] {
	return TstzRangeExp(NewFunc("tstzrange", rangeFuncParamCombiner(lowTs, highTs, bounds...), nil))
}

func DateRange(lowTs, highTs DateExpression, bounds ...StringExpression) Range[DateExpression] {
	return DateRangeExp(NewFunc("daterange", rangeFuncParamCombiner(lowTs, highTs, bounds...), nil))
}

func rangeFuncParamCombiner(low, high Expression, bounds ...StringExpression) []Expression {
	exp := []Expression{low, high}
	if len(bounds) != 0 {
		exp = append(exp, bounds[0])
	}
	return exp
}
