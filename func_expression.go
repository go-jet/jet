package jet

import "errors"

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

func (f *funcExpressionImpl) serialize(statement statementType, out *queryData, options ...serializeOption) error {
	if f == nil {
		return errors.New("Function expressions is nil. ")
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

func newFloatFunc(name string, expressions ...Expression) FloatExpression {
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

func ROW(expressions ...Expression) Expression {
	return newFunc("ROW", expressions, nil)
}

// ------------------ Mathematical functions ---------------//

func ABSf(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("ABS", floatExpression)
}

func ABSi(integerExpression IntegerExpression) IntegerExpression {
	return newIntegerFunc("ABS", integerExpression)
}

func SQRT(numericExpression NumericExpression) FloatExpression {
	return newFloatFunc("SQRT", numericExpression)
}

func CBRT(numericExpression NumericExpression) FloatExpression {
	return newFloatFunc("CBRT", numericExpression)
}

func CEIL(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("CEIL", floatExpression)
}

func FLOOR(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("FLOOR", floatExpression)
}

func ROUND(floatExpression FloatExpression, intExpression ...IntegerExpression) FloatExpression {
	if len(intExpression) > 0 {
		return newFloatFunc("ROUND", floatExpression, intExpression[0])
	}
	return newFloatFunc("ROUND", floatExpression)
}

func SIGN(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("SIGN", floatExpression)
}

func TRUNC(floatExpression FloatExpression, intExpression ...IntegerExpression) FloatExpression {
	if len(intExpression) > 0 {
		return newFloatFunc("TRUNC", floatExpression, intExpression[0])
	}
	return newFloatFunc("TRUNC", floatExpression)
}

func LN(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("LN", floatExpression)
}

func LOG(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("LOG", floatExpression)
}

// ----------------- Aggregate functions  -------------------//

func AVG(numericExpression NumericExpression) FloatExpression {
	return newFloatFunc("AVG", numericExpression)
}

func BIT_AND(integerExpression IntegerExpression) IntegerExpression {
	return newIntegerFunc("BIT_AND", integerExpression)
}

func BIT_OR(integerExpression IntegerExpression) IntegerExpression {
	return newIntegerFunc("BIT_OR", integerExpression)
}

func BOOL_AND(boolExpression BoolExpression) BoolExpression {
	return newBoolFunc("BOOL_AND", boolExpression)
}

func BOOL_OR(boolExpression BoolExpression) BoolExpression {
	return newBoolFunc("BOOL_OR", boolExpression)
}

func COUNT(expression Expression) IntegerExpression {
	return newIntegerFunc("COUNT", expression)
}

func EVERY(boolExpression BoolExpression) BoolExpression {
	return newBoolFunc("EVERY", boolExpression)
}

func MAXf(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("MAX", floatExpression)
}

func MAXi(integerExpression IntegerExpression) IntegerExpression {
	return newIntegerFunc("MAX", integerExpression)
}

func MINf(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("MIN", floatExpression)
}

func MINi(integerExpression IntegerExpression) IntegerExpression {
	return newIntegerFunc("MIN", integerExpression)
}

func SUMf(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("SUM", floatExpression)
}

func SUMi(integerExpression IntegerExpression) IntegerExpression {
	return newIntegerFunc("SUM", integerExpression)
}

//------------ String functions ------------------//

func BIT_LENGTH(stringExpression StringExpression) IntegerExpression {
	return newIntegerFunc("BIT_LENGTH", stringExpression)
}

func CHAR_LENGTH(stringExpression StringExpression) IntegerExpression {
	return newIntegerFunc("CHAR_LENGTH", stringExpression)
}

func OCTET_LENGTH(stringExpression StringExpression) IntegerExpression {
	return newIntegerFunc("OCTET_LENGTH", stringExpression)
}

func LOWER(stringExpression StringExpression) StringExpression {
	return newStringFunc("LOWER", stringExpression)
}

func UPPER(stringExpression StringExpression) StringExpression {
	return newStringFunc("UPPER", stringExpression)
}

func BTRIM(stringExpression StringExpression) StringExpression {
	return newStringFunc("BTRIM", stringExpression)
}

func LTRIM(str StringExpression, trimChars ...StringExpression) StringExpression {
	if len(trimChars) > 0 {
		return newStringFunc("LTRIM", str, trimChars[0])
	}
	return newStringFunc("LTRIM", str)
}

func RTRIM(str StringExpression, trimChars ...StringExpression) StringExpression {
	if len(trimChars) > 0 {
		return newStringFunc("RTRIM", str, trimChars[0])
	}
	return newStringFunc("RTRIM", str)
}

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

func CONVERT(str StringExpression, fromEncoding StringExpression, toEncoding StringExpression) StringExpression {
	return newStringFunc("CONVERT", str, fromEncoding, toEncoding)
}

func CONVERT_FROM(str StringExpression, fromEncoding StringExpression) StringExpression {
	return newStringFunc("CONVERT_FROM", str, fromEncoding)
}

func CONVERT_TO(str StringExpression, toEncoding StringExpression) StringExpression {
	return newStringFunc("CONVERT_TO", str, toEncoding)
}

func ENCODE(data StringExpression, format StringExpression) StringExpression {
	return newStringFunc("ENCODE", data, format)
}

func DECODE(data StringExpression, format StringExpression) StringExpression {
	return newStringFunc("DECODE", data, format)
}

//func FORMAT(formatStr StringExpression, formatArgs ...expressions) StringExpression {
//	args := []expressions{formatStr}
//	args = append(args, formatArgs...)
//	return newStringFunc("FORMAT", args...)
//}

func INITCAP(str StringExpression) StringExpression {
	return newStringFunc("INITCAP", str)
}

func LEFT(str StringExpression, n IntegerExpression) StringExpression {
	return newStringFunc("LEFT", str, n)
}

func RIGHT(str StringExpression, n IntegerExpression) StringExpression {
	return newStringFunc("RIGHT", str, n)
}

func LENGTH(str StringExpression, encoding ...StringExpression) StringExpression {
	if len(encoding) > 0 {
		return newStringFunc("LENGTH", str, encoding[0])
	}
	return newStringFunc("LENGTH", str)
}

func LPAD(str StringExpression, length IntegerExpression, text ...StringExpression) StringExpression {
	if len(text) > 0 {
		return newStringFunc("LPAD", str, length, text[0])
	}

	return newStringFunc("LPAD", str, length)
}

func RPAD(str StringExpression, length IntegerExpression, text ...StringExpression) StringExpression {
	if len(text) > 0 {
		return newStringFunc("RPAD", str, length, text[0])
	}

	return newStringFunc("RPAD", str, length)
}

func MD5(stringExpression StringExpression) StringExpression {
	return newStringFunc("MD5", stringExpression)
}

func REPEAT(str StringExpression, n IntegerExpression) StringExpression {
	return newStringFunc("REPEAT", str, n)
}

func REPLACE(text, from, to StringExpression) StringExpression {
	return newStringFunc("REPLACE", text, from, to)
}

func REVERSE(stringExpression StringExpression) StringExpression {
	return newStringFunc("REVERSE", stringExpression)
}

func STRPOS(str, substring StringExpression) IntegerExpression {
	return newIntegerFunc("STRPOS", str, substring)
}

func SUBSTR(str StringExpression, from IntegerExpression, count ...IntegerExpression) StringExpression {
	if len(count) > 0 {
		return newStringFunc("SUBSTR", str, from, count[0])
	}
	return newStringFunc("SUBSTR", str, from)
}

func TO_ASCII(str StringExpression, encoding ...StringExpression) StringExpression {
	if len(encoding) > 0 {
		return newStringFunc("TO_ASCII", str, encoding[0])
	}
	return newStringFunc("TO_ASCII", str)
}

func TO_HEX(number IntegerExpression) StringExpression {
	return newStringFunc("TO_HEX", number)
}

//----------Data Type Formatting Functions ----------------------//

func TO_CHAR(expression Expression, text StringExpression) StringExpression {
	return newStringFunc("TO_CHAR", expression, text)
}

func TO_DATE(dateStr, format StringExpression) DateExpression {
	return newDateFunc("TO_DATE", dateStr, format)
}

func TO_NUMBER(floatStr, format StringExpression) FloatExpression {
	return newFloatFunc("TO_NUMBER", floatStr, format)
}

func TO_TIMESTAMP(timestampzStr, format StringExpression) TimestampzExpression {
	return newTimestampzFunc("TO_TIMESTAMP", timestampzStr, format)
}

//----------------- Date/Time Functions and Operators ---------------//

func CURRENT_DATE() DateExpression {
	dateFunc := newDateFunc("CURRENT_DATE")
	dateFunc.noBrackets = true
	return dateFunc
}

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

func NOW() TimestampzExpression {
	return newTimestampzFunc("NOW")
}

// --------------- Conditional Expressions Functions -------------//

func COALESCE(value Expression, values ...Expression) Expression {
	var allValues = []Expression{value}
	allValues = append(allValues, values...)
	return newFunc("COALESCE", allValues, nil)
}

func NULLIF(value1, value2 Expression) Expression {
	return newFunc("NULLIF", []Expression{value1, value2}, nil)
}

func GREATEST(value Expression, values ...Expression) Expression {
	var allValues = []Expression{value}
	allValues = append(allValues, values...)
	return newFunc("GREATEST", allValues, nil)
}

func LEAST(value Expression, values ...Expression) Expression {
	var allValues = []Expression{value}
	allValues = append(allValues, values...)
	return newFunc("LEAST", allValues, nil)
}
