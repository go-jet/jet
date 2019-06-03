package sqlbuilder

import "errors"

type funcExpressionImpl struct {
	expressionInterfaceImpl

	name        string
	expressions []expression
	noBrackets  bool
}

func ROW(expressions ...expression) expression {
	return newFunc("ROW", expressions, nil)
}

func newFunc(name string, expressions []expression, parent expression) *funcExpressionImpl {
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

func newBoolFunc(name string, expressions ...expression) BoolExpression {
	boolFunc := &boolFunc{}

	boolFunc.funcExpressionImpl = *newFunc(name, expressions, boolFunc)
	boolFunc.boolInterfaceImpl.parent = boolFunc

	return boolFunc
}

type floatFunc struct {
	funcExpressionImpl
	floatInterfaceImpl
}

func newFloatFunc(name string, expressions ...expression) FloatExpression {
	floatFunc := &floatFunc{}

	floatFunc.funcExpressionImpl = *newFunc(name, expressions, floatFunc)
	floatFunc.floatInterfaceImpl.parent = floatFunc

	return floatFunc
}

type integerFunc struct {
	funcExpressionImpl
	integerInterfaceImpl
}

func newIntegerFunc(name string, expressions ...expression) IntegerExpression {
	floatFunc := &integerFunc{}

	floatFunc.funcExpressionImpl = *newFunc(name, expressions, floatFunc)
	floatFunc.integerInterfaceImpl.parent = floatFunc

	return floatFunc
}

type stringFunc struct {
	funcExpressionImpl
	stringInterfaceImpl
}

func newStringFunc(name string, expressions ...expression) StringExpression {
	stringFunc := &stringFunc{}

	stringFunc.funcExpressionImpl = *newFunc(name, expressions, stringFunc)
	stringFunc.stringInterfaceImpl.parent = stringFunc

	return stringFunc
}

type dateFunc struct {
	funcExpressionImpl
	dateInterfaceImpl
}

func newDateFunc(name string, expressions ...expression) *dateFunc {
	dateFunc := &dateFunc{}

	dateFunc.funcExpressionImpl = *newFunc(name, expressions, dateFunc)
	dateFunc.dateInterfaceImpl.parent = dateFunc

	return dateFunc
}

type timeFunc struct {
	funcExpressionImpl
	timeInterfaceImpl
}

func newTimeFunc(name string, expressions ...expression) *timeFunc {
	timeFun := &timeFunc{}

	timeFun.funcExpressionImpl = *newFunc(name, expressions, timeFun)
	timeFun.timeInterfaceImpl.parent = timeFun

	return timeFun
}

type timezFunc struct {
	funcExpressionImpl
	timezInterfaceImpl
}

func newTimezFunc(name string, expressions ...expression) *timezFunc {
	timezFun := &timezFunc{}

	timezFun.funcExpressionImpl = *newFunc(name, expressions, timezFun)
	timezFun.timezInterfaceImpl.parent = timezFun

	return timezFun
}

type timestampFunc struct {
	funcExpressionImpl
	timestampInterfaceImpl
}

func newTimestampFunc(name string, expressions ...expression) *timestampFunc {
	timestampFunc := &timestampFunc{}

	timestampFunc.funcExpressionImpl = *newFunc(name, expressions, timestampFunc)
	timestampFunc.timestampInterfaceImpl.parent = timestampFunc

	return timestampFunc
}

type timestampzFunc struct {
	funcExpressionImpl
	timestampzInterfaceImpl
}

func newTimestampzFunc(name string, expressions ...expression) *timestampzFunc {
	timestampzFunc := &timestampzFunc{}

	timestampzFunc.funcExpressionImpl = *newFunc(name, expressions, timestampzFunc)
	timestampzFunc.timestampzInterfaceImpl.parent = timestampzFunc

	return timestampzFunc
}

// ------------------ Mathematical functions ---------------//

func ABSf(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("ABS", floatExpression)
}

func ABSi(integerExpression IntegerExpression) FloatExpression {
	return newFloatFunc("ABS", integerExpression)
}

func SQRTf(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("SQRT", floatExpression)
}

func SQRTi(integerExpression IntegerExpression) FloatExpression {
	return newFloatFunc("SQRT", integerExpression)
}

func CBRTf(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("CBRT", floatExpression)
}

func CBRTi(integerExpression IntegerExpression) FloatExpression {
	return newFloatFunc("CBRT", integerExpression)
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

// ----------------- Group function operators -------------------//

func MAXf(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("MAX", floatExpression)
}

func MAXi(integerExpression IntegerExpression) IntegerExpression {
	return newIntegerFunc("MAX", integerExpression)
}

func SUMf(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("SUM", floatExpression)
}

func SUMi(integerExpression IntegerExpression) IntegerExpression {
	return newIntegerFunc("SUM", integerExpression)
}

func COUNTf(floatExpression FloatExpression) FloatExpression {
	return newFloatFunc("COUNT", floatExpression)
}

func COUNTi(integerExpression IntegerExpression) IntegerExpression {
	return newIntegerFunc("COUNT", integerExpression)
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
//func CONCAT(expressions ...expression) StringExpression {
//	return newStringFunc("CONCAT", expressions...)
//}
//
//func CONCAT_WS(expressions ...expression) StringExpression {
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

//func FORMAT(formatStr StringExpression, formatArgs ...expression) StringExpression {
//	args := []expression{formatStr}
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

func TO_CHAR(expression expression, text StringExpression) StringExpression {
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
		timezFunc = newTimezFunc("CURRENT_TIME", ConstantLiteral(precision[0]))
	} else {
		timezFunc = newTimezFunc("CURRENT_TIME")
	}

	timezFunc.noBrackets = true

	return timezFunc
}

func CURRENT_TIMESTAMP(precision ...int) TimestampzExpression {
	var timestampzFunc *timestampzFunc

	if len(precision) > 0 {
		timestampzFunc = newTimestampzFunc("CURRENT_TIMESTAMP", ConstantLiteral(precision[0]))
	} else {
		timestampzFunc = newTimestampzFunc("CURRENT_TIMESTAMP")
	}

	timestampzFunc.noBrackets = true

	return timestampzFunc
}

func LOCALTIME(precision ...int) TimeExpression {
	var timeFunc *timeFunc

	if len(precision) > 0 {
		timeFunc = newTimeFunc("LOCALTIME", ConstantLiteral(precision[0]))
	} else {
		timeFunc = newTimeFunc("LOCALTIME")
	}

	timeFunc.noBrackets = true

	return timeFunc
}

func LOCALTIMESTAMP(precision ...int) TimestampExpression {
	var timestampFunc *timestampFunc

	if len(precision) > 0 {
		timestampFunc = newTimestampFunc("LOCALTIMESTAMP", ConstantLiteral(precision[0]))
	} else {
		timestampFunc = newTimestampFunc("LOCALTIMESTAMP")
	}

	timestampFunc.noBrackets = true

	return timestampFunc
}

func NOW() TimestampzExpression {
	return newTimestampzFunc("NOW")
}
