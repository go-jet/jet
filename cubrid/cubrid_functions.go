package cubrid

// ===== Atomic click counter functions (CUBRID-specific) =====

// INCR atomically increments an integer column and returns the old value.
// Only works on SMALLINT, INT, BIGINT columns and requires exactly 1 result row.
func INCR(column IntegerExpression) IntegerExpression {
	return IntExp(Func("INCR", column))
}

// DECR atomically decrements an integer column and returns the old value.
// Only works on SMALLINT, INT, BIGINT columns and requires exactly 1 result row.
func DECR(column IntegerExpression) IntegerExpression {
	return IntExp(Func("DECR", column))
}

// ===== Conditional functions =====

// NVL returns the first non-NULL argument (CUBRID-specific alternative to COALESCE).
func NVL(expr1, expr2 Expression) Expression {
	return Func("NVL", expr1, expr2)
}

// NVL2 returns expr2 if expr1 is not NULL, otherwise returns expr3.
func NVL2(expr1, expr2, expr3 Expression) Expression {
	return Func("NVL2", expr1, expr2, expr3)
}

// DECODE is CUBRID's equivalent of a simple CASE expression.
//
//	DECODE(expr, search1, result1, search2, result2, ..., default)
func DECODE(args ...Expression) Expression {
	return Func("DECODE", args...)
}

// IF_ returns trueVal if condition is true, otherwise falseVal.
func IF_(condition BoolExpression, trueVal, falseVal Expression) Expression {
	return Func("IF", condition, trueVal, falseVal)
}

// IFNULL returns expr1 if it is not NULL, otherwise returns expr2.
func IFNULL(expr1, expr2 Expression) Expression {
	return Func("IFNULL", expr1, expr2)
}

// ===== String functions =====

// CHR returns the character corresponding to the given ASCII code.
func CHR(code IntegerExpression) StringExpression {
	return StringExp(Func("CHR", code))
}

// TRANSLATE replaces characters in string matching from_chars with to_chars.
func TRANSLATE(str, fromChars, toChars StringExpression) StringExpression {
	return StringExp(Func("TRANSLATE", str, fromChars, toChars))
}

// LOCATE returns the position of substring in string, starting at optional position.
func LOCATE(substr, str StringExpression, pos ...IntegerExpression) IntegerExpression {
	if len(pos) > 0 {
		return IntExp(Func("LOCATE", substr, str, pos[0]))
	}
	return IntExp(Func("LOCATE", substr, str))
}

// ===== Numeric functions =====

// DRAND returns a random double value between 0.0 and 1.0.
func DRAND(seed ...IntegerExpression) FloatExpression {
	if len(seed) > 0 {
		return FloatExp(Func("DRAND", seed[0]))
	}
	return FloatExp(Func("DRAND"))
}

// DRANDOM returns a random double value between 0.0 and 1.0.
func DRANDOM(seed ...IntegerExpression) FloatExpression {
	if len(seed) > 0 {
		return FloatExp(Func("DRANDOM", seed[0]))
	}
	return FloatExp(Func("DRANDOM"))
}

// ===== Date/Time functions =====

// DATEDIFF returns the number of days between two dates.
func DATEDIFF(date1, date2 DateExpression) IntegerExpression {
	return IntExp(Func("DATEDIFF", date1, date2))
}

// TIMEDIFF returns the time difference between two time/datetime values.
func TIMEDIFF(time1, time2 Expression) TimeExpression {
	return TimeExp(Func("TIMEDIFF", time1, time2))
}

// ADDDATE adds a number of days to a date.
func ADDDATE(date DateExpression, days IntegerExpression) DateExpression {
	return DateExp(Func("ADDDATE", date, days))
}

// SUBDATE subtracts a number of days from a date.
func SUBDATE(date DateExpression, days IntegerExpression) DateExpression {
	return DateExp(Func("SUBDATE", date, days))
}

// ADD_MONTHS adds the specified number of months to a date.
func ADD_MONTHS(date DateExpression, months IntegerExpression) DateExpression {
	return DateExp(Func("ADD_MONTHS", date, months))
}

// MONTHS_BETWEEN returns the number of months between two dates.
func MONTHS_BETWEEN(date1, date2 DateExpression) FloatExpression {
	return FloatExp(Func("MONTHS_BETWEEN", date1, date2))
}

// LAST_DAY returns the last day of the month for the given date.
func LAST_DAY(date DateExpression) DateExpression {
	return DateExp(Func("LAST_DAY", date))
}

// SYS_DATE returns the current system date.
var SYS_DATE = RawDate("SYS_DATE")

// SYS_TIME returns the current system time.
var SYS_TIME = RawTime("SYS_TIME")

// SYS_DATETIME returns the current system datetime.
var SYS_DATETIME = RawTimestamp("SYS_DATETIME")

// SYS_TIMESTAMP returns the current system timestamp.
var SYS_TIMESTAMP = RawTimestamp("SYS_TIMESTAMP")

// UTC_DATE returns the current UTC date.
var UTC_DATE = RawDate("UTC_DATE()")

// UTC_TIME returns the current UTC time.
var UTC_TIME = RawTime("UTC_TIME()")

// FROM_UNIXTIME converts a Unix timestamp to datetime.
func FROM_UNIXTIME(timestamp IntegerExpression, format ...StringExpression) Expression {
	if len(format) > 0 {
		return Func("FROM_UNIXTIME", timestamp, format[0])
	}
	return Func("FROM_UNIXTIME", timestamp)
}

// UNIX_TIMESTAMP converts datetime to Unix timestamp.
func UNIX_TIMESTAMP(datetime ...Expression) IntegerExpression {
	if len(datetime) > 0 {
		return IntExp(Func("UNIX_TIMESTAMP", datetime[0]))
	}
	return IntExp(Func("UNIX_TIMESTAMP"))
}

// YEAR_ extracts the year from a date expression.
func YEAR_(date Expression) IntegerExpression {
	return IntExp(Func("YEAR", date))
}

// MONTH_ extracts the month from a date expression.
func MONTH_(date Expression) IntegerExpression {
	return IntExp(Func("MONTH", date))
}

// DAY_ extracts the day from a date expression.
func DAY_(date Expression) IntegerExpression {
	return IntExp(Func("DAY", date))
}

// HOUR_ extracts the hour from a time/datetime expression.
func HOUR_(expr Expression) IntegerExpression {
	return IntExp(Func("HOUR", expr))
}

// MINUTE_ extracts the minute from a time/datetime expression.
func MINUTE_(expr Expression) IntegerExpression {
	return IntExp(Func("MINUTE", expr))
}

// SECOND_ extracts the second from a time/datetime expression.
func SECOND_(expr Expression) IntegerExpression {
	return IntExp(Func("SECOND", expr))
}

// MAKEDATE creates a date from year and day of year.
func MAKEDATE(year, dayOfYear IntegerExpression) DateExpression {
	return DateExp(Func("MAKEDATE", year, dayOfYear))
}

// MAKETIME creates a time from hour, minute, second.
func MAKETIME(hour, minute, second IntegerExpression) TimeExpression {
	return TimeExp(Func("MAKETIME", hour, minute, second))
}

// STR_TO_DATE parses a string to date using format.
func STR_TO_DATE(str, format StringExpression) DateTimeExpression {
	return TimestampExp(Func("STR_TO_DATE", str, format))
}

// DATE_FORMAT formats a date/datetime using a format string.
func DATE_FORMAT(date Expression, format StringExpression) StringExpression {
	return StringExp(Func("DATE_FORMAT", date, format))
}

// ===== Type conversion functions =====

// TO_CHAR converts a value to a string with an optional format.
func TO_CHAR(expr Expression, format ...StringExpression) StringExpression {
	if len(format) > 0 {
		return StringExp(Func("TO_CHAR", expr, format[0]))
	}
	return StringExp(Func("TO_CHAR", expr))
}

// TO_DATE converts a string to a date.
func TO_DATE(str StringExpression, format ...StringExpression) DateExpression {
	if len(format) > 0 {
		return DateExp(Func("TO_DATE", str, format[0]))
	}
	return DateExp(Func("TO_DATE", str))
}

// TO_DATETIME converts a string to a datetime.
func TO_DATETIME(str StringExpression, format ...StringExpression) DateTimeExpression {
	if len(format) > 0 {
		return TimestampExp(Func("TO_DATETIME", str, format[0]))
	}
	return TimestampExp(Func("TO_DATETIME", str))
}

// TO_TIMESTAMP converts a string to a timestamp.
func TO_TIMESTAMP(str StringExpression, format ...StringExpression) TimestampExpression {
	if len(format) > 0 {
		return TimestampExp(Func("TO_TIMESTAMP", str, format[0]))
	}
	return TimestampExp(Func("TO_TIMESTAMP", str))
}

// TO_NUMBER converts a string to a number.
func TO_NUMBER(str StringExpression) FloatExpression {
	return FloatExp(Func("TO_NUMBER", str))
}

// INSTR returns the position of substring in string.
func INSTR(str, substr StringExpression) IntegerExpression {
	return IntExp(Func("INSTR", str, substr))
}

// ===== Information functions =====

// DATABASE_ returns the current database name.
func DATABASE_() StringExpression {
	return StringExp(Func("DATABASE"))
}

// SCHEMA_ returns the current schema name.
func SCHEMA_() StringExpression {
	return StringExp(Func("SCHEMA"))
}

// USER_ returns the current user name.
func USER_() StringExpression {
	return StringExp(Func("USER"))
}

// CURRENT_USER_ returns the current user name.
func CURRENT_USER_() StringExpression {
	return StringExp(Func("CURRENT_USER"))
}

// ROW_COUNT returns the number of rows affected by the last statement.
func ROW_COUNT() IntegerExpression {
	return IntExp(Func("ROW_COUNT"))
}

// FOUND_ROWS returns the total rows of the last SELECT.
func FOUND_ROWS() IntegerExpression {
	return IntExp(Func("FOUND_ROWS"))
}

// LAST_INSERT_ID returns the last auto-generated ID.
func LAST_INSERT_ID(expr ...IntegerExpression) IntegerExpression {
	if len(expr) > 0 {
		return IntExp(Func("LAST_INSERT_ID", expr[0]))
	}
	return IntExp(Func("LAST_INSERT_ID"))
}

// ===== Collection functions =====

// SETEQ returns true if two sets are equal.
func SETEQ(set1, set2 Expression) BoolExpression {
	return BoolExp(BinaryOperator(set1, set2, "SETEQ"))
}

// SETNEQ returns true if two sets are not equal.
func SETNEQ(set1, set2 Expression) BoolExpression {
	return BoolExp(BinaryOperator(set1, set2, "SETNEQ"))
}

// SUBSET returns true if set1 is a proper subset of set2.
func SUBSET(set1, set2 Expression) BoolExpression {
	return BoolExp(BinaryOperator(set1, set2, "SUBSET"))
}

// SUBSETEQ returns true if set1 is a subset of or equal to set2.
func SUBSETEQ(set1, set2 Expression) BoolExpression {
	return BoolExp(BinaryOperator(set1, set2, "SUBSETEQ"))
}

// SUPERSET returns true if set1 is a proper superset of set2.
func SUPERSET(set1, set2 Expression) BoolExpression {
	return BoolExp(BinaryOperator(set1, set2, "SUPERSET"))
}

// SUPERSETEQ returns true if set1 is a superset of or equal to set2.
func SUPERSETEQ(set1, set2 Expression) BoolExpression {
	return BoolExp(BinaryOperator(set1, set2, "SUPERSETEQ"))
}
