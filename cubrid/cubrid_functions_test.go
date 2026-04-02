package cubrid

import (
	"testing"
)

// === Atomic click counter ===

func TestINCR(t *testing.T) {
	assertSerialize(t, INCR(table1ColInt), "INCR(table1.col_int)")
}

func TestDECR(t *testing.T) {
	assertSerialize(t, DECR(table1ColInt), "DECR(table1.col_int)")
}

// === Conditional ===

func TestNVL(t *testing.T) {
	assertSerialize(t, NVL(table1ColString, String("default")),
		"NVL(table1.col_string, ?)", "default")
}

func TestNVL2(t *testing.T) {
	assertSerialize(t, NVL2(table1ColString, String("not null"), String("is null")),
		"NVL2(table1.col_string, ?, ?)", "not null", "is null")
}

func TestDECODE(t *testing.T) {
	assertSerialize(t, DECODE(table1ColInt, Int(1), String("one"), Int(2), String("two"), String("other")),
		"DECODE(table1.col_int, ?, ?, ?, ?, ?)", int64(1), "one", int64(2), "two", "other")
}

func TestIF_(t *testing.T) {
	assertSerialize(t, IF_(table1ColBool, String("yes"), String("no")),
		"IF(table1.col_bool, ?, ?)", "yes", "no")
}

func TestIFNULL(t *testing.T) {
	assertSerialize(t, IFNULL(table1ColInt, Int(0)),
		"IFNULL(table1.col_int, ?)", int64(0))
}

// === String ===

func TestCHR(t *testing.T) {
	assertSerialize(t, CHR(Int(65)), "CHR(?)", int64(65))
}

func TestTRANSLATE(t *testing.T) {
	assertSerialize(t, TRANSLATE(table1ColString, String("abc"), String("xyz")),
		"TRANSLATE(table1.col_string, ?, ?)", "abc", "xyz")
}

func TestLOCATE(t *testing.T) {
	assertSerialize(t, LOCATE(String("bar"), table1ColString),
		"LOCATE(?, table1.col_string)", "bar")
	assertSerialize(t, LOCATE(String("bar"), table1ColString, Int(3)),
		"LOCATE(?, table1.col_string, ?)", "bar", int64(3))
}

// === Numeric ===

func TestDRAND(t *testing.T) {
	assertSerialize(t, DRAND(), "DRAND()")
	assertSerialize(t, DRAND(Int(42)), "DRAND(?)", int64(42))
}

func TestDRANDOM(t *testing.T) {
	assertSerialize(t, DRANDOM(), "DRANDOM()")
}

// === Date/Time ===

func TestDATEDIFF(t *testing.T) {
	assertSerialize(t, DATEDIFF(table1ColDate, table2ColDate),
		"DATEDIFF(table1.col_date, table2.col_date)")
}

func TestADDDATE(t *testing.T) {
	assertSerialize(t, ADDDATE(table1ColDate, Int(7)),
		"ADDDATE(table1.col_date, ?)", int64(7))
}

func TestSUBDATE(t *testing.T) {
	assertSerialize(t, SUBDATE(table1ColDate, Int(7)),
		"SUBDATE(table1.col_date, ?)", int64(7))
}

func TestADD_MONTHS(t *testing.T) {
	assertSerialize(t, ADD_MONTHS(table1ColDate, Int(3)),
		"ADD_MONTHS(table1.col_date, ?)", int64(3))
}

func TestMONTHS_BETWEEN(t *testing.T) {
	assertSerialize(t, MONTHS_BETWEEN(table1ColDate, table2ColDate),
		"MONTHS_BETWEEN(table1.col_date, table2.col_date)")
}

func TestLAST_DAY(t *testing.T) {
	assertSerialize(t, LAST_DAY(table1ColDate), "LAST_DAY(table1.col_date)")
}

func TestSYS_DATE(t *testing.T) {
	assertSerialize(t, SYS_DATE, "(SYS_DATE)")
}

func TestSYS_TIME(t *testing.T) {
	assertSerialize(t, SYS_TIME, "(SYS_TIME)")
}

func TestSYS_DATETIME(t *testing.T) {
	assertSerialize(t, SYS_DATETIME, "(SYS_DATETIME)")
}

func TestSYS_TIMESTAMP(t *testing.T) {
	assertSerialize(t, SYS_TIMESTAMP, "(SYS_TIMESTAMP)")
}

func TestUTC_DATE(t *testing.T) {
	assertSerialize(t, UTC_DATE, "(UTC_DATE())")
}

func TestUTC_TIME(t *testing.T) {
	assertSerialize(t, UTC_TIME, "(UTC_TIME())")
}

func TestFROM_UNIXTIME(t *testing.T) {
	assertSerialize(t, FROM_UNIXTIME(Int(1234567890)),
		"FROM_UNIXTIME(?)", int64(1234567890))
	assertSerialize(t, FROM_UNIXTIME(Int(1234567890), String("%Y-%m-%d")),
		"FROM_UNIXTIME(?, ?)", int64(1234567890), "%Y-%m-%d")
}

func TestUNIX_TIMESTAMP(t *testing.T) {
	assertSerialize(t, UNIX_TIMESTAMP(), "UNIX_TIMESTAMP()")
	assertSerialize(t, UNIX_TIMESTAMP(table1ColTimestamp),
		"UNIX_TIMESTAMP(table1.col_timestamp)")
}

func TestYEAR_(t *testing.T) {
	assertSerialize(t, YEAR_(table1ColDate), "YEAR(table1.col_date)")
}

func TestMONTH_(t *testing.T) {
	assertSerialize(t, MONTH_(table1ColDate), "MONTH(table1.col_date)")
}

func TestDAY_(t *testing.T) {
	assertSerialize(t, DAY_(table1ColDate), "DAY(table1.col_date)")
}

func TestHOUR_(t *testing.T) {
	assertSerialize(t, HOUR_(table1ColTime), "HOUR(table1.col_time)")
}

func TestMINUTE_(t *testing.T) {
	assertSerialize(t, MINUTE_(table1ColTime), "MINUTE(table1.col_time)")
}

func TestSECOND_(t *testing.T) {
	assertSerialize(t, SECOND_(table1ColTime), "SECOND(table1.col_time)")
}

func TestMAKEDATE(t *testing.T) {
	assertSerialize(t, MAKEDATE(Int(2024), Int(100)),
		"MAKEDATE(?, ?)", int64(2024), int64(100))
}

func TestMAKETIME(t *testing.T) {
	assertSerialize(t, MAKETIME(Int(10), Int(30), Int(45)),
		"MAKETIME(?, ?, ?)", int64(10), int64(30), int64(45))
}

func TestSTR_TO_DATE(t *testing.T) {
	assertSerialize(t, STR_TO_DATE(String("2024-01-15"), String("%Y-%m-%d")),
		"STR_TO_DATE(?, ?)", "2024-01-15", "%Y-%m-%d")
}

func TestDATE_FORMAT(t *testing.T) {
	assertSerialize(t, DATE_FORMAT(table1ColDate, String("%Y-%m-%d")),
		"DATE_FORMAT(table1.col_date, ?)", "%Y-%m-%d")
}

// === Type conversion ===

func TestTO_CHAR(t *testing.T) {
	assertSerialize(t, TO_CHAR(table1ColInt), "TO_CHAR(table1.col_int)")
	assertSerialize(t, TO_CHAR(table1ColDate, String("YYYY-MM-DD")),
		"TO_CHAR(table1.col_date, ?)", "YYYY-MM-DD")
}

func TestTO_DATE(t *testing.T) {
	assertSerialize(t, TO_DATE(String("2024-01-01")),
		"TO_DATE(?)", "2024-01-01")
	assertSerialize(t, TO_DATE(String("01/15/2024"), String("MM/DD/YYYY")),
		"TO_DATE(?, ?)", "01/15/2024", "MM/DD/YYYY")
}

func TestTO_DATETIME(t *testing.T) {
	assertSerialize(t, TO_DATETIME(String("2024-01-01 12:00:00")),
		"TO_DATETIME(?)", "2024-01-01 12:00:00")
}

func TestTO_TIMESTAMP(t *testing.T) {
	assertSerialize(t, TO_TIMESTAMP(String("2024-01-01 12:00:00")),
		"TO_TIMESTAMP(?)", "2024-01-01 12:00:00")
}

func TestTO_NUMBER(t *testing.T) {
	assertSerialize(t, TO_NUMBER(String("123.45")),
		"TO_NUMBER(?)", "123.45")
}

func TestINSTR(t *testing.T) {
	assertSerialize(t, INSTR(table1ColString, String("test")),
		"INSTR(table1.col_string, ?)", "test")
}

// === Information ===

func TestDATABASE_(t *testing.T) {
	assertSerialize(t, DATABASE_(), "DATABASE()")
}

func TestSCHEMA_(t *testing.T) {
	assertSerialize(t, SCHEMA_(), "SCHEMA()")
}

func TestUSER_(t *testing.T) {
	assertSerialize(t, USER_(), "USER()")
}

func TestROW_COUNT(t *testing.T) {
	assertSerialize(t, ROW_COUNT(), "ROW_COUNT()")
}

func TestFOUND_ROWS(t *testing.T) {
	assertSerialize(t, FOUND_ROWS(), "FOUND_ROWS()")
}

func TestLAST_INSERT_ID(t *testing.T) {
	assertSerialize(t, LAST_INSERT_ID(), "LAST_INSERT_ID()")
	assertSerialize(t, LAST_INSERT_ID(Int(100)), "LAST_INSERT_ID(?)", int64(100))
}

// === Collection ===

func TestSETEQ(t *testing.T) {
	assertSerialize(t, SETEQ(table1ColString, table2ColStr),
		"(table1.col_string SETEQ table2.col_str)")
}

func TestSETNEQ(t *testing.T) {
	assertSerialize(t, SETNEQ(table1ColString, table2ColStr),
		"(table1.col_string SETNEQ table2.col_str)")
}

func TestSUBSET(t *testing.T) {
	assertSerialize(t, SUBSET(table1ColString, table2ColStr),
		"(table1.col_string SUBSET table2.col_str)")
}

func TestSUBSETEQ(t *testing.T) {
	assertSerialize(t, SUBSETEQ(table1ColString, table2ColStr),
		"(table1.col_string SUBSETEQ table2.col_str)")
}

func TestSUPERSET(t *testing.T) {
	assertSerialize(t, SUPERSET(table1ColString, table2ColStr),
		"(table1.col_string SUPERSET table2.col_str)")
}

func TestSUPERSETEQ(t *testing.T) {
	assertSerialize(t, SUPERSETEQ(table1ColString, table2ColStr),
		"(table1.col_string SUPERSETEQ table2.col_str)")
}
