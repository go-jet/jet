package cubrid

import (
	"testing"
)

func TestAND(t *testing.T) {
	assertSerialize(t, AND(table1ColBool.EQ(Bool(true)), table2ColBool.EQ(Bool(false))),
		"(\n    (table1.col_bool = ?)\n        AND (table2.col_bool = ?)\n)", true, false)
}

func TestOR(t *testing.T) {
	assertSerialize(t, OR(table1ColBool.EQ(Bool(true)), table2ColBool.EQ(Bool(false))),
		"(\n    (table1.col_bool = ?)\n        OR (table2.col_bool = ?)\n)", true, false)
}

func TestROW(t *testing.T) {
	assertSerialize(t, ROW(Int(1), String("two")), "ROW(?, ?)", int64(1), "two")
}

func TestABSf(t *testing.T) {
	assertSerialize(t, ABSf(table1ColFloat), "ABS(table1.col_float)")
}

func TestABSi(t *testing.T) {
	assertSerialize(t, ABSi(table1ColInt), "ABS(table1.col_int)")
}

func TestSQRT(t *testing.T) {
	assertSerialize(t, SQRT(table1ColFloat), "SQRT(table1.col_float)")
}

func TestCEIL(t *testing.T) {
	assertSerialize(t, CEIL(table1ColFloat), "CEIL(table1.col_float)")
}

func TestFLOOR(t *testing.T) {
	assertSerialize(t, FLOOR(table1ColFloat), "FLOOR(table1.col_float)")
}

func TestROUND(t *testing.T) {
	assertSerialize(t, ROUND(table1ColFloat, Int(2)), "ROUND(table1.col_float, ?)", int64(2))
}

func TestCOUNT(t *testing.T) {
	assertSerialize(t, COUNT(table1ColInt), "COUNT(table1.col_int)")
	assertSerialize(t, COUNT(STAR), "COUNT(*)")
}

func TestMAX(t *testing.T) {
	assertSerialize(t, MAX(table1ColInt), "MAX(table1.col_int)")
}

func TestMIN(t *testing.T) {
	assertSerialize(t, MIN(table1ColInt), "MIN(table1.col_int)")
}

func TestSUM(t *testing.T) {
	assertSerialize(t, SUM(table1ColInt), "SUM(table1.col_int)")
}

func TestAVG(t *testing.T) {
	assertSerialize(t, AVG(table1ColFloat), "AVG(table1.col_float)")
}

func TestLOWER(t *testing.T) {
	assertSerialize(t, LOWER(table1ColString), "LOWER(table1.col_string)")
}

func TestUPPER(t *testing.T) {
	assertSerialize(t, UPPER(table1ColString), "UPPER(table1.col_string)")
}

func TestLTRIM(t *testing.T) {
	assertSerialize(t, LTRIM(table1ColString), "LTRIM(table1.col_string)")
}

func TestRTRIM(t *testing.T) {
	assertSerialize(t, RTRIM(table1ColString), "RTRIM(table1.col_string)")
}

func TestCONCAT_func(t *testing.T) {
	assertSerialize(t, CONCAT(table1ColString, String("suffix")),
		"CONCAT(table1.col_string, ?)", "suffix")
}

func TestLENGTH(t *testing.T) {
	assertSerialize(t, LENGTH(table1ColString), "LENGTH(table1.col_string)")
}

func TestSUBSTR(t *testing.T) {
	assertSerialize(t, SUBSTR(table1ColString, Int(1), Int(5)),
		"SUBSTR(table1.col_string, ?, ?)", int64(1), int64(5))
}

func TestREPLACE_func(t *testing.T) {
	assertSerialize(t, REPLACE(table1ColString, String("old"), String("new")),
		"REPLACE(table1.col_string, ?, ?)", "old", "new")
}

func TestREVERSE(t *testing.T) {
	assertSerialize(t, REVERSE(table1ColString), "REVERSE(table1.col_string)")
}

func TestCOALESCE(t *testing.T) {
	assertSerialize(t, COALESCE(table1ColInt, Int(0)),
		"COALESCE(table1.col_int, ?)", int64(0))
}

func TestNULLIF(t *testing.T) {
	assertSerialize(t, NULLIF(table1ColInt, Int(0)),
		"NULLIF(table1.col_int, ?)", int64(0))
}

func TestGREATEST(t *testing.T) {
	assertSerialize(t, GREATEST(table1ColInt, Int(10)),
		"GREATEST(table1.col_int, ?)", int64(10))
}

func TestLEAST(t *testing.T) {
	assertSerialize(t, LEAST(table1ColInt, Int(10)),
		"LEAST(table1.col_int, ?)", int64(10))
}

func TestEXTRACT(t *testing.T) {
	assertSerialize(t, EXTRACT(YEAR, table1ColTimestamp), "EXTRACT(YEAR FROM table1.col_timestamp)")
	assertSerialize(t, EXTRACT(MONTH, table1ColDate), "EXTRACT(MONTH FROM table1.col_date)")
	assertSerialize(t, EXTRACT(DAY, table1ColDate), "EXTRACT(DAY FROM table1.col_date)")
	assertSerialize(t, EXTRACT(HOUR, table1ColTime), "EXTRACT(HOUR FROM table1.col_time)")
	assertSerialize(t, EXTRACT(MINUTE, table1ColTime), "EXTRACT(MINUTE FROM table1.col_time)")
	assertSerialize(t, EXTRACT(SECOND, table1ColTime), "EXTRACT(SECOND FROM table1.col_time)")
	assertSerialize(t, EXTRACT(MILLISECOND, table1ColTimestamp), "EXTRACT(MILLISECOND FROM table1.col_timestamp)")
}

func TestROW_NUMBER(t *testing.T) {
	assertSerialize(t, ROW_NUMBER(), "ROW_NUMBER()")
}

func TestRANK(t *testing.T) {
	assertSerialize(t, RANK(), "RANK()")
}

func TestDENSE_RANK(t *testing.T) {
	assertSerialize(t, DENSE_RANK(), "DENSE_RANK()")
}
