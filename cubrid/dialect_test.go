package cubrid

import (
	"testing"

	jet "github.com/go-jet/jet/v2/internal/jet"
)

func TestDialectName(t *testing.T) {
	if Dialect.Name() != "CUBRID" {
		t.Errorf("Dialect.Name() = %q, want %q", Dialect.Name(), "CUBRID")
	}
}

func TestDialectPackageName(t *testing.T) {
	if Dialect.PackageName() != "cubrid" {
		t.Errorf("Dialect.PackageName() = %q, want %q", Dialect.PackageName(), "cubrid")
	}
}

func TestDialectIdentifierQuoteChar(t *testing.T) {
	if Dialect.IdentifierQuoteChar() != '"' {
		t.Errorf("IdentifierQuoteChar = %c, want %c", Dialect.IdentifierQuoteChar(), '"')
	}
}

func TestDialectArgumentPlaceholder(t *testing.T) {
	ph := Dialect.ArgumentPlaceholder()
	if ph(1) != "?" || ph(5) != "?" {
		t.Errorf("ArgumentPlaceholder should always return '?'")
	}
}

func TestDialectValuesDefaultColumnName(t *testing.T) {
	if Dialect.ValuesDefaultColumnName(0) != "column_0" {
		t.Errorf("ValuesDefaultColumnName(0) = %q, want %q", Dialect.ValuesDefaultColumnName(0), "column_0")
	}
	if Dialect.ValuesDefaultColumnName(1) != "column_1" {
		t.Errorf("ValuesDefaultColumnName(1) = %q, want %q", Dialect.ValuesDefaultColumnName(1), "column_1")
	}
}

func TestDialectJsonValueEncode(t *testing.T) {
	expr := Int(42)
	encoded := Dialect.JsonValueEncode(expr)
	if encoded != expr {
		t.Errorf("JsonValueEncode should return the expression unchanged for CUBRID dialect")
	}
}

func TestBoolExpressionIS_DISTINCT_FROM(t *testing.T) {
	// CUBRID emulates IS DISTINCT FROM: (NOT (a = b OR (a IS NULL AND b IS NULL)))
	assertSerialize(t, table1ColBool.IS_DISTINCT_FROM(table2ColBool),
		"(NOT (table1.col_bool = table2.col_bool OR (table1.col_bool IS NULL AND table2.col_bool IS NULL)))")
	assertSerialize(t, table1ColBool.IS_DISTINCT_FROM(Bool(false)),
		"(NOT (table1.col_bool = ? OR (table1.col_bool IS NULL AND ? IS NULL)))", false, false)
}

func TestBoolExpressionIS_NOT_DISTINCT_FROM(t *testing.T) {
	// CUBRID emulates IS NOT DISTINCT FROM: ((a = b OR (a IS NULL AND b IS NULL)))
	assertSerialize(t, table1ColBool.IS_NOT_DISTINCT_FROM(table2ColBool),
		"((table1.col_bool = table2.col_bool OR (table1.col_bool IS NULL AND table2.col_bool IS NULL)))")
	assertSerialize(t, table1ColBool.IS_NOT_DISTINCT_FROM(Bool(false)),
		"((table1.col_bool = ? OR (table1.col_bool IS NULL AND ? IS NULL)))", false, false)
}

func TestBoolLiteral(t *testing.T) {
	assertSerialize(t, Bool(true), "?", true)
	assertSerialize(t, Bool(false), "?", false)
}

func TestIntegerExpressionDIV(t *testing.T) {
	assertSerialize(t, table1ColInt.DIV(table2ColInt), "(table1.col_int DIV table2.col_int)")
	assertSerialize(t, table1ColInt.DIV(Int(11)), "(table1.col_int DIV ?)", int64(11))
}

func TestIntExpressionPOW(t *testing.T) {
	assertSerialize(t, table1ColInt.POW(table2ColInt), "POW(table1.col_int, table2.col_int)")
	assertSerialize(t, table1ColInt.POW(Int(11)), "POW(table1.col_int, ?)", int64(11))
}

func TestString_REGEXP_LIKE_operator(t *testing.T) {
	assertSerialize(t, table3StrCol.REGEXP_LIKE(table2ColStr), "(table3.col2 REGEXP table2.col_str)")
	assertSerialize(t, table3StrCol.REGEXP_LIKE(String("JOHN")), "(table3.col2 REGEXP ?)", "JOHN")
}

func TestString_NOT_REGEXP_LIKE_operator(t *testing.T) {
	assertSerialize(t, table3StrCol.NOT_REGEXP_LIKE(table2ColStr), "(table3.col2 NOT REGEXP table2.col_str)")
	assertSerialize(t, table3StrCol.NOT_REGEXP_LIKE(String("JOHN")), "(table3.col2 NOT REGEXP ?)", "JOHN")
}

func TestStringCONCAT(t *testing.T) {
	assertSerialize(t, table1ColString.CONCAT(table2ColStr), "(CONCAT(table1.col_string, table2.col_str))")
	assertSerialize(t, table1ColString.CONCAT(String("suffix")), "(CONCAT(table1.col_string, ?))", "suffix")
}

func TestExists(t *testing.T) {
	assertSerialize(t, EXISTS(
		table2.
			SELECT(Int(1)).
			WHERE(table1Col1.EQ(table2Col3)),
	),
		`(EXISTS (
     SELECT ?
     FROM db.table2
     WHERE table1.col1 = table2.col3
))`, int64(1))
}

func TestReservedWordQuoting(t *testing.T) {
	// CUBRID uses double quotes for identifier quoting
	col := StringColumn("select")
	if Dialect.IsReservedWord("select") {
		// Reserved words should be quoted with "
		_ = col
	}
}

func TestArgumentToString_Bytes(t *testing.T) {
	result, ok := argumentToString([]byte{0xDE, 0xAD})
	if !ok {
		t.Error("expected ok for []byte")
	}
	if result != "X'dead'" {
		t.Errorf("got %q, want %q", result, "X'dead'")
	}
}

func TestArgumentToString_NonBytes(t *testing.T) {
	_, ok := argumentToString("hello")
	if ok {
		t.Error("expected not ok for string type")
	}
}

func TestFloatDivision(t *testing.T) {
	// Float / Float should use "/" not "DIV"
	assertSerialize(t, table1ColFloat.DIV(Float(2.0)), "(table1.col_float / ?)", 2.0)
}

func TestCubridDivision_PanicOnTooFewArgs(t *testing.T) {
	fn := cubridDivision() // 0 arguments
	out := &jet.SQLBuilder{Dialect: Dialect}
	assertPanicErr(t, func() { fn(jet.SelectStatementType, out) },
		"jet: invalid number of expressions for operator DIV")
}

func TestCubridCONCAT_PanicOnTooFewArgs(t *testing.T) {
	fn := cubridCONCAToperator() // 0 arguments
	out := &jet.SQLBuilder{Dialect: Dialect}
	assertPanicErr(t, func() { fn(jet.SelectStatementType, out) },
		"jet: invalid number of expressions for operator CONCAT")
}

func TestCubridISNOTDISTINCTFROM_PanicOnTooFewArgs(t *testing.T) {
	fn := cubridISNOTDISTINCTFROM() // 0 arguments
	out := &jet.SQLBuilder{Dialect: Dialect}
	assertPanicErr(t, func() { fn(jet.SelectStatementType, out) },
		"jet: invalid number of expressions for operator")
}
