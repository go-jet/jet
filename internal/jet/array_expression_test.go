package jet

import (
	"github.com/lib/pq"
	"testing"
)

func TestArrayExpressionEQ(t *testing.T) {
	assertClauseSerialize(t, table1ColStringArray.EQ(table2ColArray), "(table1.col_array_string = table2.col_array_string)")
}

func TestArrayExpressionNOT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColStringArray.NOT_EQ(table2ColArray), "(table1.col_array_string != table2.col_array_string)")
	assertClauseSerialize(t, table1ColStringArray.NOT_EQ(StringArray([]string{"x"})), "(table1.col_array_string != $1)", pq.StringArray{"x"})
}

func TestArrayExpressionLT(t *testing.T) {
	assertClauseSerialize(t, table1ColStringArray.LT(table2ColArray), "(table1.col_array_string < table2.col_array_string)")
}

func TestArrayExpressionGT(t *testing.T) {
	assertClauseSerialize(t, table1ColStringArray.GT(table2ColArray), "(table1.col_array_string > table2.col_array_string)")
}

func TestArrayExpressionLT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColStringArray.LT_EQ(table2ColArray), "(table1.col_array_string <= table2.col_array_string)")
}

func TestArrayExpressionGT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColStringArray.GT_EQ(table2ColArray), "(table1.col_array_string >= table2.col_array_string)")
}

func TestArrayExpressionCONTAINS(t *testing.T) {
	assertClauseSerialize(t, table1ColStringArray.CONTAINS(table2ColArray), "(table1.col_array_string @> table2.col_array_string)")
	assertClauseSerialize(t, table1ColStringArray.CONTAINS(StringArray([]string{"x"})), "(table1.col_array_string @> $1)", pq.StringArray{"x"})
}

func TestArrayExpressionCONTAINED_BY(t *testing.T) {
	assertClauseSerialize(t, table1ColStringArray.IS_CONTAINED_BY(table2ColArray), "(table1.col_array_string <@ table2.col_array_string)")
	assertClauseSerialize(t, table1ColStringArray.IS_CONTAINED_BY(StringArray([]string{"x"})), "(table1.col_array_string <@ $1)", pq.StringArray{"x"})
}

func TestArrayExpressionOVERLAP(t *testing.T) {
	assertClauseSerialize(t, table1ColStringArray.OVERLAP(table2ColArray), "(table1.col_array_string && table2.col_array_string)")
}

func TestArrayExpressionCONCAT(t *testing.T) {
	assertClauseSerialize(t, table1ColStringArray.CONCAT(table2ColArray), "(table1.col_array_string || table2.col_array_string)")
	assertClauseSerialize(t, table1ColStringArray.CONCAT(StringArray([]string{"x"})), "(table1.col_array_string || $1)", pq.StringArray{"x"})
}

func TestArrayExpressionCONCAT_ELEMENT(t *testing.T) {
	assertClauseSerialize(t, table1ColStringArray.CONCAT_ELEMENT(StringExp(table2ColArray.AT(Int(1)))), "(table1.col_array_string || (table2.col_array_string[$1]))", int64(1))
	assertClauseSerialize(t, table1ColStringArray.CONCAT_ELEMENT(String("x")), "(table1.col_array_string || $1)", "x")
}

func TestArrayExpressionAT(t *testing.T) {
	assertClauseSerialize(t, table1ColStringArray.AT(Int(1)), "(table1.col_array_string[$1])", int64(1))
}
