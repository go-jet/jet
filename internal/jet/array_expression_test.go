package jet

import (
	"github.com/lib/pq"
	"testing"
)

func TestArrayExpressionEQ(t *testing.T) {
	assertClauseSerialize(t, table1ColArray.EQ(table2ColArray), "(table1.col_array_string = table2.col_array_string)")
}

func TestArrayExpressionNOT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColArray.NOT_EQ(table2ColArray), "(table1.col_array_string != table2.col_array_string)")
	assertClauseSerialize(t, table1ColArray.NOT_EQ(StringArray([]string{"x"})), "(table1.col_array_string != $1)", pq.StringArray{"x"})
}

func TestArrayExpressionLT(t *testing.T) {
	assertClauseSerialize(t, table1ColArray.LT(table2ColArray), "(table1.col_array_string < table2.col_array_string)")
}

func TestArrayExpressionGT(t *testing.T) {
	assertClauseSerialize(t, table1ColArray.GT(table2ColArray), "(table1.col_array_string > table2.col_array_string)")
}

func TestArrayExpressionLT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColArray.LT_EQ(table2ColArray), "(table1.col_array_string <= table2.col_array_string)")
}

func TestArrayExpressionGT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColArray.GT_EQ(table2ColArray), "(table1.col_array_string >= table2.col_array_string)")
}

func TestArrayExpressionCONTAINS(t *testing.T) {
	assertClauseSerialize(t, table1ColArray.CONTAINS(table2ColArray), "(table1.col_array_string @> table2.col_array_string)")
	assertClauseSerialize(t, table1ColArray.CONTAINS(StringArray([]string{"x"})), "(table1.col_array_string @> $1)", pq.StringArray{"x"})
}

func TestArrayExpressionCONTAINED_BY(t *testing.T) {
	assertClauseSerialize(t, table1ColArray.IS_CONTAINED_BY(table2ColArray), "(table1.col_array_string <@ table2.col_array_string)")
	assertClauseSerialize(t, table1ColArray.IS_CONTAINED_BY(StringArray([]string{"x"})), "(table1.col_array_string <@ $1)", pq.StringArray{"x"})
}

func TestArrayExpressionOVERLAP(t *testing.T) {
	assertClauseSerialize(t, table1ColArray.OVERLAP(table2ColArray), "(table1.col_array_string && table2.col_array_string)")
}

func TestArrayExpressionCONCAT(t *testing.T) {
	assertClauseSerialize(t, table1ColArray.CONCAT(table2ColArray), "(table1.col_array_string || table2.col_array_string)")
	assertClauseSerialize(t, table1ColArray.CONCAT(StringArray([]string{"x"})), "(table1.col_array_string || $1)", pq.StringArray{"x"})
}

func TestArrayExpressionCONCAT_ELEMENT(t *testing.T) {
	assertClauseSerialize(t, table1ColArray.CONCAT_ELEMENT(StringExp(table2ColArray.AT(Int(1)))), "(table1.col_array_string || (table2.col_array_string[$1]))", int64(1))
	assertClauseSerialize(t, table1ColArray.CONCAT_ELEMENT(String("x")), "(table1.col_array_string || $1)", "x")
}

func TestArrayExpressionAT(t *testing.T) {
	assertClauseSerialize(t, table1ColArray.AT(Int(1)), "(table1.col_array_string[$1])", int64(1))
}
