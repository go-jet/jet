package jet

import (
	"testing"
)

func TestArrayExpressionEQ(t *testing.T) {
	assertClauseSerialize(t, table1ColStringArray.EQ(table2ColArray), "(table1.col_array_string = table2.col_array_string)")
}

func TestArrayExpressionNOT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColStringArray.NOT_EQ(table2ColArray), "(table1.col_array_string != table2.col_array_string)")
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
}

func TestArrayExpressionCONTAINED_BY(t *testing.T) {
	assertClauseSerialize(t, table1ColStringArray.IS_CONTAINED_BY(table2ColArray), "(table1.col_array_string <@ table2.col_array_string)")
}

func TestArrayExpressionOVERLAP(t *testing.T) {
	assertClauseSerialize(t, table1ColStringArray.OVERLAP(table2ColArray), "(table1.col_array_string && table2.col_array_string)")
}

func TestArrayExpressionCONCAT(t *testing.T) {
	assertClauseSerialize(t, table1ColStringArray.CONCAT(table2ColArray), "(table1.col_array_string || table2.col_array_string)")
}

func TestArrayExpressionCONCAT_ELEMENT(t *testing.T) {
	assertClauseSerialize(t, table1ColStringArray.CONCAT_ELEMENT(StringExp(table2ColArray.AT(Int(1)))), "(table1.col_array_string || table2.col_array_string[$1])", int64(1))
	assertClauseSerialize(t, table1ColStringArray.CONCAT_ELEMENT(String("x")), "(table1.col_array_string || $1)", "x")
}

func TestArrayExpressionAT(t *testing.T) {
	assertClauseSerialize(t, table1ColStringArray.AT(Int(1)), "table1.col_array_string[$1]", int64(1))
}

func TestCastToArrayElemType(t *testing.T) {
	var _ BoolExpression = CastToArrayElemType[BoolExpression](ARRAY[BoolExpression](), table1Col1)
	var _ IntegerExpression = CastToArrayElemType[IntegerExpression](ARRAY[IntegerExpression](), table1Col1)
	var _ FloatExpression = CastToArrayElemType[FloatExpression](ARRAY[FloatExpression](), table1Col1)
	var _ StringExpression = CastToArrayElemType[StringExpression](ARRAY[StringExpression](), table1Col1)
	var _ BlobExpression = CastToArrayElemType[BlobExpression](ARRAY[BlobExpression](), table1Col1)
	var _ DateExpression = CastToArrayElemType[DateExpression](ARRAY[DateExpression](), table1Col1)
	var _ TimestampExpression = CastToArrayElemType[TimestampExpression](ARRAY[TimestampExpression](), table1Col1)
	var _ TimestampzExpression = CastToArrayElemType[TimestampzExpression](ARRAY[TimestampzExpression](), table1Col1)
	var _ TimeExpression = CastToArrayElemType[TimeExpression](ARRAY[TimeExpression](), table1Col1)
	var _ TimezExpression = CastToArrayElemType[TimezExpression](ARRAY[TimezExpression](), table1Col1)
	var _ IntervalExpression = CastToArrayElemType[IntervalExpression](ARRAY[IntervalExpression](), table1Col1)
}
