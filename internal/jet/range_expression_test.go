package jet

import "testing"

func TestRangeExpressionEQ(t *testing.T) {
	assertClauseSerialize(t, table1ColRange.EQ(table2ColRange), "(table1.col_range = table2.col_range)")
	assertClauseSerialize(t, table1ColRange.EQ(Int8Range(Int8(1), Int8(4), String("[)"))), "(table1.col_range = int8range($1, $2, $3))", int8(1), int8(4), "[)")
}

func TestRangeExpressionNOT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColRange.NOT_EQ(table2ColRange), "(table1.col_range != table2.col_range)")
	assertClauseSerialize(t, table1ColRange.NOT_EQ(Int8Range(Int8(1), Int8(4), String("[)"))), "(table1.col_range != int8range($1, $2, $3))", int8(1), int8(4), "[)")
}

func TestRangeExpressionLT(t *testing.T) {
	assertClauseSerialize(t, table1ColRange.LT(table2ColRange), "(table1.col_range < table2.col_range)")
	assertClauseSerialize(t, table1ColRange.LT(Int8Range(Int8(1), Int8(4), String("[)"))), "(table1.col_range < int8range($1, $2, $3))", int8(1), int8(4), "[)")
}

func TestRangeExpressionLT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColRange.LT_EQ(table2ColRange), "(table1.col_range <= table2.col_range)")
	assertClauseSerialize(t, table1ColRange.LT_EQ(Int8Range(Int8(1), Int8(4), String("[)"))), "(table1.col_range <= int8range($1, $2, $3))", int8(1), int8(4), "[)")
}

func TestRangeExpressionGT(t *testing.T) {
	assertClauseSerialize(t, table1ColRange.GT(table2ColRange), "(table1.col_range > table2.col_range)")
	assertClauseSerialize(t, table1ColRange.GT(Int8Range(Int8(1), Int8(4), String("[)"))), "(table1.col_range > int8range($1, $2, $3))", int8(1), int8(4), "[)")
}

func TestRangeExpressionGT_EQ(t *testing.T) {
	assertClauseSerialize(t, table1ColRange.GT_EQ(table2ColRange), "(table1.col_range >= table2.col_range)")
	assertClauseSerialize(t, table1ColRange.GT_EQ(Int8Range(Int8(1), Int8(4), String("[)"))), "(table1.col_range >= int8range($1, $2, $3))", int8(1), int8(4), "[)")
}

func TestRangeExpressionCONTAINS_RANGE(t *testing.T) {
	assertClauseSerialize(t, table1ColRange.CONTAINS_RANGE(table2ColRange), "(table1.col_range @> table2.col_range)")
	assertClauseSerialize(t, table1ColRange.CONTAINS_RANGE(Int8Range(Int8(1), Int8(4), String("[)"))), "(table1.col_range @> int8range($1, $2, $3))", int8(1), int8(4), "[)")
}

func TestRangeExpressionCONTAINS(t *testing.T) {
	assertClauseSerialize(t, table1ColRange.CONTAINS(table2Col3), "(table1.col_range @> table2.col3)")
	assertClauseSerialize(t, table1ColRange.CONTAINS(Int8(1)), "(table1.col_range @> $1)", int8(1))
}

func TestRangeExpressionOVERLAP(t *testing.T) {
	assertClauseSerialize(t, table1ColRange.OVERLAP(table2ColRange), "(table1.col_range && table2.col_range)")
	assertClauseSerialize(t, table1ColRange.OVERLAP(Int8Range(Int8(1), Int8(4), String("[)"))), "(table1.col_range && int8range($1, $2, $3))", int8(1), int8(4), "[)")
}

func TestRangeExpressionUNION(t *testing.T) {
	assertClauseSerialize(t, table1ColRange.UNION(table2ColRange), "(table1.col_range + table2.col_range)")
	assertClauseSerialize(t, table1ColRange.UNION(Int8Range(Int8(1), Int8(4), String("[)"))), "(table1.col_range + int8range($1, $2, $3))", int8(1), int8(4), "[)")
}

func TestRangeExpressionINTERSECTION(t *testing.T) {
	assertClauseSerialize(t, table1ColRange.INTERSECTION(table2ColRange), "(table1.col_range * table2.col_range)")
	assertClauseSerialize(t, table1ColRange.INTERSECTION(Int8Range(Int8(1), Int8(4), String("[)"))), "(table1.col_range * int8range($1, $2, $3))", int8(1), int8(4), "[)")
}

func TestRangeExpressionDIFFERENCE(t *testing.T) {
	assertClauseSerialize(t, table1ColRange.DIFFERENCE(table2ColRange), "(table1.col_range - table2.col_range)")
	assertClauseSerialize(t, table1ColRange.DIFFERENCE(Int8Range(Int8(1), Int8(4), String("[)"))), "(table1.col_range - int8range($1, $2, $3))", int8(1), int8(4), "[)")
}
