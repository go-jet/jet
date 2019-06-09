package sqlbuilder

import (
	"testing"
)

var subQuery = table1.SELECT(table1ColFloat, table1ColInt).AsTable("sub_query")

func TestNewBoolColumn(t *testing.T) {
	boolColumn := BoolColumn("colBool").From(subQuery)
	assertClauseSerialize(t, boolColumn, "sub_query.colBool")
	assertClauseSerialize(t, boolColumn.EQ(Bool(true)), "(sub_query.colBool = $1)", true)
	assertProjectionSerialize(t, boolColumn, `sub_query.colBool AS "sub_query.colBool"`)

	boolColumn2 := table1ColBool.From(subQuery)
	assertClauseSerialize(t, boolColumn2, `sub_query."table1.colBool"`)
	assertClauseSerialize(t, boolColumn2.EQ(Bool(true)), `(sub_query."table1.colBool" = $1)`, true)
	assertProjectionSerialize(t, boolColumn2, `sub_query."table1.colBool" AS "sub_query.table1.colBool"`)
}

func TestNewIntColumn(t *testing.T) {
	intColumn := IntegerColumn("colInt").From(subQuery)
	assertClauseSerialize(t, intColumn, "sub_query.colInt")
	assertClauseSerialize(t, intColumn.EQ(Int(12)), "(sub_query.colInt = $1)", int64(12))
	assertProjectionSerialize(t, intColumn, `sub_query.colInt AS "sub_query.colInt"`)

	intColumn2 := table1ColInt.From(subQuery)
	assertClauseSerialize(t, intColumn2, `sub_query."table1.colInt"`)
	assertClauseSerialize(t, intColumn2.EQ(Int(14)), `(sub_query."table1.colInt" = $1)`, int64(14))
	assertProjectionSerialize(t, intColumn2, `sub_query."table1.colInt" AS "sub_query.table1.colInt"`)

}

func TestNewFloatColumnColumn(t *testing.T) {
	floatColumn := FloatColumn("colFloat").From(subQuery)
	assertClauseSerialize(t, floatColumn, "sub_query.colFloat")
	assertClauseSerialize(t, floatColumn.EQ(Float(1.11)), "(sub_query.colFloat = $1)", float64(1.11))
	assertProjectionSerialize(t, floatColumn, `sub_query.colFloat AS "sub_query.colFloat"`)

	floatColumn2 := table1ColFloat.From(subQuery)
	assertClauseSerialize(t, floatColumn2, `sub_query."table1.colFloat"`)
	assertClauseSerialize(t, floatColumn2.EQ(Float(2.22)), `(sub_query."table1.colFloat" = $1)`, float64(2.22))
	assertProjectionSerialize(t, floatColumn2, `sub_query."table1.colFloat" AS "sub_query.table1.colFloat"`)

}
