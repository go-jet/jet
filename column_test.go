package jet

import "testing"

func TestColumn(t *testing.T) {
	column := newColumn("col", "", nil)
	column.expressionInterfaceImpl.parent = &column

	AssertPostgreClauseSerialize(t, column, "col")
	column.setTableName("table1")
	AssertPostgreClauseSerialize(t, column, "table1.col")
	assertProjectionSerialize(t, &column, `table1.col AS "table1.col"`)
	assertProjectionSerialize(t, column.AS("alias1"), `table1.col AS "alias1"`)
}
