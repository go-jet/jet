package jet

import "testing"

func TestColumn(t *testing.T) {
	column := NewColumnImpl("col", "", nil)
	column.ExpressionInterfaceImpl.Parent = &column

	assertClauseSerialize(t, column, "col")
	column.setTableName("table1")
	assertClauseSerialize(t, column, "table1.col")
	assertProjectionSerialize(t, &column, `table1.col AS "table1.col"`)
	assertProjectionSerialize(t, column.AS("alias1"), `table1.col AS "alias1"`)
}
