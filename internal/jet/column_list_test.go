package jet

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestColumnList_SET(t *testing.T) {
	columnList1 := ColumnList{IntegerColumn("id"), StringColumn("Name"), BoolColumn("active")}
	columnList2 := ColumnList{IntegerColumn("id"), StringColumn("Name"), BoolColumn("active")}

	columnList1.SET(columnList2)

	columnList3 := ColumnList{IntegerColumn("id"), StringColumn("Name")}

	require.PanicsWithValue(t, "jet: column list length mismatch: expected 2 columns, got 3", func() {
		columnList3.SET(columnList1)
	})

	columnList4 := ColumnList{IntegerColumn("id"), StringColumn("FullName"), BoolColumn("active")}

	require.PanicsWithValue(t, "jet: column name mismatch at index 1: expected column 'Name', got 'FullName'", func() {
		columnList1.SET(columnList4)
	})
}

func TestColumnList_SET_Serialization(t *testing.T) {
	columnList1 := ColumnList{IntegerColumn("id"), StringColumn("name")}
	columnList2 := ColumnList{IntegerColumn("id"), StringColumn("name")}

	assignment := columnList1.SET(columnList2)
	assertClauseSerialize(t, assignment, "id = id,\nname = name")
}

func TestColumnList_SET_WithExpression(t *testing.T) {
	columnList := ColumnList{IntegerColumn("count")}
	assignment := columnList.SET(Int(100))
	assertClauseSerialize(t, assignment, "(count) = $1", int64(100))
}
