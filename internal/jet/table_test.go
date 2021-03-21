package jet

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewTable(t *testing.T) {
	newTable := NewTable("schema", "table", "", IntegerColumn("intCol"))

	require.Equal(t, newTable.SchemaName(), "schema")
	require.Equal(t, newTable.TableName(), "table")

	require.Equal(t, len(newTable.columns()), 1)
	require.Equal(t, newTable.columns()[0].Name(), "intCol")
}

func TestNewJoinTable(t *testing.T) {
	newTable1 := NewTable("schema", "table", "", IntegerColumn("intCol1"))
	newTable2 := NewTable("schema", "table2", "", IntegerColumn("intCol2"))

	joinTable := NewJoinTable(newTable1, newTable2, InnerJoin, IntegerColumn("intCol1").EQ(IntegerColumn("intCol2")))

	assertClauseSerialize(t, joinTable, `schema.table
INNER JOIN schema.table2 ON ("intCol1" = "intCol2")`)

	require.Equal(t, joinTable.SchemaName(), "schema")
	require.Equal(t, joinTable.TableName(), "")

	require.Equal(t, len(joinTable.columns()), 2)
	require.Equal(t, joinTable.columns()[0].Name(), "intCol1")
	require.Equal(t, joinTable.columns()[1].Name(), "intCol2")
}
