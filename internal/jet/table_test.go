package jet

import (
	"gotest.tools/assert"
	"testing"
)

func TestNewTable(t *testing.T) {
	newTable := NewTable("schema", "table", IntegerColumn("intCol"))

	assert.Equal(t, newTable.SchemaName(), "schema")
	assert.Equal(t, newTable.TableName(), "table")

	assert.Equal(t, len(newTable.columns()), 1)
	assert.Equal(t, newTable.columns()[0].Name(), "intCol")
}

func TestNewJoinTable(t *testing.T) {
	newTable1 := NewTable("schema", "table", IntegerColumn("intCol1"))
	newTable2 := NewTable("schema", "table2", IntegerColumn("intCol2"))

	joinTable := NewJoinTable(newTable1, newTable2, InnerJoin, IntegerColumn("intCol1").EQ(IntegerColumn("intCol2")))

	assertClauseSerialize(t, joinTable, `schema.table
INNER JOIN schema.table2 ON ("intCol1" = "intCol2")`)

	assert.Equal(t, joinTable.SchemaName(), "schema")
	assert.Equal(t, joinTable.TableName(), "")

	assert.Equal(t, len(joinTable.columns()), 2)
	assert.Equal(t, joinTable.columns()[0].Name(), "intCol1")
	assert.Equal(t, joinTable.columns()[1].Name(), "intCol2")
}
