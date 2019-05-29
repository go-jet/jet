package sqlbuilder

import (
	"fmt"
	"gotest.tools/assert"
	"testing"
)

func TestDeleteUnconditionally(t *testing.T) {
	_, _, err := table1.DELETE().Sql()
	assert.Assert(t, err != nil)
}

func TestDeleteWithWhere(t *testing.T) {
	sql, _, err := table1.DELETE().WHERE(table1Col1.EQ(Int(1))).Sql()
	assert.NilError(t, err)

	fmt.Println(sql)
	expectedSql := `
DELETE FROM db.table1
WHERE table1.col1 = $1;
`
	assert.Equal(t, sql, expectedSql)
}
