package sqlbuilder

import (
	"gotest.tools/assert"
	"testing"
)

func TestDeleteUnconditionally(t *testing.T) {
	_, err := table1.Delete().String()
	assert.Assert(t, err != nil)
}

func TestDeleteWithWhere(t *testing.T) {
	sql, err := table1.Delete().WHERE(table1Col1.EqL(1)).String()
	assert.NilError(t, err)

	assert.Equal(t, sql, "DELETE FROM db.table1 WHERE table1.col1 = 1;")
}
