package sqlbuilder

import (
	"gotest.tools/assert"
	"testing"
)

func TestLockSingleTable(t *testing.T) {
	lock := table1.LOCK().IN(LOCK_ROW_SHARE)

	queryStr, _, err := lock.Sql()

	assert.NilError(t, err)
	assert.Equal(t, queryStr, `
LOCK TABLE db.table1 IN ROW SHARE MODE;
`)
}

func TestLockMultipleTable(t *testing.T) {
	lock := LOCK(table2, table1).IN(LOCK_ACCESS_EXCLUSIVE).NOWAIT()

	queryStr, _, err := lock.Sql()

	assert.NilError(t, err)
	assert.Equal(t, queryStr, `
LOCK TABLE db.table2, db.table1 IN ACCESS EXCLUSIVE MODE NOWAIT;
`)
}
