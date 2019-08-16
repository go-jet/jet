package mysql

import (
	"github.com/go-jet/jet/internal/testutils"
	. "github.com/go-jet/jet/mysql"
	. "github.com/go-jet/jet/tests/.gentestdata/mysql/dvds/table"
	"gotest.tools/assert"
	"testing"
)

func TestLockRead(t *testing.T) {
	query := Customer.LOCK().READ()

	testutils.AssertStatementSql(t, query, `
LOCK TABLES dvds.customer READ;
`)

	_, err := query.Exec(db)
	assert.NilError(t, err)
}

func TestLockWrite(t *testing.T) {
	query := Customer.LOCK().WRITE()

	testutils.AssertStatementSql(t, query, `
LOCK TABLES dvds.customer WRITE;
`)

	_, err := query.Exec(db)
	assert.NilError(t, err)
}

func TestUnlockTables(t *testing.T) {
	query := UNLOCK_TABLES()

	testutils.AssertStatementSql(t, query, `
UNLOCK TABLES;
`)

	_, err := query.Exec(db)
	assert.NilError(t, err)
}
