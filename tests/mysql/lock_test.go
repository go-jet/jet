package mysql

import (
	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/mysql"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/mysql/dvds/table"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLockRead(t *testing.T) {
	query := Customer.LOCK().READ()

	testutils.AssertStatementSql(t, query, `
LOCK TABLES dvds.customer READ;
`)

	_, err := query.Exec(db)
	require.NoError(t, err)
	requireLogged(t, query)
}

func TestLockWrite(t *testing.T) {
	query := Customer.LOCK().WRITE()

	testutils.AssertStatementSql(t, query, `
LOCK TABLES dvds.customer WRITE;
`)

	_, err := query.Exec(db)
	require.NoError(t, err)
	requireLogged(t, query)
}

func TestUnlockTables(t *testing.T) {
	query := UNLOCK_TABLES()

	testutils.AssertStatementSql(t, query, `
UNLOCK TABLES;
`)

	_, err := query.Exec(db)
	require.NoError(t, err)
	requireLogged(t, query)
}
