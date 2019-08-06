package postgres

import (
	"github.com/go-jet/jet/internal/testutils"
	"testing"
)

func TestSelectLock(t *testing.T) {
	testutils.AssertStatementSql(t, SELECT(table1ColBool).FROM(table1).FOR(UPDATE()), `
SELECT table1.col_bool AS "table1.col_bool"
FROM db.table1
FOR UPDATE;
`)
	testutils.AssertStatementSql(t, SELECT(table1ColBool).FROM(table1).FOR(SHARE().NOWAIT()), `
SELECT table1.col_bool AS "table1.col_bool"
FROM db.table1
FOR SHARE NOWAIT;
`)

	testutils.AssertStatementSql(t, SELECT(table1ColBool).FROM(table1).FOR(KEY_SHARE().NOWAIT()), `
SELECT table1.col_bool AS "table1.col_bool"
FROM db.table1
FOR KEY SHARE NOWAIT;
`)
	testutils.AssertStatementSql(t, SELECT(table1ColBool).FROM(table1).FOR(NO_KEY_UPDATE().SKIP_LOCKED()), `
SELECT table1.col_bool AS "table1.col_bool"
FROM db.table1
FOR NO KEY UPDATE SKIP LOCKED;
`)
}
