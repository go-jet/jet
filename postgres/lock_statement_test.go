package postgres

import (
	"testing"
)

func TestLockTable(t *testing.T) {
	assertStatementSql(t, table1.LOCK().IN(LOCK_ACCESS_SHARE), `
LOCK TABLE db.table1 IN ACCESS SHARE MODE;
`)
	assertStatementSql(t, table1.LOCK().IN(LOCK_ROW_SHARE), `
LOCK TABLE db.table1 IN ROW SHARE MODE;
`)
	assertStatementSql(t, table1.LOCK().IN(LOCK_ROW_EXCLUSIVE), `
LOCK TABLE db.table1 IN ROW EXCLUSIVE MODE;
`)
	assertStatementSql(t, table1.LOCK().IN(LOCK_SHARE_UPDATE_EXCLUSIVE), `
LOCK TABLE db.table1 IN SHARE UPDATE EXCLUSIVE MODE;
`)
	assertStatementSql(t, table1.LOCK().IN(LOCK_SHARE), `
LOCK TABLE db.table1 IN SHARE MODE;
`)
	assertStatementSql(t, table1.LOCK().IN(LOCK_SHARE_ROW_EXCLUSIVE), `
LOCK TABLE db.table1 IN SHARE ROW EXCLUSIVE MODE;
`)
	assertStatementSql(t, table1.LOCK().IN(LOCK_EXCLUSIVE), `
LOCK TABLE db.table1 IN EXCLUSIVE MODE;
`)
	assertStatementSql(t, table1.LOCK().IN(LOCK_ACCESS_EXCLUSIVE).NOWAIT(), `
LOCK TABLE db.table1 IN ACCESS EXCLUSIVE MODE NOWAIT;
`)
}
