package mysql

import "testing"

func TestLockRead(t *testing.T) {
	assertStatementSql(t, table2.LOCK().READ(), `
LOCK TABLES db.table2 READ;
`)
}

func TestLockWrite(t *testing.T) {
	assertStatementSql(t, table2.LOCK().WRITE(), `
LOCK TABLES db.table2 WRITE;
`)
}

func TestUNLOCK_TABLES(t *testing.T) {
	assertStatementSql(t, UNLOCK_TABLES(), `
UNLOCK TABLES;
`)
}
