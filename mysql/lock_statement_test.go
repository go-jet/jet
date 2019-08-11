package mysql

import "testing"

func TestLockRead(t *testing.T) {
	assertStatement(t, table2.LOCK().READ(), `
LOCK TABLES db.table2 READ;
`)
}

func TestLockWrite(t *testing.T) {
	assertStatement(t, table2.LOCK().WRITE(), `
LOCK TABLES db.table2 WRITE;
`)
}

func TestUNLOCK_TABLES(t *testing.T) {
	assertStatement(t, UNLOCK_TABLES(), `
UNLOCK TABLES;
`)
}
