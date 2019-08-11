package mysql

import (
	"testing"
)

func TestDeleteUnconditionally(t *testing.T) {
	assertStatementErr(t, table1.DELETE(), `jet: WHERE clause not set`)
	assertStatementErr(t, table1.DELETE().WHERE(nil), `jet: WHERE clause not set`)
}

func TestDeleteWithWhere(t *testing.T) {
	assertStatement(t, table1.DELETE().WHERE(table1Col1.EQ(Int(1))), `
DELETE FROM db.table1
WHERE table1.col1 = ?;
`, int64(1))
}
