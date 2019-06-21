package jet

import (
	"testing"
)

func TestDeleteUnconditionally(t *testing.T) {
	assertStatementErr(t, table1.DELETE(), `deleting without a WHERE clause`)
	assertStatementErr(t, table1.DELETE().WHERE(nil), `deleting without a WHERE clause`)
}

func TestDeleteWithWhere(t *testing.T) {
	assertStatement(t, table1.DELETE().WHERE(table1Col1.EQ(Int(1))), `
DELETE FROM db.table1
WHERE table1.col1 = $1;
`, int64(1))
}
