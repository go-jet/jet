package cubrid

import (
	"testing"
)

func TestVALUES_InInsert(t *testing.T) {
	v := VALUES(ROW(Int(1), String("a")), ROW(Int(2), String("b")))
	vt := v.AS("vals", IntegerColumn("id"), StringColumn("name"))

	// Verify VALUES can be used as a table
	_ = vt.AllColumns()
}

func TestVALUES_DefaultColumnNames(t *testing.T) {
	v := VALUES(ROW(Int(1), String("a")), ROW(Int(2), String("b")))
	vt := v.AS("vals")

	assertStatementSql(t,
		SELECT(vt.AllColumns()).FROM(vt), `
SELECT vals.column_0 AS "column_0",
     vals.column_1 AS "column_1"
FROM (
          VALUES ROW(?, ?),
                 ROW(?, ?)
     ) AS vals;
`, int64(1), "a", int64(2), "b")
}
