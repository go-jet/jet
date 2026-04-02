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
