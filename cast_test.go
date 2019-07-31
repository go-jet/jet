package jet

import "testing"

func TestCastAS(t *testing.T) {
	AssertClauseSerialize(t, NewCastImpl(Int(1)).As("boolean"), "CAST(? AS boolean)", int64(1))
	AssertClauseSerialize(t, NewCastImpl(table2Col3).As("real"), "CAST(table2.col3 AS real)")
	AssertClauseSerialize(t, NewCastImpl(table2Col3.ADD(table2Col3)).As("integer"), "CAST((table2.col3 + table2.col3) AS integer)")
}
