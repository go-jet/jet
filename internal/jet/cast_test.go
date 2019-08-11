package jet

import (
	"testing"
)

func TestCastAS(t *testing.T) {
	assertClauseSerialize(t, NewCastImpl(Int(1)).AS("boolean"), "CAST($1 AS boolean)", int64(1))
	assertClauseSerialize(t, NewCastImpl(table2Col3).AS("real"), "CAST(table2.col3 AS real)")
	assertClauseSerialize(t, NewCastImpl(table2Col3.ADD(table2Col3)).AS("integer"), "CAST((table2.col3 + table2.col3) AS integer)")
}
