package jet

import "testing"

func TestRawExpression(t *testing.T) {
	assertClauseSerialize(t, RAW("current_database()"), "current_database()")
}
