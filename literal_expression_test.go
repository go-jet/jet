package jet

import "testing"

func TestRawExpression(t *testing.T) {
	AssertPostgreClauseSerialize(t, RAW("current_database()"), "current_database()")
}
