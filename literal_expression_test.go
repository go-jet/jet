package jet

import "testing"

func TestRawExpression(t *testing.T) {
	assertPostgreClauseSerialize(t, RAW("current_database()"), "current_database()")
}
