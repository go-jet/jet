package sqlbuilder

import "testing"

func TestRawExpression(t *testing.T) {
	assertExpressionSerialize(t, RAW("current_database()"), "current_database()")
}
