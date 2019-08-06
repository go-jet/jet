package jet

import (
	"testing"
	"time"
)

func TestRawExpression(t *testing.T) {
	assertClauseSerialize(t, RAW("current_database()"), "current_database()")

	var timeT = time.Date(2009, 11, 17, 20, 34, 58, 651387237, time.UTC)

	assertClauseSerialize(t, DateT(timeT), "$1", timeT)
}
