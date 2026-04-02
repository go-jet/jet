package cubrid

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRawStatement(t *testing.T) {
	stmt := RawStatement("SELECT 1")
	queryStr, _ := stmt.Sql()
	require.Contains(t, queryStr, "SELECT 1")
}
