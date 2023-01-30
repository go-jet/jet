package postgres

import (
	. "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_generated/table"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMutableColumnsExcludeGeneratedColumn(t *testing.T) {
	t.Run("should not have the generated column in mutableColumns", func(t *testing.T) {
		require.Equal(t, 2, len(People.MutableColumns))
		require.Equal(t, People.PeopleName, People.MutableColumns[0])
		require.Equal(t, People.PeopleHeightCm, People.MutableColumns[1])
	})
}
