package jet

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOptionalOrDefaultString(t *testing.T) {
	require.Equal(t, OptionalOrDefaultString("default"), "default")
	require.Equal(t, OptionalOrDefaultString("default", "optional"), "optional")
}

func TestOptionalOrDefaultExpression(t *testing.T) {
	defaultExpression := table2ColFloat
	optionalExpression := table1Col1

	require.Equal(t, OptionalOrDefaultExpression(defaultExpression), defaultExpression)
	require.Equal(t, OptionalOrDefaultExpression(defaultExpression, optionalExpression), optionalExpression)
}

func TestJoinAlias(t *testing.T) {
	require.Equal(t, joinAlias("", ""), "")
	require.Equal(t, joinAlias("foo", "bar"), "foo.bar")
	require.Equal(t, joinAlias("foo.*", "bar"), "foo.bar")
	require.Equal(t, joinAlias("", "bar"), "bar")
}
