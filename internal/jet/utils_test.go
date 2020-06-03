package jet

import (
	"github.com/stretchr/testify/require"
	"testing"
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
