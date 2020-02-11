package jet

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOptionalOrDefaultString(t *testing.T) {
	assert.Equal(t, OptionalOrDefaultString("default"), "default")
	assert.Equal(t, OptionalOrDefaultString("default", "optional"), "optional")
}

func TestOptionalOrDefaultExpression(t *testing.T) {
	defaultExpression := table2ColFloat
	optionalExpression := table1Col1

	assert.Equal(t, OptionalOrDefaultExpression(defaultExpression), defaultExpression)
	assert.Equal(t, OptionalOrDefaultExpression(defaultExpression, optionalExpression), optionalExpression)
}
