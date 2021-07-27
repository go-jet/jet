package template

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestToGoEnumValueIdentifier(t *testing.T) {
	require.Equal(t, defaultEnumValueName("enum_name", "enum_value"), "EnumValue")
	require.Equal(t, defaultEnumValueName("NumEnum", "100"), "NumEnum100")
}
