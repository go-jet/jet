package snaker

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSnakeToCamel(t *testing.T) {
	require.Equal(t, SnakeToCamel(""), "")
	require.Equal(t, SnakeToCamel("potato_"), "Potato")
	require.Equal(t, SnakeToCamel("this_has_to_be_uppercased"), "ThisHasToBeUppercased")
	require.Equal(t, SnakeToCamel("this_is_an_id"), "ThisIsAnID")
	require.Equal(t, SnakeToCamel("this_is_an_identifier"), "ThisIsAnIdentifier")
	require.Equal(t, SnakeToCamel("id"), "ID")
	require.Equal(t, SnakeToCamel("oauth_client"), "OAuthClient")
}
