package snaker

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSnakeToCamel(t *testing.T) {
	require.Equal(t, SnakeToCamel(""), "")
	require.Equal(t, SnakeToCamel("_", false), "")
	require.Equal(t, SnakeToCamel("potato_"), "Potato")
	require.Equal(t, SnakeToCamel("potato_", false), "potato")
	require.Equal(t, SnakeToCamel("Potato_", false), "potato")
	require.Equal(t, SnakeToCamel("this_has_to_be_uppercased"), "ThisHasToBeUppercased")
	require.Equal(t, SnakeToCamel("this_is_an_id"), "ThisIsAnID")
	require.Equal(t, SnakeToCamel("this_is_an_identifier"), "ThisIsAnIdentifier")
	require.Equal(t, SnakeToCamel("id"), "ID")
	require.Equal(t, SnakeToCamel("oauth_client"), "OAuthClient")
}

func TestCamelToSnake(t *testing.T) {
	require.Equal(t, "", CamelToSnake(""))
	require.Equal(t, "_", CamelToSnake("_"))
	require.Equal(t, "snake_case", CamelToSnake("snake_case"))
	require.Equal(t, "camel_case", CamelToSnake("camelCase"))
	require.Equal(t, "jet_is_cool_as_hell", CamelToSnake("jetIsCoolAsHell"))
	require.Equal(t, "jet_is_cool_as_hell", CamelToSnake("jet_is_cool_as_hell"))
	require.Equal(t, "id", CamelToSnake("ID"))
}
