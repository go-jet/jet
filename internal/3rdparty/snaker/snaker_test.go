package snaker

import (
	"gotest.tools/assert"
	"testing"
)

func TestSnakeToCamel(t *testing.T) {
	assert.Equal(t, SnakeToCamel(""), "")
	assert.Equal(t, SnakeToCamel("potato_"), "Potato")
	assert.Equal(t, SnakeToCamel("this_has_to_be_uppercased"), "ThisHasToBeUppercased")
	assert.Equal(t, SnakeToCamel("this_is_an_id"), "ThisIsAnID")
	assert.Equal(t, SnakeToCamel("this_is_an_identifier"), "ThisIsAnIdentifier")
	assert.Equal(t, SnakeToCamel("id"), "ID")
	assert.Equal(t, SnakeToCamel("oauth_client"), "OAuthClient")
}
