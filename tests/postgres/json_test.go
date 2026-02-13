package postgres

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	. "github.com/go-jet/jet/v2/postgres"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/table"
)

type AllTypesJsonRawMessageResult struct {
	JSON     string           `alias:"all_types.json"`
	JsonbPtr *json.RawMessage `alias:"all_types.jsonb_ptr"`
	Jsonb    json.RawMessage  `alias:"all_types.jsonb"`
}

func TestJsonRawMessage(t *testing.T) {
	var dest []AllTypesJsonRawMessageResult

	err := SELECT(AllTypes.JSON, AllTypes.JsonbPtr, AllTypes.Jsonb).
		FROM(AllTypes).
		LIMIT(2).
		QueryContext(ctx, db, &dest)

	require.NoError(t, err)
	require.Len(t, dest, 2)

	require.JSONEq(t, allTypesRow0.JSON, dest[0].JSON)
	require.JSONEq(t, allTypesRow0.Jsonb, string(dest[0].Jsonb))
	require.NotNil(t, dest[0].JsonbPtr)
	require.JSONEq(t, *allTypesRow0.JsonbPtr, string(*dest[0].JsonbPtr))

	require.JSONEq(t, allTypesRow1.JSON, dest[1].JSON)
	require.JSONEq(t, allTypesRow1.Jsonb, string(dest[1].Jsonb))
	require.Nil(t, dest[1].JsonbPtr)
}
