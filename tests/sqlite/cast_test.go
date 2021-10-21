package sqlite

import (
	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/sqlite"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCast(t *testing.T) {
	query := SELECT(
		CAST(String("test")).AS("CHARACTER").AS("result.AS1"),
		CAST(Float(11.33)).AS_TEXT().AS("result.text"),
		CAST(String("33.44")).AS_REAL().AS("result.real"),
		CAST(String("33")).AS_INTEGER().AS("result.integer"),
		CAST(String("Blob blob")).AS_BLOB().AS("result.blob"),
	)

	type Result struct {
		As1     string
		Text    string
		Real    float64
		Integer int64
		Blob    []byte
	}

	var dest Result

	err := query.Query(db, &dest)
	require.NoError(t, err)

	testutils.AssertDeepEqual(t, dest, Result{
		As1:     "test",
		Text:    "11.33",
		Real:    33.44,
		Integer: 33,
		Blob:    []byte("Blob blob"),
	})

	requireLogged(t, query)
}
