package mysql

import (
	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/mysql"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/mysql/test_sample/table"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestCast(t *testing.T) {

	query := SELECT(
		CAST(String("test")).AS("CHAR CHARACTER SET utf8").AS("result.AS1"),
		CAST(String("2011-02-02")).AS_DATE().AS("result.date1"),
		CAST(String("14:06:10")).AS_TIME().AS("result.time"),
		CAST(String("2011-02-02 14:06:10")).AS_DATETIME().AS("result.datetime"),

		CAST(Int(150)).AS_CHAR().AS("result.char1"),
		CAST(Int(150)).AS_CHAR(30).AS("result.char2"),

		CAST(Int(5).SUB(Int(10))).AS_SIGNED().AS("result.signed"),
		CAST(Int(5).ADD(Int(10))).AS_UNSIGNED().AS("result.unsigned"),
		CAST(String("Some text")).AS_BINARY().AS("result.binary"),
	).FROM(AllTypes)

	testutils.AssertStatementSql(t, query, `
SELECT CAST(? AS CHAR CHARACTER SET utf8) AS "result.AS1",
     CAST(? AS DATE) AS "result.date1",
     CAST(? AS TIME) AS "result.time",
     CAST(? AS DATETIME) AS "result.datetime",
     CAST(? AS CHAR) AS "result.char1",
     CAST(? AS CHAR(30)) AS "result.char2",
     CAST((? - ?) AS SIGNED) AS "result.signed",
     CAST((? + ?) AS UNSIGNED) AS "result.unsigned",
     CAST(? AS BINARY) AS "result.binary"
FROM test_sample.all_types;
`, "test", "2011-02-02", "14:06:10", "2011-02-02 14:06:10", int64(150), int64(150), int64(5),
		int64(10), int64(5), int64(10), "Some text")

	type Result struct {
		As1      string
		Date1    time.Time
		Time     time.Time
		DateTime time.Time
		Char1    string
		Char2    string
		Signed   int
		Unsigned int
		Binary   string
	}

	var dest Result

	err := query.Query(db, &dest)

	require.NoError(t, err)

	testutils.AssertDeepEqual(t, dest, Result{
		As1:      "test",
		Date1:    *testutils.Date("2011-02-02"),
		Time:     *testutils.TimeWithoutTimeZone("14:06:10"),
		DateTime: *testutils.TimestampWithoutTimeZone("2011-02-02 14:06:10", 0),
		Char1:    "150",
		Char2:    "150",
		Signed:   -5,
		Unsigned: 15,
		Binary:   "Some text",
	})

	requireLogged(t, query)
}
