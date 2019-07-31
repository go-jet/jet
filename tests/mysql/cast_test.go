package mysql

import (
	"github.com/go-jet/jet/internal/testutils"
	. "github.com/go-jet/jet/mysql"
	. "github.com/go-jet/jet/tests/.gentestdata/mysql/test_sample/table"
	"gotest.tools/assert"
	"testing"
	"time"
)

func TestCast(t *testing.T) {

	query := SELECT(
		CAST(String("2011-02-02")).AS_DATE().AS("result.date"),
		CAST(String("14:06:10")).AS_TIME().AS("result.time"),
		CAST(String("2011-02-02 14:06:10")).AS_DATETIME().AS("result.datetime"),
		CAST(Int(150)).AS_CHAR().AS("result.char"),
		CAST(Int(5).SUB(Int(10))).AS_SIGNED().AS("result.signed"),
		CAST(Int(5).ADD(Int(10))).AS_UNSIGNED().AS("result.unsigned"),
		CAST(String("Some text")).AS_BINARY().AS("result.binary"),
	).FROM(AllTypes)

	testutils.AssertStatementSql(t, query, `
SELECT CAST(? AS DATE) AS "result.date",
     CAST(? AS TIME) AS "result.time",
     CAST(? AS DATETIME) AS "result.datetime",
     CAST(? AS CHAR) AS "result.char",
     CAST((? - ?) AS SIGNED) AS "result.signed",
     CAST((? + ?) AS UNSIGNED) AS "result.unsigned",
     CAST(? AS BINARY) AS "result.binary"
FROM test_sample.all_types;
`, "2011-02-02", "14:06:10", "2011-02-02 14:06:10", int64(150), int64(5), int64(10), int64(5), int64(10), "Some text")

	type Result struct {
		Date     time.Time
		Time     time.Time
		DateTime time.Time
		Char     string
		Signed   int
		Unsigned int
		Binary   string
	}

	var dest Result

	err := query.Query(db, &dest)

	assert.NilError(t, err)

	assert.DeepEqual(t, dest, Result{
		Date:     *testutils.Date("2011-02-02"),
		Time:     *testutils.TimeWithoutTimeZone("14:06:10"),
		DateTime: *testutils.TimestampWithoutTimeZone("2011-02-02 14:06:10", 0),
		Char:     "150",
		Signed:   -5,
		Unsigned: 15,
		Binary:   "Some text",
	})
}
