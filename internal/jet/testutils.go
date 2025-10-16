package jet

import (
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

var defaultDialect = NewDialect(DialectParams{ // just for tests
	AliasQuoteChar:      '"',
	IdentifierQuoteChar: '"',
	ArgumentPlaceholder: func(ord int) string {
		return "$" + strconv.Itoa(ord)
	},
	ArgumentToString: func(value any) (string, bool) {
		return "", false
	},
})

var (
	table1Col1           = IntegerColumn("col1")
	table1ColInt         = IntegerColumn("col_int")
	table1ColFloat       = FloatColumn("col_float")
	table1Col3           = IntegerColumn("col3")
	table1ColTime        = TimeColumn("col_time")
	table1ColTimez       = TimezColumn("col_timez")
	table1ColTimestamp   = TimestampColumn("col_timestamp")
	table1ColTimestampz  = TimestampzColumn("col_timestampz")
	table1ColBool        = BoolColumn("col_bool")
	table1ColDate        = DateColumn("col_date")
	table1ColRange       = RangeColumn[Int8Expression]("col_range")
	table1ColStringArray = ArrayColumn[StringExpression]("col_array_string")
	table1ColBoolArray   = ArrayColumn[BoolExpression]("col_array_bool")
	table1ColIntArray    = ArrayColumn[IntegerExpression]("col_array_int")
)
var table1 = NewTable("db", "table1", "", table1Col1, table1ColInt, table1ColFloat, table1Col3, table1ColTime, table1ColTimez, table1ColBool, table1ColDate, table1ColRange, table1ColTimestamp, table1ColTimestampz, table1ColStringArray, table1ColBoolArray, table1ColIntArray)

var (
	table2Col3          = IntegerColumn("col3")
	table2Col4          = IntegerColumn("col4")
	table2ColInt        = IntegerColumn("col_int")
	table2ColFloat      = FloatColumn("col_float")
	table2ColStr        = StringColumn("col_str")
	table2ColBool       = BoolColumn("col_bool")
	table2ColTime       = TimeColumn("col_time")
	table2ColTimez      = TimezColumn("col_timez")
	table2ColTimestamp  = TimestampColumn("col_timestamp")
	table2ColTimestampz = TimestampzColumn("col_timestampz")
	table2ColDate       = DateColumn("col_date")
	table2ColRange      = RangeColumn[Int8Expression]("col_range")
	table2ColArray      = ArrayColumn[StringExpression]("col_array_string")
)
var table2 = NewTable("db", "table2", "", table2Col3, table2Col4, table2ColInt, table2ColFloat, table2ColStr, table2ColBool, table2ColTime, table2ColTimez, table2ColDate, table2ColRange, table2ColTimestamp, table2ColTimestampz, table2ColArray)

var (
	table3Col1   = IntegerColumn("col1")
	table3ColInt = IntegerColumn("col_int")
	table3StrCol = StringColumn("col2")
)
var table3 = NewTable("db", "table3", "", table3Col1, table3ColInt, table3StrCol)

func assertClauseSerialize(t *testing.T, clause Serializer, query string, args ...interface{}) {
	out := SQLBuilder{Dialect: defaultDialect}
	clause.serialize(SelectStatementType, &out)

	//fmt.Println(out.Buff.String())

	require.Equal(t, out.Buff.String(), query)
	require.Equal(t, out.Args, args)
}

func assertClauseSerializeErr(t *testing.T, clause Serializer, errString string) {
	defer func() {
		r := recover()
		require.Equal(t, r, errString)
	}()

	out := SQLBuilder{Dialect: defaultDialect}
	clause.serialize(SelectStatementType, &out)
}

func assertClauseDebugSerialize(t *testing.T, clause Serializer, query string, args ...interface{}) {
	out := SQLBuilder{Dialect: defaultDialect, Debug: true}
	clause.serialize(SelectStatementType, &out)

	//fmt.Println(out.Buff.String())

	require.Equal(t, out.Buff.String(), query)
	require.Equal(t, out.Args, args)
}

func assertProjectionSerialize(t *testing.T, projection Projection, query string, args ...interface{}) {
	out := SQLBuilder{Dialect: defaultDialect}
	projection.serializeForProjection(SelectStatementType, &out)

	require.Equal(t, out.Buff.String(), query)
	require.Equal(t, out.Args, args)
}
