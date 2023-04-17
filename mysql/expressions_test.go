package mysql

import (
	"testing"
	time2 "time"

	"github.com/stretchr/testify/require"
)

func TestRaw(t *testing.T) {
	assertSerialize(t, Raw("current_database()"), "(current_database())")
	assertDebugSerialize(t, Raw("current_database()"), "(current_database())")

	assertSerialize(t, Raw(":first_arg + table.colInt + :second_arg", RawArgs{":first_arg": 11, ":second_arg": 22}),
		"(? + table.colInt + ?)", 11, 22)
	assertDebugSerialize(t, Raw(":first_arg + table.colInt + :second_arg", RawArgs{":first_arg": 11, ":second_arg": 22}),
		"(11 + table.colInt + 22)")

	assertSerialize(t,
		Int(700).ADD(RawInt("#1 + table.colInt + #2", RawArgs{"#1": 11, "#2": 22})),
		"(? + (? + table.colInt + ?))",
		int64(700), 11, 22)
	assertDebugSerialize(t,
		Int(700).ADD(RawInt("#1 + table.colInt + #2", RawArgs{"#1": 11, "#2": 22})),
		"(700 + (11 + table.colInt + 22))")
}

func TestRawDuplicateArguments(t *testing.T) {
	assertSerialize(t, Raw(":arg + table.colInt + :arg", RawArgs{":arg": 11}),
		"(? + table.colInt + ?)", 11, 11)

	assertSerialize(t, Raw("#age + table.colInt + #year + #age + #year + 11", RawArgs{"#age": 11, "#year": 2000}),
		"(? + table.colInt + ? + ? + ? + 11)", 11, 2000, 11, 2000)

	assertSerialize(t, Raw("#1 + all_types.integer + #2 + #1 + #2 + #3 + #4",
		RawArgs{"#1": 11, "#2": 22, "#3": 33, "#4": 44}),
		`(? + all_types.integer + ? + ? + ? + ? + ?)`, 11, 22, 11, 22, 33, 44)
}

func TestRawInvalidArguments(t *testing.T) {
	defer func() {
		r := recover()
		require.Equal(t, "jet: named argument 'first_arg' does not appear in raw query", r)
	}()

	assertSerialize(t, Raw("table.colInt + :second_arg", RawArgs{"first_arg": 11}), "(table.colInt + ?)", 22)
}

func TestRawType(t *testing.T) {
	assertSerialize(t, RawBool("table.colInt < :float", RawArgs{":float": 11.22}).IS_FALSE(),
		"(table.colInt < ?) IS FALSE", 11.22)

	assertSerialize(t, RawFloat("table.colInt + &float", RawArgs{"&float": 11.22}).EQ(Float(3.14)),
		"((table.colInt + ?) = ?)", 11.22, 3.14)
	assertSerialize(t, RawString("table.colStr || str", RawArgs{"str": "doe"}).EQ(String("john doe")),
		"((table.colStr || ?) = ?)", "doe", "john doe")

	time := time2.Now()
	assertSerialize(t, RawTime("table.colTime").EQ(TimeT(time)),
		"((table.colTime) = CAST(? AS TIME))", time)
	assertSerialize(t, RawTimestamp("table.colTimestamp").EQ(TimestampT(time)),
		"((table.colTimestamp) = TIMESTAMP(?))", time)
	assertSerialize(t, RawDate("table.colDate").EQ(DateT(time)),
		"((table.colDate) = CAST(? AS DATE))", time)
}
