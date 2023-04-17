package postgres

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRaw(t *testing.T) {
	assertSerialize(t, Raw("current_database()"), "(current_database())")
	assertDebugSerialize(t, Raw("current_database()"), "(current_database())")

	assertSerialize(t, Raw(":first_arg + table.colInt + :second_arg", RawArgs{":first_arg": 11, ":second_arg": 22}),
		"($1 + table.colInt + $2)", 11, 22)
	assertDebugSerialize(t, Raw(":first_arg + table.colInt + :second_arg", RawArgs{":first_arg": 11, ":second_arg": 22}),
		"(11 + table.colInt + 22)")

	assertSerialize(t,
		Int(700).ADD(RawInt(":first_arg + table.colInt + :second_arg", RawArgs{":first_arg": 11, ":second_arg": 22})),
		"($1 + ($2 + table.colInt + $3))",
		int64(700), 11, 22)
	assertDebugSerialize(t,
		Int(700).ADD(RawInt(":first_arg + table.colInt + :second_arg", RawArgs{":first_arg": 11, ":second_arg": 22})),
		"(700 + (11 + table.colInt + 22))")
}

func TestDuplicateArguments(t *testing.T) {
	assertSerialize(t, Raw(":arg + table.colInt + :arg", RawArgs{":arg": 11}),
		"($1 + table.colInt + $1)", 11)
	assertDebugSerialize(t, Raw(":arg + table.colInt + :arg", RawArgs{":arg": 11}),
		"(11 + table.colInt + 11)")

	assertSerialize(t, Raw("#age + table.colInt + #year + #age + #year + 11", RawArgs{"#age": 11, "#year": 2000}),
		"($1 + table.colInt + $2 + $1 + $2 + 11)", 11, 2000)
	assertDebugSerialize(t, Raw("#age + table.colInt + #year + #age + #year + 11", RawArgs{"#age": 11, "#year": 2000}),
		"(11 + table.colInt + 2000 + 11 + 2000 + 11)")

	assertSerialize(t, Raw("#1 + all_types.integer + #2 + #1 + #2 + #3 + #4",
		RawArgs{"#1": 11, "#2": 22, "#3": 33, "#4": 44}),
		`($1 + all_types.integer + $2 + $1 + $2 + $3 + $4)`, 11, 22, 33, 44)
	assertDebugSerialize(t, Raw("#1 + all_types.integer + #2 + #1 + #2 + #3 + #4",
		RawArgs{"#1": 11, "#2": 22, "#3": 33, "#4": 44}),
		`(11 + all_types.integer + 22 + 11 + 22 + 33 + 44)`)
}

func TestRawInvalidArguments(t *testing.T) {
	defer func() {
		r := recover()
		require.Equal(t, "jet: named argument 'first_arg' does not appear in raw query", r)
	}()

	assertSerialize(t, Raw("table.colInt + :second_arg", RawArgs{
		"first_arg":  11,
		"second_arg": 22,
	}), "(table.colInt + $1)", 22)
}

func TestRawHelperMethods(t *testing.T) {
	assertSerialize(t, RawBool("table.colInt < :float", RawArgs{":float": 11.22}).IS_FALSE(),
		"(table.colInt < $1) IS FALSE", 11.22)

	assertSerialize(t, RawFloat("table.colInt + :float", RawArgs{":float": 11.22}).EQ(Float(3.14)),
		"((table.colInt + $1) = $2)", 11.22, 3.14)
	assertSerialize(t, RawString("table.colStr || str", RawArgs{"str": "doe"}).EQ(String("john doe")),
		"((table.colStr || $1) = $2::text)", "doe", "john doe")

	now := time.Now()
	assertSerialize(t, RawTime("table.colTime").EQ(TimeT(now)),
		"((table.colTime) = $1::time without time zone)", now)
	assertSerialize(t, RawTimez("table.colTime").EQ(TimezT(now)),
		"((table.colTime) = $1::time with time zone)", now)
	assertSerialize(t, RawTimestamp("table.colTimestamp").EQ(TimestampT(now)),
		"((table.colTimestamp) = $1::timestamp without time zone)", now)
	assertSerialize(t, RawTimestampz("table.colTimestampz").EQ(TimestampzT(now)),
		"((table.colTimestampz) = $1::timestamp with time zone)", now)
	assertSerialize(t, RawDate("table.colDate").EQ(DateT(now)),
		"((table.colDate) = $1::date)", now)
}
