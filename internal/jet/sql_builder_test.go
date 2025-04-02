package jet

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestArgToString(t *testing.T) {
	s := &SQLBuilder{Dialect: defaultDialect, Debug: true}

	require.Equal(t, s.argToString(true), "TRUE")
	require.Equal(t, s.argToString(false), "FALSE")

	require.Equal(t, s.argToString(int(-32)), "-32")
	require.Equal(t, s.argToString(uint(32)), "32")
	require.Equal(t, s.argToString(int8(-43)), "-43")
	require.Equal(t, s.argToString(uint8(43)), "43")
	require.Equal(t, s.argToString(int16(-54)), "-54")
	require.Equal(t, s.argToString(uint16(54)), "54")
	require.Equal(t, s.argToString(int32(-65)), "-65")
	require.Equal(t, s.argToString(uint32(65)), "65")
	require.Equal(t, s.argToString(int64(-64)), "-64")
	require.Equal(t, s.argToString(uint64(64)), "64")
	require.Equal(t, s.argToString(float32(2.0)), "2")
	require.Equal(t, s.argToString(float64(1.11)), "1.11")

	require.Equal(t, s.argToString("john"), "'john'")
	require.Equal(t, s.argToString("It's text"), "'It''s text'")
	require.Equal(t, s.argToString([]byte("john")), "'john'")
	require.Equal(t, s.argToString(uuid.MustParse("b68dbff4-a87d-11e9-a7f2-98ded00c39c6")), "'b68dbff4-a87d-11e9-a7f2-98ded00c39c6'")

	time, err := time.Parse("Mon Jan 2 15:04:05 -0700 MST 2006", "Mon Jan 2 15:04:05 -0700 MST 2006")
	require.NoError(t, err)
	require.Equal(t, s.argToString(time), "'2006-01-02 15:04:05-07:00'")

	func() {
		defer func() {
			require.Equal(t, recover().(string), "jet: map[string]bool type can not be used as SQL query parameter")
		}()

		s.argToString(map[string]bool{})
	}()
}

func TestFallTrough(t *testing.T) {
	require.Equal(t, FallTrough([]SerializeOption{ShortName}), []SerializeOption{ShortName})
	require.Equal(t, FallTrough([]SerializeOption{SkipNewLine}), []SerializeOption(nil))
	require.Equal(t, FallTrough([]SerializeOption{ShortName, SkipNewLine}), []SerializeOption{ShortName})
}

func TestShouldQuote(t *testing.T) {
	require.Equal(t, shouldQuoteIdentifier("123"), true)
	require.Equal(t, shouldQuoteIdentifier("123.235"), true)
	require.Equal(t, shouldQuoteIdentifier("abc123"), false)
	require.Equal(t, shouldQuoteIdentifier("abc.123"), true)
	require.Equal(t, shouldQuoteIdentifier("abc_123"), false)
	require.Equal(t, shouldQuoteIdentifier("Abc_123"), true)
	require.Equal(t, shouldQuoteIdentifier("ǄƜĐǶ"), true)
}
