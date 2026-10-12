package qrm

import (
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// setRowValue imitates the driver side of a scan: what qrm receives for a
// column is a *interface{} holding the driver value (pgx delivers an SQL
// array column as []any, NULL as a nil interface).
func setRowValue(scanContext *ScanContext, value any) {
	v := value
	scanContext.row[0] = &v
}

func TestMapRowToBaseTypeSliceSQLArray(t *testing.T) {
	t.Run("pgx array column fills plain string slice element-wise", func(t *testing.T) {
		scanContext := newScanContext([]string{"tags"})
		setRowValue(scanContext, []any{"a", "b", "c"})

		destination := []string{"stale"}
		updated, err := mapRowToBaseTypeSlice(scanContext, reflectValueOf(&destination), nil)
		require.NoError(t, err)
		require.True(t, updated)
		require.Equal(t, []string{"a", "b", "c"}, destination)
	})

	t.Run("pgx array column fills int32 slice", func(t *testing.T) {
		scanContext := newScanContext([]string{"numbers"})
		setRowValue(scanContext, []any{int32(1), int32(2)})

		destination := []int32{}
		updated, err := mapRowToBaseTypeSlice(scanContext, reflectValueOf(&destination), nil)
		require.NoError(t, err)
		require.True(t, updated)
		require.Equal(t, []int32{1, 2}, destination)
	})

	t.Run("pgx NULL array elements become zero values in a plain slice", func(t *testing.T) {
		scanContext := newScanContext([]string{"tags"})
		setRowValue(scanContext, []any{"a", nil})

		destination := []string{}
		updated, err := mapRowToBaseTypeSlice(scanContext, reflectValueOf(&destination), nil)
		require.NoError(t, err)
		require.True(t, updated)
		require.Equal(t, []string{"a", ""}, destination)
	})

	t.Run("pgx NULL array elements become nil in a pointer slice", func(t *testing.T) {
		scanContext := newScanContext([]string{"tags"})
		setRowValue(scanContext, []any{"x", nil})

		destination := []*string{}
		updated, err := mapRowToBaseTypeSlice(scanContext, reflectValueOf(&destination), nil)
		require.NoError(t, err)
		require.True(t, updated)
		x := "x"
		require.Equal(t, []*string{&x, nil}, destination)
	})

	t.Run("pgx array of uuid.UUID scans into []uuid.UUID", func(t *testing.T) {
		scanContext := newScanContext([]string{"ids"})
		setRowValue(scanContext, []any{uuid.MustParse("00000000-0000-0000-0000-000000000001"), uuid.MustParse("00000000-0000-0000-0000-000000000002")})

		destination := []uuid.UUID{}
		updated, err := mapRowToBaseTypeSlice(scanContext, reflectValueOf(&destination), nil)
		require.NoError(t, err)
		require.True(t, updated)
		require.Equal(t, []uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000001"), uuid.MustParse("00000000-0000-0000-0000-000000000002")}, destination)
	})

	t.Run("NULL array column leaves the destination untouched", func(t *testing.T) {
		scanContext := newScanContext([]string{"tags"})
		setRowValue(scanContext, nil)

		destination := []string{"unchanged"}
		updated, err := mapRowToBaseTypeSlice(scanContext, reflectValueOf(&destination), nil)
		require.NoError(t, err)
		require.False(t, updated)
		require.Equal(t, []string{"unchanged"}, destination)
	})

	t.Run("scalar column keeps the one-row-per-element behavior", func(t *testing.T) {
		scanContext := newScanContext([]string{"name"})
		setRowValue(scanContext, "single")

		destination := []string{}
		updated, err := mapRowToBaseTypeSlice(scanContext, reflectValueOf(&destination), nil)
		require.NoError(t, err)
		require.True(t, updated)
		require.Equal(t, []string{"single"}, destination)
	})
}

func reflectValueOf(v any) reflect.Value {
	return reflect.ValueOf(v)
}
