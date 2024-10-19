package internal

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/go-jet/jet/v2/internal/utils/min"
	"reflect"
	"strconv"
	"time"
)

var (
	castOverFlowError = fmt.Errorf("cannot cast a negative value to an unsigned value, buffer overflow error")
)

// NullBool struct
type NullBool struct {
	sql.NullBool
}

// Scan implements the Scanner interface.
func (nb *NullBool) Scan(value interface{}) error {
	switch v := value.(type) {
	case bool:
		nb.Bool, nb.Valid = v, true
	case int8, int16, int32, int64, int:
		intVal := reflect.ValueOf(v).Int()

		if intVal != 0 && intVal != 1 {
			return fmt.Errorf("can't assign %T(%d) to bool", value, value)
		}

		nb.Bool = intVal == 1
		nb.Valid = true
	case uint8, uint16, uint32, uint64, uint:
		uintVal := reflect.ValueOf(v).Uint()

		if uintVal != 0 && uintVal != 1 {
			return fmt.Errorf("can't assign %T(%d) to bool", value, value)
		}

		nb.Bool = uintVal == 1
		nb.Valid = true
	default:
		return nb.NullBool.Scan(value)
	}

	return nil
}

// NullTime struct
type NullTime struct {
	sql.NullTime
}

// Scan implements the Scanner interface.
func (nt *NullTime) Scan(value interface{}) error {
	err := nt.NullTime.Scan(value)

	if err == nil {
		return nil
	}

	// Some of the drivers (pgx, mysql) are not parsing all of the time formats(date, time with time zone,...) and are just forwarding string value.
	// At this point we try to parse those values using some of the predefined formats
	nt.Time, nt.Valid = tryParseAsTime(value)

	if !nt.Valid {
		return fmt.Errorf("can't scan time.Time from %q", value)
	}

	return nil
}

var formats = []string{
	"2006-01-02 15:04:05-07:00",  // sqlite
	"2006-01-02 15:04:05.999999", // go-sql-driver/mysql
	"15:04:05-07",                // pgx
	"15:04:05.999999",            // pgx
}

func tryParseAsTime(value interface{}) (time.Time, bool) {

	var timeStr string

	switch v := value.(type) {
	case string:
		timeStr = v
	case []byte:
		timeStr = string(v)
	case int64:
		return time.Unix(v, 0), true // sqlite
	default:
		return time.Time{}, false
	}

	for _, format := range formats {
		formatLen := min.Int(len(format), len(timeStr))
		t, err := time.Parse(format[:formatLen], timeStr)

		if err != nil {
			continue
		}

		return t, true
	}

	return time.Time{}, false
}

// NullUInt64 struct
type NullUInt64 struct {
	UInt64 uint64
	Valid  bool
}

// Scan implements the Scanner interface.
func (n *NullUInt64) Scan(value interface{}) error {
	var stringValue string
	switch v := value.(type) {
	case nil:
		n.Valid = false
		return nil
	case int64:
		if v < 0 {
			return castOverFlowError
		}
		n.UInt64, n.Valid = uint64(v), true
		return nil
	case int32:
		if v < 0 {
			return castOverFlowError
		}
		n.UInt64, n.Valid = uint64(v), true
		return nil
	case int16:
		if v < 0 {
			return castOverFlowError
		}
		n.UInt64, n.Valid = uint64(v), true
		return nil
	case int8:
		if v < 0 {
			return castOverFlowError
		}
		n.UInt64, n.Valid = uint64(v), true
		return nil
	case int:
		if v < 0 {
			return castOverFlowError
		}
		n.UInt64, n.Valid = uint64(v), true
		return nil
	case uint64:
		n.UInt64, n.Valid = v, true
		return nil
	case uint32:
		n.UInt64, n.Valid = uint64(v), true
		return nil
	case uint16:
		n.UInt64, n.Valid = uint64(v), true
		return nil
	case uint8:
		n.UInt64, n.Valid = uint64(v), true
		return nil
	case uint:
		n.UInt64, n.Valid = uint64(v), true
		return nil
	case []byte:
		stringValue = string(v)
	case string:
		stringValue = v
	default:
		return fmt.Errorf("can't scan uint64 from %v", value)
	}

	uintV, err := strconv.ParseUint(stringValue, 10, 64)
	if err != nil {
		return err
	}
	n.UInt64 = uintV
	n.Valid = true

	return nil
}

// Value implements the driver Valuer interface.
func (n NullUInt64) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.UInt64, nil
}
