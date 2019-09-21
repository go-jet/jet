package internal

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"time"
)

//===============================================================//

// NullByteArray struct
type NullByteArray struct {
	ByteArray []byte
	Valid     bool
}

// Scan implements the Scanner interface.
func (nb *NullByteArray) Scan(value interface{}) error {
	switch v := value.(type) {
	case nil:
		nb.Valid = false
		return nil
	case []byte:
		nb.ByteArray = append(v[:0:0], v...)
		nb.Valid = true
		return nil
	default:
		return fmt.Errorf("can't scan []byte from %v", value)
	}
}

// Value implements the driver Valuer interface.
func (nb NullByteArray) Value() (driver.Value, error) {
	if !nb.Valid {
		return nil, nil
	}
	return nb.ByteArray, nil
}

//===============================================================//

// NullTime struct
type NullTime struct {
	Time  time.Time
	Valid bool // Valid is true if Time is not NULL
}

// Scan implements the Scanner interface.
func (nt *NullTime) Scan(value interface{}) (err error) {
	switch v := value.(type) {
	case nil:
		nt.Valid = false
		return
	case time.Time:
		nt.Time, nt.Valid = v, true
		return
	case []byte:
		nt.Time, nt.Valid = parseTime(string(v))
		return
	case string:
		nt.Time, nt.Valid = parseTime(v)
		return
	default:
		return fmt.Errorf("can't scan time.Time from %v", value)
	}
}

// Value implements the driver Valuer interface.
func (nt NullTime) Value() (driver.Value, error) {
	if !nt.Valid {
		return nil, nil
	}
	return nt.Time, nil
}

const formatTime = "2006-01-02 15:04:05.999999"

func parseTime(timeStr string) (t time.Time, valid bool) {

	var format string

	switch len(timeStr) {
	case 8:
		format = formatTime[11:19]
	case 10, 19, 21, 22, 23, 24, 25, 26:
		format = formatTime[:len(timeStr)]
	default:
		return t, false
	}

	t, err := time.Parse(format, timeStr)
	return t, err == nil
}

//===============================================================//

// NullInt8 struct
type NullInt8 struct {
	Int8  int8
	Valid bool
}

// Scan implements the Scanner interface.
func (n *NullInt8) Scan(value interface{}) (err error) {
	switch v := value.(type) {
	case nil:
		n.Valid = false
		return
	case int64:
		n.Int8, n.Valid = int8(v), true
		return
	case int8:
		n.Int8, n.Valid = v, true
		return
	case []byte:
		intV, err := strconv.ParseInt(string(v), 10, 8)
		if err == nil {
			n.Int8, n.Valid = int8(intV), true
		}
		return err
	default:
		return fmt.Errorf("can't scan int8 from %v", value)
	}
}

// Value implements the driver Valuer interface.
func (n NullInt8) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Int8, nil
}

//===============================================================//

// NullInt16 struct
type NullInt16 struct {
	Int16 int16
	Valid bool
}

// Scan implements the Scanner interface.
func (n *NullInt16) Scan(value interface{}) error {

	switch v := value.(type) {
	case nil:
		n.Valid = false
		return nil
	case int64:
		n.Int16, n.Valid = int16(v), true
		return nil
	case int16:
		n.Int16, n.Valid = v, true
		return nil
	case int8:
		n.Int16, n.Valid = int16(v), true
		return nil
	case uint8:
		n.Int16, n.Valid = int16(v), true
		return nil
	case []byte:
		intV, err := strconv.ParseInt(string(v), 10, 16)
		if err == nil {
			n.Int16, n.Valid = int16(intV), true
		}
		return nil
	default:
		return fmt.Errorf("can't scan int16 from %v", value)
	}
}

// Value implements the driver Valuer interface.
func (n NullInt16) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Int16, nil
}

//===============================================================//

// NullInt32 struct
type NullInt32 struct {
	Int32 int32
	Valid bool
}

// Scan implements the Scanner interface.
func (n *NullInt32) Scan(value interface{}) error {
	switch v := value.(type) {
	case nil:
		n.Valid = false
		return nil
	case int64:
		n.Int32, n.Valid = int32(v), true
		return nil
	case int32:
		n.Int32, n.Valid = v, true
		return nil
	case int16:
		n.Int32, n.Valid = int32(v), true
		return nil
	case uint16:
		n.Int32, n.Valid = int32(v), true
		return nil
	case int8:
		n.Int32, n.Valid = int32(v), true
		return nil
	case uint8:
		n.Int32, n.Valid = int32(v), true
		return nil
	case []byte:
		intV, err := strconv.ParseInt(string(v), 10, 32)
		if err == nil {
			n.Int32, n.Valid = int32(intV), true
		}
		return nil
	default:
		return fmt.Errorf("can't scan int32 from %v", value)
	}
}

// Value implements the driver Valuer interface.
func (n NullInt32) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Int32, nil
}

//===============================================================//

// NullFloat32 struct
type NullFloat32 struct {
	Float32 float32
	Valid   bool
}

// Scan implements the Scanner interface.
func (n *NullFloat32) Scan(value interface{}) error {
	switch v := value.(type) {
	case nil:
		n.Valid = false
		return nil
	case float64:
		n.Float32, n.Valid = float32(v), true
		return nil
	case float32:
		n.Float32, n.Valid = v, true
		return nil
	default:
		return fmt.Errorf("can't scan float32 from %v", value)
	}
}

// Value implements the driver Valuer interface.
func (n NullFloat32) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Float32, nil
}
