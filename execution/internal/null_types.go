package internal

import (
	"database/sql/driver"
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
	case []byte:
		nb.ByteArray = append(v[:0:0], v...)
		nb.Valid = true
	default:
		nb.Valid = false
	}
	return nil
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
func (nt *NullTime) Scan(value interface{}) error {
	nt.Time, nt.Valid = value.(time.Time)
	return nil
}

// Value implements the driver Valuer interface.
func (nt NullTime) Value() (driver.Value, error) {
	if !nt.Valid {
		return nil, nil
	}
	return nt.Time, nil
}

//===============================================================//

// NullInt8 struct
type NullInt8 struct {
	Int8  int8
	Valid bool
}

// Scan implements the Scanner interface.
func (n *NullInt8) Scan(value interface{}) error {

	switch v := value.(type) {
	case int64:
		n.Int8, n.Valid = int8(v), true
		return nil
	case int8:
		n.Int8, n.Valid = v, true
		return nil
	case []byte:
		intV, err := strconv.ParseInt(string(v), 10, 8)
		if err == nil {
			n.Int8, n.Valid = int8(intV), true
			return nil
		}
	}

	n.Valid = false

	return nil
}

// Value implements the driver Valuer interface.
func (n NullInt8) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Int8, nil
}

//===============================================================//

// NullUInt8 struct
type NullUInt8 struct {
	Uint8 uint8
	Valid bool
}

// Scan implements the Scanner interface.
func (n *NullUInt8) Scan(value interface{}) error {

	switch v := value.(type) {
	case uint8:
		n.Uint8, n.Valid = v, true
		return nil
	case []byte:
		intV, err := strconv.ParseInt(string(v), 10, 8)
		if err == nil {
			n.Uint8, n.Valid = uint8(intV), true
			return nil
		}
	}

	n.Valid = false

	return nil
}

// Value implements the driver Valuer interface.
func (n NullUInt8) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Uint8, nil
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
			return nil
		}
	}

	n.Valid = false

	return nil
}

// Value implements the driver Valuer interface.
func (n NullInt16) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Int16, nil
}

//===============================================================//

// NullUInt16 struct
type NullUInt16 struct {
	UInt16 uint16
	Valid  bool
}

// Scan implements the Scanner interface.
func (n *NullUInt16) Scan(value interface{}) error {

	switch v := value.(type) {
	case uint16:
		n.UInt16, n.Valid = v, true
		return nil
	case int8:
		n.UInt16, n.Valid = uint16(v), true
		return nil
	case uint8:
		n.UInt16, n.Valid = uint16(v), true
		return nil
	case []byte:
		intV, err := strconv.ParseInt(string(v), 10, 16)
		if err == nil {
			n.UInt16, n.Valid = uint16(intV), true
			return nil
		}
	}

	n.Valid = false

	return nil
}

// Value implements the driver Valuer interface.
func (n NullUInt16) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.UInt16, nil
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
			return nil
		}
	}

	n.Valid = false

	return nil
}

// Value implements the driver Valuer interface.
func (n NullInt32) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Int32, nil
}

//===============================================================//

// NullInt32 struct
type NullUInt32 struct {
	UInt32 uint32
	Valid  bool
}

// Scan implements the Scanner interface.
func (n *NullUInt32) Scan(value interface{}) error {

	switch v := value.(type) {
	case uint32:
		n.UInt32, n.Valid = v, true
		return nil
	case int16:
		n.UInt32, n.Valid = uint32(v), true
		return nil
	case uint16:
		n.UInt32, n.Valid = uint32(v), true
		return nil
	case int8:
		n.UInt32, n.Valid = uint32(v), true
		return nil
	case uint8:
		n.UInt32, n.Valid = uint32(v), true
		return nil
	case []byte:
		intV, err := strconv.ParseInt(string(v), 10, 32)
		if err == nil {
			n.UInt32, n.Valid = uint32(intV), true
			return nil
		}
	}

	n.Valid = false

	return nil
}

// Value implements the driver Valuer interface.
func (n NullUInt32) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.UInt32, nil
}

//===============================================================//

// NullInt32 struct
type NullInt64 struct {
	Int64 int64
	Valid bool
}

// Scan implements the Scanner interface.
func (n *NullInt64) Scan(value interface{}) error {
	switch v := value.(type) {
	case int64:
		n.Int64, n.Valid = v, true
		return nil
	case int32:
		n.Int64, n.Valid = int64(v), true
		return nil
	case uint32:
		n.Int64, n.Valid = int64(v), true
		return nil
	case int16:
		n.Int64, n.Valid = int64(v), true
		return nil
	case uint16:
		n.Int64, n.Valid = int64(v), true
		return nil
	case int8:
		n.Int64, n.Valid = int64(v), true
		return nil
	case uint8:
		n.Int64, n.Valid = int64(v), true
		return nil
	case []byte:
		intV, err := strconv.ParseInt(string(v), 10, 32)
		if err == nil {
			n.Int64, n.Valid = int64(intV), true
			return nil
		}
	}

	n.Valid = false

	return nil
}

// Value implements the driver Valuer interface.
func (n NullInt64) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Int64, nil
}

//===============================================================//

// NullInt32 struct
type NullUInt64 struct {
	UInt64 uint64
	Valid  bool
}

// Scan implements the Scanner interface.
func (n *NullUInt64) Scan(value interface{}) error {
	switch v := value.(type) {
	case uint64:
		n.UInt64, n.Valid = v, true
		return nil
	case int32:
		n.UInt64, n.Valid = uint64(v), true
		return nil
	case uint32:
		n.UInt64, n.Valid = uint64(v), true
		return nil
	case int16:
		n.UInt64, n.Valid = uint64(v), true
		return nil
	case uint16:
		n.UInt64, n.Valid = uint64(v), true
		return nil
	case int8:
		n.UInt64, n.Valid = uint64(v), true
		return nil
	case uint8:
		n.UInt64, n.Valid = uint64(v), true
		return nil
	case []byte:
		intV, err := strconv.ParseInt(string(v), 10, 32)
		if err == nil {
			n.UInt64, n.Valid = uint64(intV), true
			return nil
		}
	}

	n.Valid = false

	return nil
}

// Value implements the driver Valuer interface.
func (n NullUInt64) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.UInt64, nil
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
	case float64:
		n.Float32, n.Valid = float32(v), true
		return nil
	case float32:
		n.Float32, n.Valid = v, true
		return nil
	}

	n.Valid = false

	return nil
}

// Value implements the driver Valuer interface.
func (n NullFloat32) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Float32, nil
}
