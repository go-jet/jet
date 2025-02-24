package datetime

import (
	//"github.com/go-jet/jet/v2/internal/utils/min"
	"time"
)

// ExtractTimeComponents extracts number of days, hours, minutes, seconds, microseconds from duration
func ExtractTimeComponents(duration time.Duration) (days, hours, minutes, seconds, microseconds int64) {
	days = int64(duration / (24 * time.Hour))
	reminder := duration % (24 * time.Hour)

	hours = int64(reminder / time.Hour)
	reminder = reminder % time.Hour

	minutes = int64(reminder / time.Minute)
	reminder = reminder % time.Minute

	seconds = int64(reminder / time.Second)
	reminder = reminder % time.Second

	microseconds = int64(reminder / time.Microsecond)

	return
}

// TryParseAsTime attempts to parse the provided value as a time using one of the given formats.
//
// The function iterates over the provided formats and tries to parse the value into a time.Time object.
// It returns the parsed time and a boolean indicating whether the parsing was successful.
func TryParseAsTime(value interface{}, formats []string) (time.Time, bool) {

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
		formatLen := min(len(format), len(timeStr))
		t, err := time.Parse(format[:formatLen], timeStr)

		if err != nil {
			continue
		}

		return t, true
	}

	return time.Time{}, false
}
