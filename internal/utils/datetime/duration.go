package datetime

import "time"

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
