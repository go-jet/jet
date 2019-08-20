package testutils

import (
	"strings"
	"time"
)

// Date creates time from t string
func Date(t string) *time.Time {
	newTime, err := time.Parse("2006-01-02", t)

	if err != nil {
		panic(err)
	}

	return &newTime
}

// TimestampWithoutTimeZone creates time from t
func TimestampWithoutTimeZone(t string, precision int) *time.Time {

	precisionStr := ""

	if precision > 0 {
		precisionStr = "." + strings.Repeat("9", precision)
	}

	newTime, err := time.Parse("2006-01-02 15:04:05"+precisionStr+" +0000", t+" +0000")

	if err != nil {
		panic(err)
	}

	return &newTime
}

// TimeWithoutTimeZone creates time from t
func TimeWithoutTimeZone(t string) *time.Time {
	newTime, err := time.Parse("15:04:05", t)

	if err != nil {
		panic(err)
	}

	return &newTime
}

// TimeWithTimeZone creates time from t
func TimeWithTimeZone(t string) *time.Time {
	newTimez, err := time.Parse("15:04:05 -0700", t)

	if err != nil {
		panic(err)
	}

	return &newTimez
}

// TimestampWithTimeZone creates time from t
func TimestampWithTimeZone(t string, precision int) *time.Time {

	precisionStr := ""

	if precision > 0 {
		precisionStr = "." + strings.Repeat("9", precision)
	}

	newTime, err := time.Parse("2006-01-02 15:04:05"+precisionStr+" -0700 MST", t)

	if err != nil {
		panic(err)
	}

	return &newTime
}
