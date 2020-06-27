package testutils

import (
	"github.com/go-jet/jet/v2/internal/utils"
	"strings"
	"time"
)

// Date creates time from t string
func Date(t string) *time.Time {
	newTime, err := time.Parse("2006-01-02", t)

	utils.PanicOnError(err)

	return &newTime
}

// TimestampWithoutTimeZone creates time from t
func TimestampWithoutTimeZone(t string, precision int) *time.Time {

	precisionStr := ""

	if precision > 0 {
		precisionStr = "." + strings.Repeat("9", precision)
	}

	newTime, err := time.Parse("2006-01-02 15:04:05"+precisionStr+" +0000", t+" +0000")

	utils.PanicOnError(err)

	return &newTime
}

// TimeWithoutTimeZone creates time from t
func TimeWithoutTimeZone(t string) *time.Time {
	newTime, err := time.Parse("15:04:05", t)

	utils.PanicOnError(err)

	return &newTime
}

// TimeWithTimeZone creates time from t
func TimeWithTimeZone(t string) *time.Time {
	newTimez, err := time.Parse("15:04:05 -0700", t)

	utils.PanicOnError(err)

	return &newTimez
}

// TimestampWithTimeZone creates time from t
func TimestampWithTimeZone(t string, precision int) *time.Time {

	precisionStr := ""

	if precision > 0 {
		precisionStr = "." + strings.Repeat("9", precision)
	}

	newTime, err := time.Parse("2006-01-02 15:04:05"+precisionStr+" -0700 MST", t)

	utils.PanicOnError(err)

	return &newTime
}
