// Query building functions for expression components
package sqlbuilder

import (
	"strconv"
	"strings"
	"time"
)

type intervalExpression struct {
	expressionInterfaceImpl
	duration time.Duration
	negative bool
}

var intervalSep = ":"

func (c *intervalExpression) Serialize(out *queryData, options ...serializeOption) error {
	hours := c.duration / time.Hour
	minutes := (c.duration % time.Hour) / time.Minute
	sec := (c.duration % time.Minute) / time.Second
	msec := (c.duration % time.Second) / time.Microsecond
	out.WriteString("INTERVAL '")
	if c.negative {
		out.WriteString("-")
	}
	out.WriteString(strconv.FormatInt(int64(hours), 10))
	out.WriteString(intervalSep)
	out.WriteString(strconv.FormatInt(int64(minutes), 10))
	out.WriteString(intervalSep)
	out.WriteString(strconv.FormatInt(int64(sec), 10))
	out.WriteString(intervalSep)
	out.WriteString(strconv.FormatInt(int64(msec), 10))
	out.WriteString("' HOUR_MICROSECOND")

	return nil
}

// Interval returns a representation of duration
// in a form "INTERVAL `hour:min:sec:microsec` HOUR_MICROSECOND"
func Interval(duration time.Duration) Expression {
	negative := false
	if duration < 0 {
		negative = true
		duration = -duration
	}
	return &intervalExpression{
		duration: duration,
		negative: negative,
	}
}

var likeEscaper = strings.NewReplacer("_", "\\_", "%", "\\%")

func EscapeForLike(s string) string {
	return likeEscaper.Replace(s)
}
