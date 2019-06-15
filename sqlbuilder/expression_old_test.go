// +build disabled

package sqlbuilder

import (
	"bytes"
	"time"

	gc "gopkg.in/check.v1"
)

func (s *ExprSuite) TestInterval(c *gc.C) {
	testTable := []struct {
		interval    time.Duration
		expected    string
		expectedErr error
	}{
		{
			interval: 50 * time.Microsecond,
			expected: "INTERVAL '0:0:0:50' HOUR_MICROSECOND",
		},
		{
			interval: -50 * time.Microsecond,
			expected: "INTERVAL '-0:0:0:50' HOUR_MICROSECOND",
		},
		{
			interval: 50*time.Microsecond + 50*time.Second,
			expected: "INTERVAL '0:0:50:50' HOUR_MICROSECOND",
		},
		{
			interval: 50*time.Microsecond +
				50*time.Second +
				50*time.Minute,
			expected: "INTERVAL '0:50:50:50' HOUR_MICROSECOND",
		},
		{
			interval: 50*time.Microsecond +
				50*time.Second +
				50*time.Minute +
				50*time.Hour,
			expected: "INTERVAL '50:50:50:50' HOUR_MICROSECOND",
		},
		{
			interval: 50 * time.Hour,
			expected: "INTERVAL '50:0:0:0' HOUR_MICROSECOND",
		},
		{
			interval: 50*time.Hour + 50*time.Minute,
			expected: "INTERVAL '50:50:0:0' HOUR_MICROSECOND",
		},
		{
			interval: 50*time.Hour + 50*time.Minute + 50*time.Second,
			expected: "INTERVAL '50:50:50:0' HOUR_MICROSECOND",
		},
		{
			interval: 0,
			expected: "INTERVAL '0:0:0:0' HOUR_MICROSECOND",
		},
		{
			interval: 50 * time.Nanosecond,
			expected: "INTERVAL '0:0:0:0' HOUR_MICROSECOND",
		},
	}
	buf := &bytes.Buffer{}

	for i, tt := range testTable {
		buf.Reset()
		err := Interval(tt.interval).Serialize(buf)
		c.Assert(err, gc.Equals, tt.expectedErr,
			gc.Commentf("experiment #%d", i))
		if err == nil {
			c.Assert(buf.String(), gc.Equals, tt.expected,
				gc.Commentf("experiment #%d", i))
		}
	}
}
