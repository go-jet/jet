package pq

// Copyright (c) 2011-2013, 'pq' Contributors Portions Copyright (C) 2011 Blake Mizerany

import (
	"testing"
	"time"
)

var formatTimeTests = []struct {
	time     time.Time
	expected string
}{
	{time.Time{}, "0001-01-01 00:00:00Z"},
	{time.Date(2001, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", 0)), "2001-02-03 04:05:06.123456789Z"},
	{time.Date(2001, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", 2*60*60)), "2001-02-03 04:05:06.123456789+02:00"},
	{time.Date(2001, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", -6*60*60)), "2001-02-03 04:05:06.123456789-06:00"},
	{time.Date(2001, time.February, 3, 4, 5, 6, 0, time.FixedZone("", -(7*60*60+30*60+9))), "2001-02-03 04:05:06-07:30:09"},

	{time.Date(1, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", 0)), "0001-02-03 04:05:06.123456789Z"},
	{time.Date(1, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", 2*60*60)), "0001-02-03 04:05:06.123456789+02:00"},
	{time.Date(1, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", -6*60*60)), "0001-02-03 04:05:06.123456789-06:00"},

	{time.Date(0, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", 0)), "0001-02-03 04:05:06.123456789Z BC"},
	{time.Date(0, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", 2*60*60)), "0001-02-03 04:05:06.123456789+02:00 BC"},
	{time.Date(0, time.February, 3, 4, 5, 6, 123456789, time.FixedZone("", -6*60*60)), "0001-02-03 04:05:06.123456789-06:00 BC"},

	{time.Date(1, time.February, 3, 4, 5, 6, 0, time.FixedZone("", -(7*60*60+30*60+9))), "0001-02-03 04:05:06-07:30:09"},
	{time.Date(0, time.February, 3, 4, 5, 6, 0, time.FixedZone("", -(7*60*60+30*60+9))), "0001-02-03 04:05:06-07:30:09 BC"},
}

func TestFormatTs(t *testing.T) {
	for i, tt := range formatTimeTests {
		val := string(FormatTimestamp(tt.time))
		if val != tt.expected {
			t.Errorf("%d: incorrect time format %q, want %q", i, val, tt.expected)
		}
	}
}
