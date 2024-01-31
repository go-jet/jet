package postgres

import "github.com/go-jet/jet/v2/internal/jet"

const (
	// DEFAULT is jet equivalent of SQL DEFAULT
	DEFAULT = jet.DEFAULT
)

var (
	// NULL is jet equivalent of SQL NULL
	NULL = jet.NULL
	// STAR is jet equivalent of SQL *
	STAR = jet.STAR
	// PLUS_INFINITY is jet equivalent for sql infinity
	PLUS_INFINITY = jet.PLUS_INFINITY
	// MINUS_INFINITY is jet equivalent for sql -infinity
	MINUS_INFINITY = jet.MINUS_INFINITY
)
