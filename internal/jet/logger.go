package jet

import "context"

// PrintableStatement is a statement which sql query can be logged
type PrintableStatement interface {
	Sql() (query string, args []interface{})
	DebugSql() (query string)
}

// LoggerFunc is a definition of a function user can implement to support automatic statement logging.
type LoggerFunc func(ctx context.Context, statement PrintableStatement)

var logger LoggerFunc

// SetLoggerFunc sets automatic statement logging
func SetLoggerFunc(loggerFunc LoggerFunc) {
	logger = loggerFunc
}
