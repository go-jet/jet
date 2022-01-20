package jet

import (
	"context"
	"runtime"
	"strings"
	"time"
)

// PrintableStatement is a statement which sql query can be logged
type PrintableStatement interface {
	Sql() (query string, args []interface{})
	DebugSql() (query string)
}

// LoggerFunc is a function user can implement to support automatic statement logging.
type LoggerFunc func(ctx context.Context, statement PrintableStatement)

var logger LoggerFunc

// SetLoggerFunc sets automatic statement logging
func SetLoggerFunc(loggerFunc LoggerFunc) {
	logger = loggerFunc
}

func callLogger(ctx context.Context, statement Statement) {
	if logger != nil {
		logger(ctx, statement)
	}
}

// QueryInfo contains information about executed query
type QueryInfo struct {
	Statement PrintableStatement
	// Depending on how the statement is executed, RowsProcessed is:
	// 	- Number of rows returned for Query() and QueryContext() methods
	// 	- RowsAffected() for Exec() and ExecContext() methods
	// 	- Always 0 for Rows() method.
	RowsProcessed int64
	Duration      time.Duration
	Err           error
}

// QueryLoggerFunc is a function user can implement to retrieve more information about statement executed.
type QueryLoggerFunc func(ctx context.Context, info QueryInfo)

var queryLoggerFunc QueryLoggerFunc

// SetQueryLogger sets automatic query logging function.
func SetQueryLogger(loggerFunc QueryLoggerFunc) {
	queryLoggerFunc = loggerFunc
}

func callQueryLoggerFunc(ctx context.Context, info QueryInfo) {
	if queryLoggerFunc != nil {
		queryLoggerFunc(ctx, info)
	}
}

// Caller returns information about statement caller
func (q QueryInfo) Caller() (file string, line int, function string) {
	skip := 4
	// depending on execution type (Query, QueryContext, Exec, ...) looped once or twice
	for {
		var pc uintptr
		var ok bool

		pc, file, line, ok = runtime.Caller(skip)
		if !ok {
			return
		}

		funcDetails := runtime.FuncForPC(pc)
		if !strings.Contains(funcDetails.Name(), "github.com/go-jet/jet/v2/internal") {
			function = funcDetails.Name()
			return
		}

		skip++
	}
}
