package cubrid

import "github.com/go-jet/jet/v2/internal/jet"

// Statement is common interface for all statements
type Statement = jet.Statement

// Rows wraps sql.Rows type with a support for query result mapping
type Rows = jet.Rows

// Projection is interface for all projection types.
type Projection = jet.Projection

// ProjectionList can be used to create conditional constructed projection list.
type ProjectionList = jet.ProjectionList

// ColumnAssigment is interface wrapper around column assigment
type ColumnAssigment = jet.ColumnAssigment

// PrintableStatement is a statement which sql query can be logged
type PrintableStatement = jet.PrintableStatement

// OrderByClause is input for ORDER BY.
type OrderByClause = jet.OrderByClause

// GroupByClause is input for GROUP BY.
type GroupByClause = jet.GroupByClause

// SetLogger sets automatic statement logging
// Deprecated: use SetQueryLogger instead.
var SetLogger = jet.SetLoggerFunc

// SetQueryLogger sets automatic query logging function.
var SetQueryLogger = jet.SetQueryLogger

// QueryInfo contains information about executed query
type QueryInfo = jet.QueryInfo
