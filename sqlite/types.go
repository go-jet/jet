package sqlite

import "github.com/go-jet/jet/v2/internal/jet"

// Statement is common interface for all statements(SELECT, INSERT, UPDATE, DELETE, LOCK)
type Statement = jet.Statement

// Rows wraps sql.Rows type with a support for query result mapping
type Rows = jet.Rows

// Projection is interface for all projection types. Types that can be part of, for instance SELECT clause.
type Projection = jet.Projection

// ProjectionList can be used to create conditional constructed projection list.
type ProjectionList = jet.ProjectionList

// ColumnAssigment is interface wrapper around column assigment
type ColumnAssigment = jet.ColumnAssigment

// PrintableStatement is a statement which sql query can be logged
type PrintableStatement = jet.PrintableStatement

// OrderByClause is the combination of an expression and the wanted ordering to use as input for ORDER BY.
type OrderByClause = jet.OrderByClause

// GroupByClause interface to use as input for GROUP_BY
type GroupByClause = jet.GroupByClause

// SetLogger sets automatic statement logging.
// Deprecated: use SetQueryLogger instead.
var SetLogger = jet.SetLoggerFunc

// SetQueryLogger sets automatic query logging function.
var SetQueryLogger = jet.SetQueryLogger

// QueryInfo contains information about executed query
type QueryInfo = jet.QueryInfo
