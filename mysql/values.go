package mysql

import "github.com/go-jet/jet/v2/internal/jet"

type values struct {
	jet.Values
}

// VALUES is a table value constructor that computes a set of one or more rows as a temporary constant table.
// Each row is defined by the ROW constructor, which takes one or more expressions.
//
// Example usage:
//
//	VALUES(
//		ROW(Int32(204), Float32(1.21)),
//		ROW(Int32(207), Float32(1.02)),
//	)
func VALUES(rows ...RowExpression) values {
	return values{Values: jet.Values(rows)}
}

// AS assigns an alias to the temporary VALUES table, allowing it to be referenced
// within SQL FROM clauses, just like a regular table.
// By default, VALUES columns are named `column1`, `column2`, etc... Default column aliasing can be
// overwritten by passing new list of columns.
//
// Example usage:
//
//	VALUES(...).AS("film_values", IntegerColumn("length"), TimestampColumn("update_date"))
func (v values) AS(alias string, columns ...Column) SelectTable {
	return newSelectTable(v, alias, columns)
}
