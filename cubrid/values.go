package cubrid

import "github.com/go-jet/jet/v2/internal/jet"

type values struct{ jet.Values }

// VALUES is a table value constructor.
func VALUES(rows ...RowExpression) values { return values{Values: jet.Values(rows)} }

// AS assigns an alias to the temporary VALUES table.
func (v values) AS(alias string, columns ...Column) SelectTable {
	return newSelectTable(v, alias, columns)
}
