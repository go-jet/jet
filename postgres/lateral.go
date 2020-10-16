package postgres

import "github.com/go-jet/jet/v2/internal/jet"

func LATERAL(selectStmt SelectStatement, alias string) SelectTable {
	subQuery := &selectTableImpl{
		SelectTable: jet.NewLateral(selectStmt, alias),
	}

	subQuery.readableTableInterfaceImpl.parent = subQuery

	return subQuery
}
