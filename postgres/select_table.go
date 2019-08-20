package postgres

import "github.com/go-jet/jet/internal/jet"

// SelectTable is interface for MySQL sub-queries
type SelectTable interface {
	readableTable
	jet.SelectTable
}

type selectTableImpl struct {
	jet.SelectTable
	readableTableInterfaceImpl
}

func newSelectTable(selectStmt jet.StatementWithProjections, alias string) SelectTable {
	subQuery := &selectTableImpl{
		SelectTable: jet.NewSelectTable(selectStmt, alias),
	}

	subQuery.readableTableInterfaceImpl.parent = subQuery

	return subQuery
}
