package postgres

import "github.com/go-jet/jet/v2/internal/jet"

// SelectTable is interface for MySQL sub-queries
type SelectTable interface {
	readableTable
	jet.SelectTable
}

type selectTableImpl struct {
	jet.SelectTable
	readableTableInterfaceImpl
}

func newSelectTable(selectStmt jet.SerializerStatement, alias string) SelectTable {
	subQuery := &selectTableImpl{
		SelectTable: jet.NewSelectTable(selectStmt, alias),
	}

	subQuery.readableTableInterfaceImpl.parent = subQuery

	return subQuery
}
