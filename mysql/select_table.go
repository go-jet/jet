package mysql

import "github.com/go-jet/jet/internal/jet"

type SelectTable interface {
	ReadableTable
	jet.SelectTable
}

type selectTableImpl struct {
	jet.SelectTableImpl2
	readableTableInterfaceImpl
}

func newSelectTable(selectStmt jet.StatementWithProjections, alias string) SelectTable {
	subQuery := &selectTableImpl{
		SelectTableImpl2: jet.NewSelectTable(selectStmt, alias),
	}

	subQuery.readableTableInterfaceImpl.parent = subQuery

	return subQuery
}
