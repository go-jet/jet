package postgres

import "github.com/go-jet/jet/v2/internal/jet"

// SelectTable is interface for postgres temporary tables like sub-queries, VALUES, CTEs etc...
type SelectTable interface {
	readableTable
	jet.SelectTable
}

type selectTableImpl struct {
	jet.SelectTable
	readableTableInterfaceImpl
}

func newSelectTable(serializerWithProjections jet.SerializerHasProjections, alias string, columnAliases []jet.ColumnExpression) SelectTable {
	subQuery := &selectTableImpl{
		SelectTable: jet.NewSelectTable(serializerWithProjections, alias, columnAliases),
	}

	subQuery.readableTableInterfaceImpl.parent = subQuery

	return subQuery
}
