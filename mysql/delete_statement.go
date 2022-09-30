package mysql

import "github.com/go-jet/jet/v2/internal/jet"

// DeleteStatement is interface for MySQL DELETE statement
type DeleteStatement interface {
	Statement

	OPTIMIZER_HINTS(hints ...OptimizerHint) DeleteStatement

	USING(tables ...ReadableTable) DeleteStatement
	WHERE(expression BoolExpression) DeleteStatement
	ORDER_BY(orderByClauses ...OrderByClause) DeleteStatement
	LIMIT(limit int64) DeleteStatement
}

type deleteStatementImpl struct {
	jet.SerializerStatement

	Delete  jet.ClauseDelete
	Using   jet.ClauseFrom
	Where   jet.ClauseWhere
	OrderBy jet.ClauseOrderBy
	Limit   jet.ClauseLimit
}

func newDeleteStatement(table Table) DeleteStatement {
	newDelete := &deleteStatementImpl{}
	newDelete.SerializerStatement = jet.NewStatementImpl(Dialect, jet.DeleteStatementType, newDelete,
		&newDelete.Delete,
		&newDelete.Using,
		&newDelete.Where,
		&newDelete.OrderBy,
		&newDelete.Limit,
	)

	newDelete.Delete.Table = table
	newDelete.Using.Name = "USING"
	newDelete.Where.Mandatory = true
	newDelete.Limit.Count = -1

	return newDelete
}

func (d *deleteStatementImpl) OPTIMIZER_HINTS(hints ...OptimizerHint) DeleteStatement {
	d.Delete.OptimizerHints = hints
	return d
}

func (d *deleteStatementImpl) USING(tables ...ReadableTable) DeleteStatement {
	d.Using.Tables = readableTablesToSerializerList(tables)
	return d
}

func (d *deleteStatementImpl) WHERE(expression BoolExpression) DeleteStatement {
	d.Where.Condition = expression
	return d
}

func (d *deleteStatementImpl) ORDER_BY(orderByClauses ...OrderByClause) DeleteStatement {
	d.OrderBy.List = orderByClauses
	return d
}

func (d *deleteStatementImpl) LIMIT(limit int64) DeleteStatement {
	d.Limit.Count = limit
	return d
}
