package sqlite

import "github.com/go-jet/jet/v2/internal/jet"

// DeleteStatement is interface for MySQL DELETE statement
type DeleteStatement interface {
	Statement

	WHERE(expression BoolExpression) DeleteStatement
	ORDER_BY(orderByClauses ...OrderByClause) DeleteStatement
	LIMIT(limit int64) DeleteStatement
	RETURNING(projections ...Projection) DeleteStatement
}

type deleteStatementImpl struct {
	jet.SerializerStatement

	Delete    jet.ClauseDelete
	Where     jet.ClauseWhere
	OrderBy   jet.ClauseOrderBy
	Limit     jet.ClauseLimit
	Returning jet.ClauseReturning
}

func newDeleteStatement(table Table) DeleteStatement {
	newDelete := &deleteStatementImpl{}
	newDelete.SerializerStatement = jet.NewStatementImpl(Dialect, jet.DeleteStatementType, newDelete,
		&newDelete.Delete,
		&newDelete.Where,
		&newDelete.OrderBy,
		&newDelete.Limit,
		&newDelete.Returning,
	)

	newDelete.Delete.Table = table
	newDelete.Where.Mandatory = true
	newDelete.Limit.Count = -1

	return newDelete
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

func (d *deleteStatementImpl) RETURNING(projections ...jet.Projection) DeleteStatement {
	d.Returning.ProjectionList = projections
	return d
}
