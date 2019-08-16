package mysql

import "github.com/go-jet/jet/internal/jet"

type DeleteStatement interface {
	Statement

	WHERE(expression BoolExpression) DeleteStatement
	ORDER_BY(orderByClauses ...jet.OrderByClause) DeleteStatement
	LIMIT(limit int64) DeleteStatement
}

type deleteStatementImpl struct {
	jet.StatementImpl

	Delete  jet.ClauseStatementBegin
	Where   jet.ClauseWhere
	OrderBy jet.ClauseOrderBy
	Limit   jet.ClauseLimit
}

func newDeleteStatement(table Table) DeleteStatement {
	newDelete := &deleteStatementImpl{}
	newDelete.StatementImpl = jet.NewStatementImpl(Dialect, jet.DeleteStatementType, newDelete, &newDelete.Delete,
		&newDelete.Where, &newDelete.OrderBy, &newDelete.Limit)

	newDelete.Delete.Name = "DELETE FROM"
	newDelete.Delete.Tables = append(newDelete.Delete.Tables, table)
	newDelete.Where.Mandatory = true
	newDelete.Limit.Count = -1

	return newDelete
}

func (d *deleteStatementImpl) WHERE(expression BoolExpression) DeleteStatement {
	d.Where.Condition = expression
	return d
}

func (s *deleteStatementImpl) ORDER_BY(orderByClauses ...jet.OrderByClause) DeleteStatement {
	s.OrderBy.List = orderByClauses
	return s
}

func (s *deleteStatementImpl) LIMIT(limit int64) DeleteStatement {
	s.Limit.Count = limit
	return s
}
