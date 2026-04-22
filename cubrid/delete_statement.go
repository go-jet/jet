package cubrid

import "github.com/go-jet/jet/v2/internal/jet"

// DeleteStatement is interface for CUBRID DELETE statement
type DeleteStatement interface {
	Statement
	WHERE(expression BoolExpression) DeleteStatement
	ORDER_BY(orderByClauses ...OrderByClause) DeleteStatement
	LIMIT(limit int64) DeleteStatement
}

func newDeleteStatement(table Table) DeleteStatement {
	d := &deleteStatementImpl{}
	d.SerializerStatement = jet.NewStatementImpl(Dialect, jet.DeleteStatementType, d,
		&d.Delete, &d.Where, &d.OrderBy, &d.Limit)
	d.Delete.Table = table
	d.Where.Mandatory = true
	d.Limit.Count = -1
	return d
}

type deleteStatementImpl struct {
	jet.SerializerStatement
	Delete  jet.ClauseDelete
	Where   jet.ClauseWhere
	OrderBy jet.ClauseOrderBy
	Limit   jet.ClauseLimit
}

func (d *deleteStatementImpl) WHERE(e BoolExpression) DeleteStatement { d.Where.Condition = e; return d }
func (d *deleteStatementImpl) ORDER_BY(o ...OrderByClause) DeleteStatement {
	d.OrderBy.List = o; return d
}
func (d *deleteStatementImpl) LIMIT(l int64) DeleteStatement { d.Limit.Count = l; return d }
