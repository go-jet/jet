package mysql

import "github.com/go-jet/jet/internal/jet"

type SelectLock = jet.SelectLock

var (
	UPDATE = jet.NewSelectLock("UPDATE")
	SHARE  = jet.NewSelectLock("SHARE")
)

type SelectStatement interface {
	Statement
	jet.HasProjections
	Expression

	DISTINCT() SelectStatement
	FROM(table ReadableTable) SelectStatement
	WHERE(expression BoolExpression) SelectStatement
	GROUP_BY(groupByClauses ...jet.GroupByClause) SelectStatement
	HAVING(boolExpression BoolExpression) SelectStatement
	ORDER_BY(orderByClauses ...jet.OrderByClause) SelectStatement
	LIMIT(limit int64) SelectStatement
	OFFSET(offset int64) SelectStatement
	FOR(lock SelectLock) SelectStatement
	LOCK_IN_SHARE_MODE() SelectStatement

	UNION(rhs SelectStatement) SetStatement
	UNION_ALL(rhs SelectStatement) SetStatement

	AsTable(alias string) SelectTable
}

//SELECT creates new SelectStatement with list of projections
func SELECT(projection Projection, projections ...Projection) SelectStatement {
	return newSelectStatement(nil, append([]Projection{projection}, projections...))
}

func toJetProjectionList(projections []Projection) []jet.Projection {
	ret := []jet.Projection{}

	for _, projection := range projections {
		ret = append(ret, projection)
	}

	return ret
}

func newSelectStatement(table ReadableTable, projections []Projection) SelectStatement {
	newSelect := &selectStatementImpl{}
	newSelect.ExpressionStatementImpl.StatementImpl = jet.NewStatementImpl(Dialect, jet.SelectStatementType, newSelect, &newSelect.Select,
		&newSelect.From, &newSelect.Where, &newSelect.GroupBy, &newSelect.Having, &newSelect.OrderBy,
		&newSelect.Limit, &newSelect.Offset, &newSelect.For, &newSelect.ShareLock)

	newSelect.ExpressionStatementImpl.ExpressionInterfaceImpl.Parent = newSelect

	newSelect.Select.Projections = toJetProjectionList(projections)
	newSelect.From.Table = table
	newSelect.Limit.Count = -1
	newSelect.Offset.Count = -1
	newSelect.ShareLock.Name = "LOCK IN SHARE MODE"
	newSelect.ShareLock.InNewLine = true

	newSelect.setOperatorsImpl.parent = newSelect

	return newSelect
}

type selectStatementImpl struct {
	jet.ExpressionStatementImpl
	setOperatorsImpl

	Select    jet.ClauseSelect
	From      jet.ClauseFrom
	Where     jet.ClauseWhere
	GroupBy   jet.ClauseGroupBy
	Having    jet.ClauseHaving
	OrderBy   jet.ClauseOrderBy
	Limit     jet.ClauseLimit
	Offset    jet.ClauseOffset
	For       jet.ClauseFor
	ShareLock jet.ClauseOptional
}

func (s *selectStatementImpl) DISTINCT() SelectStatement {
	s.Select.Distinct = true
	return s
}

func (s *selectStatementImpl) FROM(table ReadableTable) SelectStatement {
	s.From.Table = table
	return s
}

func (s *selectStatementImpl) WHERE(condition BoolExpression) SelectStatement {
	s.Where.Condition = condition
	return s
}

func (s *selectStatementImpl) GROUP_BY(groupByClauses ...jet.GroupByClause) SelectStatement {
	s.GroupBy.List = groupByClauses
	return s
}

func (s *selectStatementImpl) HAVING(boolExpression BoolExpression) SelectStatement {
	s.Having.Condition = boolExpression
	return s
}

func (s *selectStatementImpl) ORDER_BY(orderByClauses ...jet.OrderByClause) SelectStatement {
	s.OrderBy.List = orderByClauses
	return s
}

func (s *selectStatementImpl) LIMIT(limit int64) SelectStatement {
	s.Limit.Count = limit
	return s
}

func (s *selectStatementImpl) OFFSET(offset int64) SelectStatement {
	s.Offset.Count = offset
	return s
}

func (s *selectStatementImpl) FOR(lock SelectLock) SelectStatement {
	s.For.Lock = lock
	return s
}

func (s *selectStatementImpl) LOCK_IN_SHARE_MODE() SelectStatement {
	s.ShareLock.Show = true
	return s
}

func (s *selectStatementImpl) AsTable(alias string) SelectTable {
	return newSelectTable(s, alias)
}
