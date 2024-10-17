package postgres

import (
	"math"

	"github.com/go-jet/jet/v2/internal/jet"
)

// RowLock is interface for SELECT statement row lock types
type RowLock = jet.RowLock

// Row lock types
var (
	UPDATE        = jet.NewRowLock("UPDATE")
	NO_KEY_UPDATE = jet.NewRowLock("NO KEY UPDATE")
	SHARE         = jet.NewRowLock("SHARE")
	KEY_SHARE     = jet.NewRowLock("KEY SHARE")
)

// Window function clauses
var (
	PARTITION_BY = jet.PARTITION_BY
	ORDER_BY     = jet.ORDER_BY
	UNBOUNDED    = int64(math.MaxInt64)
	CURRENT_ROW  = jet.CURRENT_ROW
)

// PRECEDING window frame clause
func PRECEDING(offset int64) jet.FrameExtent {
	return jet.PRECEDING(toJetFrameOffset(offset))
}

// FOLLOWING window frame clause
func FOLLOWING(offset int64) jet.FrameExtent {
	return jet.FOLLOWING(toJetFrameOffset(offset))
}

// Window definition reference
var Window = jet.WindowName

// SelectStatement is interface for PostgreSQL SELECT statement
type SelectStatement interface {
	Statement
	jet.HasProjections
	Expression

	DISTINCT(on ...jet.ColumnExpression) SelectStatement
	FROM(tables ...ReadableTable) SelectStatement
	WHERE(expression BoolExpression) SelectStatement
	GROUP_BY(groupByClauses ...GroupByClause) SelectStatement
	HAVING(boolExpression BoolExpression) SelectStatement
	WINDOW(name string) windowExpand
	ORDER_BY(orderByClauses ...OrderByClause) SelectStatement
	LIMIT(limit int64) SelectStatement
	OFFSET(offset int64) SelectStatement
	// OFFSET_e can be used when an integer expression is needed as offset, otherwise OFFSET can be used
	OFFSET_e(offset IntegerExpression) SelectStatement
	FETCH_FIRST(count IntegerExpression) fetchExpand
	FOR(lock RowLock) SelectStatement

	UNION(rhs SelectStatement) setStatement
	UNION_ALL(rhs SelectStatement) setStatement
	INTERSECT(rhs SelectStatement) setStatement
	INTERSECT_ALL(rhs SelectStatement) setStatement
	EXCEPT(rhs SelectStatement) setStatement
	EXCEPT_ALL(rhs SelectStatement) setStatement

	AsTable(alias string) SelectTable
}

// SELECT creates new SelectStatement with list of projections
func SELECT(projection Projection, projections ...Projection) SelectStatement {
	return newSelectStatement(nil, append([]Projection{projection}, projections...))
}

func newSelectStatement(table ReadableTable, projections []Projection) SelectStatement {
	newSelect := &selectStatementImpl{}
	newSelect.ExpressionStatement = jet.NewExpressionStatementImpl(Dialect, jet.SelectStatementType, newSelect,
		&newSelect.Select,
		&newSelect.From,
		&newSelect.Where,
		&newSelect.GroupBy,
		&newSelect.Having,
		&newSelect.Window,
		&newSelect.OrderBy,
		&newSelect.Limit,
		&newSelect.Offset,
		&newSelect.Fetch,
		&newSelect.For)

	newSelect.Select.ProjectionList = projections
	if table != nil {
		newSelect.From.Tables = []jet.Serializer{table}
	}
	newSelect.Limit.Count = -1

	newSelect.setOperatorsImpl.parent = newSelect

	return newSelect
}

type selectStatementImpl struct {
	jet.ExpressionStatement
	setOperatorsImpl

	Select  jet.ClauseSelect
	From    jet.ClauseFrom
	Where   jet.ClauseWhere
	GroupBy jet.ClauseGroupBy
	Having  jet.ClauseHaving
	Window  jet.ClauseWindow
	OrderBy jet.ClauseOrderBy
	Limit   jet.ClauseLimit
	Offset  jet.ClauseOffset
	Fetch   jet.ClauseFetch
	For     jet.ClauseFor
}

func (s *selectStatementImpl) DISTINCT(on ...jet.ColumnExpression) SelectStatement {
	s.Select.Distinct = true
	s.Select.DistinctOnColumns = on
	return s
}

func (s *selectStatementImpl) FROM(tables ...ReadableTable) SelectStatement {
	s.From.Tables = readableTablesToSerializerList(tables)
	return s
}

func (s *selectStatementImpl) WHERE(condition BoolExpression) SelectStatement {
	s.Where.Condition = condition
	return s
}

func (s *selectStatementImpl) GROUP_BY(groupByClauses ...GroupByClause) SelectStatement {
	s.GroupBy.List = groupByClauses
	return s
}

func (s *selectStatementImpl) HAVING(boolExpression BoolExpression) SelectStatement {
	s.Having.Condition = boolExpression
	return s
}

func (s *selectStatementImpl) WINDOW(name string) windowExpand {
	s.Window.Definitions = append(s.Window.Definitions, jet.WindowDefinition{Name: name})
	return windowExpand{selectStatement: s}
}

func (s *selectStatementImpl) ORDER_BY(orderByClauses ...OrderByClause) SelectStatement {
	s.OrderBy.List = orderByClauses
	return s
}

func (s *selectStatementImpl) LIMIT(limit int64) SelectStatement {
	s.Limit.Count = limit
	return s
}

func (s *selectStatementImpl) OFFSET(offset int64) SelectStatement {
	s.Offset.Count = Int(offset)
	return s
}

func (s *selectStatementImpl) OFFSET_e(offset IntegerExpression) SelectStatement {
	s.Offset.Count = offset
	return s
}

func (s *selectStatementImpl) FETCH_FIRST(count IntegerExpression) fetchExpand {
	s.Fetch.Count = count

	return fetchExpand{
		selectStatement: s,
	}
}

func (s *selectStatementImpl) FOR(lock RowLock) SelectStatement {
	s.For.Lock = lock
	return s
}

func (s *selectStatementImpl) AsTable(alias string) SelectTable {
	return newSelectTable(s, alias, nil)
}

//-----------------------------------------------------

type windowExpand struct {
	selectStatement *selectStatementImpl
}

func (w windowExpand) AS(window ...jet.Window) SelectStatement {
	if len(window) == 0 {
		return w.selectStatement
	}
	windowsDefinition := w.selectStatement.Window.Definitions
	windowsDefinition[len(windowsDefinition)-1].Window = window[0]
	return w.selectStatement
}

func toJetFrameOffset(offset int64) jet.Serializer {
	if offset == UNBOUNDED {
		return jet.UNBOUNDED
	}
	return jet.FixedLiteral(offset)
}

func readableTablesToSerializerList(tables []ReadableTable) []jet.Serializer {
	var ret []jet.Serializer
	for _, table := range tables {
		ret = append(ret, table)
	}
	return ret
}

type fetchExpand struct {
	selectStatement *selectStatementImpl
}

func (f fetchExpand) ROWS_ONLY() SelectStatement {
	f.selectStatement.Fetch.WithTies = false

	return f.selectStatement
}

func (f fetchExpand) ROWS_WITH_TIES() SelectStatement {
	f.selectStatement.Fetch.WithTies = true

	return f.selectStatement
}
