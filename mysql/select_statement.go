package mysql

import (
	"github.com/go-jet/jet/v2/internal/jet"
)

// RowLock is interface for SELECT statement row lock types
type RowLock = jet.RowLock

// Row lock types
var (
	UPDATE = jet.NewRowLock("UPDATE")
	SHARE  = jet.NewRowLock("SHARE")
)

// Window function clauses
var (
	PARTITION_BY = jet.PARTITION_BY
	ORDER_BY     = jet.ORDER_BY
	UNBOUNDED    = jet.UNBOUNDED
	CURRENT_ROW  = jet.CURRENT_ROW
)

// PRECEDING window frame clause
func PRECEDING(offset interface{}) jet.FrameExtent {
	return jet.PRECEDING(toJetFrameOffset(offset))
}

// FOLLOWING window frame clause
func FOLLOWING(offset interface{}) jet.FrameExtent {
	return jet.FOLLOWING(toJetFrameOffset(offset))
}

// Window is used to specify window reference from WINDOW clause
var Window = jet.WindowName

// SelectStatement is interface for MySQL SELECT statement
type SelectStatement interface {
	Statement
	jet.HasProjections
	Expression

	OPTIMIZER_HINTS(hints ...OptimizerHint) SelectStatement

	DISTINCT() SelectStatement
	FROM(tables ...ReadableTable) SelectStatement
	WHERE(expression BoolExpression) SelectStatement
	GROUP_BY(groupByClauses ...GroupByClause) SelectStatement
	HAVING(boolExpression BoolExpression) SelectStatement
	WINDOW(name string) windowExpand
	ORDER_BY(orderByClauses ...OrderByClause) SelectStatement
	LIMIT(limit int64) SelectStatement
	OFFSET(offset int64) SelectStatement
	FOR(lock RowLock) SelectStatement
	LOCK_IN_SHARE_MODE() SelectStatement

	UNION(rhs SelectStatement) setStatement
	UNION_ALL(rhs SelectStatement) setStatement

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
		&newSelect.For,
		&newSelect.ShareLock,
	)

	newSelect.Select.ProjectionList = projections
	if table != nil {
		newSelect.From.Tables = []jet.Serializer{table}
	}
	newSelect.Limit.Count = -1
	newSelect.ShareLock.Name = "LOCK IN SHARE MODE"
	newSelect.ShareLock.InNewLine = true

	newSelect.setOperatorsImpl.parent = newSelect

	return newSelect
}

type selectStatementImpl struct {
	jet.ExpressionStatement
	setOperatorsImpl

	Select    jet.ClauseSelect
	From      jet.ClauseFrom
	Where     jet.ClauseWhere
	GroupBy   jet.ClauseGroupBy
	Having    jet.ClauseHaving
	Window    jet.ClauseWindow
	OrderBy   jet.ClauseOrderBy
	Limit     jet.ClauseLimit
	Offset    jet.ClauseOffset
	For       jet.ClauseFor
	ShareLock jet.ClauseOptional
}

func (s *selectStatementImpl) OPTIMIZER_HINTS(hints ...OptimizerHint) SelectStatement {
	s.Select.OptimizerHints = hints
	return s
}

func (s *selectStatementImpl) DISTINCT() SelectStatement {
	s.Select.Distinct = true
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

func (s *selectStatementImpl) FOR(lock RowLock) SelectStatement {
	s.For.Lock = lock
	return s
}

func (s *selectStatementImpl) LOCK_IN_SHARE_MODE() SelectStatement {
	s.ShareLock.Show = true
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

func toJetFrameOffset(offset interface{}) jet.Serializer {
	if offset == UNBOUNDED {
		return jet.UNBOUNDED
	}

	// check for interval expression
	//if exp, ok := offset.(Expression); ok {
	//	return exp
	//}

	return jet.FixedLiteral(offset)
}

func readableTablesToSerializerList(tables []ReadableTable) []jet.Serializer {
	var ret []jet.Serializer
	for _, table := range tables {
		ret = append(ret, table)
	}
	return ret
}
