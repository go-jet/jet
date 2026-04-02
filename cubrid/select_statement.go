package cubrid

import "github.com/go-jet/jet/v2/internal/jet"

// RowLock is interface for SELECT statement row lock types
type RowLock = jet.RowLock

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

// SelectStatement is interface for CUBRID SELECT statement
type SelectStatement interface {
	Statement
	jet.HasProjections
	Expression

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

	// CUBRID hierarchical query support
	START_WITH(condition BoolExpression) SelectStatement
	CONNECT_BY(condition BoolExpression) SelectStatement
	CONNECT_BY_NOCYCLE(condition BoolExpression) SelectStatement
	ORDER_SIBLINGS_BY(orderByClauses ...OrderByClause) SelectStatement

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
	return newSelectStatement(jet.SelectStatementType, nil, append([]Projection{projection}, projections...))
}

func newSelectStatement(stmtType jet.StatementType, table ReadableTable, projections []Projection) *selectStatementImpl {
	newSelect := &selectStatementImpl{}
	newSelect.ExpressionStatement = jet.NewExpressionStatementImpl(Dialect, stmtType, newSelect,
		&newSelect.Select, &newSelect.From, &newSelect.Where, &newSelect.GroupBy,
		&newSelect.Having, &newSelect.Window,
		&newSelect.StartWith, &newSelect.ConnectBy, &newSelect.OrderSiblingsBy,
		&newSelect.OrderBy, &newSelect.Limit, &newSelect.Offset, &newSelect.For)

	newSelect.Select.ProjectionList = projections
	if table != nil {
		newSelect.From.Tables = []jet.Serializer{table}
	}
	newSelect.Limit.Count = -1
	newSelect.setOperatorsImpl.root = newSelect
	return newSelect
}

type selectStatementImpl struct {
	jet.ExpressionStatement
	setOperatorsImpl
	Select          jet.ClauseSelect
	From            jet.ClauseFrom
	Where           jet.ClauseWhere
	GroupBy         jet.ClauseGroupBy
	Having          jet.ClauseHaving
	Window          jet.ClauseWindow
	StartWith       jet.ClauseStartWith
	ConnectBy       jet.ClauseConnectBy
	OrderSiblingsBy jet.ClauseOrderSiblingsBy
	OrderBy         jet.ClauseOrderBy
	Limit           jet.ClauseLimit
	Offset          jet.ClauseOffset
	For             jet.ClauseFor
}

func (s *selectStatementImpl) DISTINCT() SelectStatement     { s.Select.Distinct = true; return s }
func (s *selectStatementImpl) FROM(tables ...ReadableTable) SelectStatement {
	s.From.Tables = readableTablesToSerializerList(tables); return s
}
func (s *selectStatementImpl) WHERE(c BoolExpression) SelectStatement { s.Where.Condition = c; return s }
func (s *selectStatementImpl) GROUP_BY(g ...GroupByClause) SelectStatement {
	s.GroupBy.List = g; return s
}
func (s *selectStatementImpl) HAVING(c BoolExpression) SelectStatement {
	s.Having.Condition = c; return s
}
func (s *selectStatementImpl) WINDOW(name string) windowExpand {
	s.Window.Definitions = append(s.Window.Definitions, jet.WindowDefinition{Name: name})
	return windowExpand{selectStatement: s}
}
func (s *selectStatementImpl) START_WITH(c BoolExpression) SelectStatement {
	s.StartWith.Condition = c; return s
}
func (s *selectStatementImpl) CONNECT_BY(c BoolExpression) SelectStatement {
	s.ConnectBy.Condition = c; s.ConnectBy.NoCycle = false; return s
}
func (s *selectStatementImpl) CONNECT_BY_NOCYCLE(c BoolExpression) SelectStatement {
	s.ConnectBy.Condition = c; s.ConnectBy.NoCycle = true; return s
}
func (s *selectStatementImpl) ORDER_SIBLINGS_BY(o ...OrderByClause) SelectStatement {
	s.OrderSiblingsBy.List = o; return s
}
func (s *selectStatementImpl) ORDER_BY(o ...OrderByClause) SelectStatement {
	s.OrderBy.List = o; return s
}
func (s *selectStatementImpl) LIMIT(l int64) SelectStatement  { s.Limit.Count = l; return s }
func (s *selectStatementImpl) OFFSET(o int64) SelectStatement { s.Offset.Count = Int(o); return s }
func (s *selectStatementImpl) FOR(lock RowLock) SelectStatement { s.For.Lock = lock; return s }
func (s *selectStatementImpl) AsTable(alias string) SelectTable {
	return newSelectTable(s, alias, nil)
}

type windowExpand struct{ selectStatement *selectStatementImpl }

func (w windowExpand) AS(window ...jet.Window) SelectStatement {
	if len(window) == 0 {
		return w.selectStatement
	}
	defs := w.selectStatement.Window.Definitions
	defs[len(defs)-1].Window = window[0]
	return w.selectStatement
}

func toJetFrameOffset(offset interface{}) jet.Serializer {
	if offset == UNBOUNDED {
		return jet.UNBOUNDED
	}
	return jet.FixedLiteral(offset)
}

func readableTablesToSerializerList(tables []ReadableTable) []jet.Serializer {
	ret := make([]jet.Serializer, len(tables))
	for i, t := range tables {
		ret[i] = t
	}
	return ret
}
