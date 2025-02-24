package postgres

import (
	"github.com/go-jet/jet/v2/internal/jet"
	"strings"
)

// SELECT_JSON_ARR creates a new SelectJsonStatement with a list of projections.
func SELECT_JSON_ARR(projections ...Projection) SelectStatement {
	return newSelectStatementJson(projections, jet.SelectJsonArrStatementType)
}

// SELECT_JSON_OBJ creates a new SelectJsonStatement with a list of projections.
func SELECT_JSON_OBJ(projections ...Projection) SelectStatement {
	return newSelectStatementJson(projections, jet.SelectJsonObjStatementType)
}

type selectJsonStatement struct {
	*selectStatementImpl

	subQuery      *selectStatementImpl
	statementType jet.StatementType
}

func (s *selectJsonStatement) AS(alias string) Projection {
	s.setSubQueryAlias(strings.ToLower(alias) + "_")

	return s.selectStatementImpl.AS(alias)
}

func (s *selectJsonStatement) FROM(table ...ReadableTable) SelectStatement {
	s.subQuery.From.Tables = readableTablesToSerializerList(table)

	return s
}

func (s *selectJsonStatement) DISTINCT(on ...jet.ColumnExpression) SelectStatement {
	s.subQuery.Select.Distinct = true
	s.subQuery.Select.DistinctOnColumns = on
	return s
}

func (s *selectJsonStatement) WHERE(condition BoolExpression) SelectStatement {
	s.subQuery.Where.Condition = condition
	return s
}

func (s *selectJsonStatement) GROUP_BY(groupByClauses ...GroupByClause) SelectStatement {
	s.subQuery.GroupBy.List = groupByClauses
	return s
}

func (s *selectJsonStatement) HAVING(boolExpression BoolExpression) SelectStatement {
	s.subQuery.Having.Condition = boolExpression
	return s
}

func (s *selectJsonStatement) WINDOW(name string) windowExpand {
	s.subQuery.Window.Definitions = append(s.subQuery.Window.Definitions, jet.WindowDefinition{Name: name})
	return windowExpand{
		selectStatement: s.subQuery,
		rootStmt:        s,
	}
}

func (s *selectJsonStatement) ORDER_BY(orderByClauses ...OrderByClause) SelectStatement {
	s.subQuery.OrderBy.List = orderByClauses
	return s
}

func (s *selectJsonStatement) LIMIT(limit int64) SelectStatement {
	s.subQuery.Limit.Count = limit
	return s
}

func (s *selectJsonStatement) OFFSET(offset int64) SelectStatement {
	s.subQuery.Offset.Count = Int(offset)
	return s
}

func (s *selectJsonStatement) OFFSET_e(offset IntegerExpression) SelectStatement {
	s.subQuery.Offset.Count = offset
	return s
}

func (s *selectJsonStatement) FETCH_FIRST(count IntegerExpression) fetchExpand {
	s.subQuery.Fetch.Count = count

	return fetchExpand{
		selectStatement: s.subQuery,
		rootStmt:        s,
	}
}

func (s *selectJsonStatement) FOR(lock RowLock) SelectStatement {
	s.subQuery.For.Lock = lock
	return s
}

func newSelectStatementJson(projections []Projection, statementType jet.StatementType) SelectStatement {
	newSelectJson := &selectJsonStatement{
		selectStatementImpl: newSelectStatement(statementType, nil, nil),
		subQuery:            newSelectStatement(statementType, nil, projections),
		statementType:       statementType,
	}

	newSelectJson.setOperatorsImpl.stmtRoot = newSelectJson

	newSelectJson.setSubQueryAlias("")

	return newSelectJson
}

func (s *selectJsonStatement) setSubQueryAlias(alias string) {
	subQueryAlias := alias + "records"
	jsonAlias := alias + "json"

	s.Select.ProjectionList = ProjectionList{constructJsonFunc(s.statementType, subQueryAlias).AS(jsonAlias)}

	s.From.Tables = []jet.Serializer{newSelectTable(s.subQuery, subQueryAlias, nil)}
}

func constructJsonFunc(statementType jet.StatementType, subQueryAlias string) Expression {
	rowToJson := Func("row_to_json", CustomExpression(Token(subQueryAlias)))

	if statementType == jet.SelectJsonArrStatementType {
		return Func("json_agg", rowToJson)
	}

	return rowToJson
}
