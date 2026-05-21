package mysql

import (
	"github.com/go-jet/jet/v2/internal/jet"
)

// SelectJsonStatement is an interface for MySQL statements that generate JSON on the server.
type SelectJsonStatement interface {
	Statement
	jet.Serializer

	AS(alias string) Projection

	FROM(table ReadableTable) SelectJsonStatement
	WHERE(condition BoolExpression) SelectJsonStatement
	ORDER_BY(orderByClauses ...OrderByClause) SelectJsonStatement
	LIMIT(limit int64) SelectJsonStatement
	OFFSET(offset int64) SelectJsonStatement
}

// SELECT_JSON_ARR creates a new SelectJsonStatement with a list of projections.
func SELECT_JSON_ARR(projections ...Projection) SelectJsonStatement {
	return newSelectStatementJson(projections, jet.SelectJsonArrStatementType)
}

// SELECT_JSON_OBJ creates a new SelectJsonStatement with a list of projections.
func SELECT_JSON_OBJ(projections ...Projection) SelectJsonStatement {
	return newSelectStatementJson(projections, jet.SelectJsonObjStatementType)
}

type selectJsonStatement struct {
	*selectStatementImpl

	projections   []Projection
	statementType jet.StatementType

	// SELECT_JSON_ARR internal clauses
	arrOrderBy *jet.ClauseOrderBy
	arrLimit   *jet.ClauseLimit
	arrOffset  *jet.ClauseOffset
}

func newSelectStatementJson(projections []Projection, statementType jet.StatementType) SelectJsonStatement {
	newSelectJson := &selectJsonStatement{
		selectStatementImpl: newSelectStatement(statementType, nil, nil),

		projections:   projections,
		statementType: statementType,

		arrOrderBy: &jet.ClauseOrderBy{},
		arrLimit:   &jet.ClauseLimit{Count: -1},
		arrOffset:  &jet.ClauseOffset{},
	}

	newSelectJson.constructProjectionList()

	return newSelectJson
}

func (s *selectJsonStatement) constructProjectionList() {
	jsonProjection := Func("JSON_OBJECT", CustomExpression(jet.JsonObjProjectionList(s.projections)))

	if s.statementType == jet.SelectJsonArrStatementType {
		jsonProjection = Func("JSON_ARRAYAGG", CustomExpression(
			jsonProjection,
			s.arrOrderBy,
			s.arrLimit,
			s.arrOffset,
		))
	}

	s.Select.ProjectionList = ProjectionList{jsonProjection.AS("json")}
}

func (s *selectJsonStatement) FROM(table ReadableTable) SelectJsonStatement {
	s.From.Tables = []jet.Serializer{table}

	return s
}

func (s *selectJsonStatement) WHERE(condition BoolExpression) SelectJsonStatement {
	s.Where.Condition = condition
	return s
}

func (s *selectJsonStatement) ORDER_BY(orderBy ...OrderByClause) SelectJsonStatement {
	if s.statementType == jet.SelectJsonArrStatementType {
		s.arrOrderBy.List = orderBy
	} else {
		s.OrderBy.List = orderBy
	}

	return s
}

func (s *selectJsonStatement) LIMIT(limit int64) SelectJsonStatement {
	if s.statementType == jet.SelectJsonArrStatementType {
		s.arrLimit.Count = limit
	} else {
		s.Limit.Count = limit
	}

	return s
}

func (s *selectJsonStatement) OFFSET(offset int64) SelectJsonStatement {
	if s.statementType == jet.SelectJsonArrStatementType {
		s.arrOffset.Count = Int(offset)
	} else {
		s.Offset.Count = Int(offset)
	}

	return s
}
