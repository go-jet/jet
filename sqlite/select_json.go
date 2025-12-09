package sqlite

import (
	"github.com/go-jet/jet/v2/internal/jet"
)

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
}

func newSelectStatementJson(projections []Projection, statementType jet.StatementType) SelectJsonStatement {
	newSelect := &selectJsonStatement{
		selectStatementImpl: newSelectStatement(statementType, nil, nil),
	}

	newSelect.Select.ProjectionList = ProjectionList{constructJsonFunc(projections, statementType).AS("json")}

	return newSelect
}

func constructJsonFunc(projections []Projection, statementType jet.StatementType) Expression {
	jsonObj := Func("JSON_OBJECT", CustomExpression(jet.JsonObjProjectionList(projections)))

	if statementType == jet.SelectJsonArrStatementType {
		return Func("JSON_GROUP_ARRAY", jsonObj)
	}

	return jsonObj
}

func (s *selectJsonStatement) FROM(table ReadableTable) SelectJsonStatement {
	s.From.Tables = []jet.Serializer{table}

	return s
}

func (s *selectJsonStatement) WHERE(condition BoolExpression) SelectJsonStatement {
	s.Where.Condition = condition
	return s
}

func (s *selectJsonStatement) ORDER_BY(orderByClauses ...OrderByClause) SelectJsonStatement {
	s.OrderBy.List = orderByClauses
	return s
}

func (s *selectJsonStatement) LIMIT(limit int64) SelectJsonStatement {
	s.Limit.Count = limit
	return s
}

func (s *selectJsonStatement) OFFSET(offset int64) SelectJsonStatement {
	s.Offset.Count = Int(offset)
	return s
}
