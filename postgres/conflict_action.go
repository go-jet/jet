package postgres

import "github.com/go-jet/jet/internal/jet"

type conflictAction interface {
	jet.Serializer
	SET(column jet.ColumnSerializer, expression interface{}) conflictAction
	WHERE(condition BoolExpression) conflictAction
}

// SET creates conflict action for ON_CONFLICT clause
func SET(column jet.ColumnSerializer, expression interface{}) conflictAction {
	conflictAction := updateConflictActionImpl{}
	conflictAction.doUpdate = jet.KeywordClause{Keyword: "DO UPDATE"}
	conflictAction.Serializer = jet.NewSerializerClauseImpl(&conflictAction.doUpdate, &conflictAction.set, &conflictAction.where)
	conflictAction.SET(column, expression)
	return &conflictAction
}

type updateConflictActionImpl struct {
	jet.Serializer

	doUpdate jet.KeywordClause
	set      jet.SetClause
	where    jet.ClauseWhere
}

func (u *updateConflictActionImpl) SET(column jet.ColumnSerializer, expression interface{}) conflictAction {
	u.set = append(u.set, jet.SetPair{Column: column, Value: jet.ToSerializerValue(expression)})
	return u
}

func (u *updateConflictActionImpl) WHERE(condition BoolExpression) conflictAction {
	u.where.Condition = condition
	return u
}
