package postgres

import "github.com/go-jet/jet/v2/internal/jet"

type conflictAction interface {
	jet.Serializer
	WHERE(condition BoolExpression) conflictAction
}

// SET creates conflict action for ON_CONFLICT clause
func SET(assigments ...ColumnAssigment) conflictAction {
	conflictAction := updateConflictActionImpl{}
	conflictAction.doUpdate = jet.KeywordClause{Keyword: "DO UPDATE"}
	conflictAction.Serializer = jet.NewSerializerClauseImpl(&conflictAction.doUpdate, &conflictAction.set, &conflictAction.where)
	conflictAction.set = assigments
	return &conflictAction
}

type updateConflictActionImpl struct {
	jet.Serializer

	doUpdate jet.KeywordClause
	set      jet.SetClauseNew
	where    jet.ClauseWhere
}

func (u *updateConflictActionImpl) WHERE(condition BoolExpression) conflictAction {
	u.where.Condition = condition
	return u
}
