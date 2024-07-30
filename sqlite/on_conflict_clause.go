package sqlite

import (
	"github.com/go-jet/jet/v2/internal/jet"
	"github.com/go-jet/jet/v2/internal/utils/is"
)

type onConflict interface {
	WHERE(indexPredicate BoolExpression) conflictTarget
	conflictTarget
}

type conflictTarget interface {
	DO_NOTHING() InsertStatement
	DO_UPDATE(action conflictAction) InsertStatement
}

type onConflictClause struct {
	insertStatement  InsertStatement
	indexExpressions []jet.ColumnExpression
	whereClause      jet.ClauseWhere
	do               jet.Serializer
}

func (o *onConflictClause) WHERE(indexPredicate BoolExpression) conflictTarget {
	o.whereClause.Condition = indexPredicate
	return o
}

func (o *onConflictClause) DO_NOTHING() InsertStatement {
	o.do = jet.Keyword("DO NOTHING")
	return o.insertStatement
}

func (o *onConflictClause) DO_UPDATE(action conflictAction) InsertStatement {
	o.do = action
	return o.insertStatement
}

func (o *onConflictClause) Serialize(statementType jet.StatementType, out *jet.SQLBuilder, options ...jet.SerializeOption) {
	if is.Nil(o.do) {
		return
	}

	out.NewLine()
	out.WriteString("ON CONFLICT")
	if len(o.indexExpressions) > 0 {
		out.WriteString("(")
		jet.SerializeColumnExpressions(o.indexExpressions, statementType, out, jet.ShortName)
		out.WriteString(")")
	}

	o.whereClause.Serialize(statementType, out, jet.SkipNewLine, jet.ShortName)

	out.IncreaseIdent(7)
	jet.Serialize(o.do, statementType, out)
	out.DecreaseIdent(7)
}

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
