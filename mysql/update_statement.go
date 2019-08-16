package mysql

import "github.com/go-jet/jet/internal/jet"

// UpdateStatement is interface of SQL UPDATE statement
type UpdateStatement interface {
	jet.Statement

	SET(value interface{}, values ...interface{}) UpdateStatement
	MODEL(data interface{}) UpdateStatement

	WHERE(expression BoolExpression) UpdateStatement
}

type updateStatementImpl struct {
	jet.StatementImpl

	Update jet.ClauseUpdate
	Set    jet.ClauseSet
	Where  jet.ClauseWhere
}

func newUpdateStatement(table Table, columns []jet.Column) UpdateStatement {
	update := &updateStatementImpl{}
	update.StatementImpl = jet.NewStatementImpl(Dialect, jet.UpdateStatementType, update, &update.Update,
		&update.Set, &update.Where)

	update.Update.Table = table
	update.Set.Columns = columns
	update.Where.Mandatory = true

	return update
}

func (u *updateStatementImpl) SET(value interface{}, values ...interface{}) UpdateStatement {
	u.Set.Values = jet.UnwindRowFromValues(value, values)
	return u
}

func (u *updateStatementImpl) MODEL(data interface{}) UpdateStatement {
	u.Set.Values = jet.UnwindRowFromModel(u.Set.Columns, data)
	return u
}

func (u *updateStatementImpl) WHERE(expression BoolExpression) UpdateStatement {
	u.Where.Condition = expression
	return u
}
