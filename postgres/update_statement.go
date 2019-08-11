package postgres

import "github.com/go-jet/jet/internal/jet"

// UpdateStatement is interface of SQL UPDATE statement
type UpdateStatement interface {
	jet.Statement

	SET(value interface{}, values ...interface{}) UpdateStatement
	MODEL(data interface{}) UpdateStatement

	WHERE(expression BoolExpression) UpdateStatement
	RETURNING(projections ...jet.Projection) UpdateStatement
}

type updateStatementImpl struct {
	jet.StatementImpl

	Update    jet.ClauseUpdate
	Set       jet.ClauseSet
	Where     jet.ClauseWhere
	Returning jet.ClauseReturning
}

func newUpdateStatement(table WritableTable, columns []jet.IColumn) UpdateStatement {
	update := &updateStatementImpl{}
	update.StatementImpl = jet.NewStatementImpl(Dialect, jet.UpdateStatementType, update, &update.Update,
		&update.Set, &update.Where, &update.Returning)

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

func (u *updateStatementImpl) RETURNING(projections ...jet.Projection) UpdateStatement {
	u.Returning.Projections = projections
	return u
}
