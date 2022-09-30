package mysql

import "github.com/go-jet/jet/v2/internal/jet"

// UpdateStatement is interface of SQL UPDATE statement
type UpdateStatement interface {
	jet.Statement

	OPTIMIZER_HINTS(hints ...OptimizerHint) UpdateStatement

	SET(value interface{}, values ...interface{}) UpdateStatement
	MODEL(data interface{}) UpdateStatement

	WHERE(expression BoolExpression) UpdateStatement
}

type updateStatementImpl struct {
	jet.SerializerStatement

	Update jet.ClauseUpdate
	Set    jet.SetClause
	SetNew jet.SetClauseNew
	Where  jet.ClauseWhere
}

func newUpdateStatement(table Table, columns []jet.Column) UpdateStatement {
	update := &updateStatementImpl{}
	update.SerializerStatement = jet.NewStatementImpl(Dialect, jet.UpdateStatementType, update,
		&update.Update,
		&update.Set,
		&update.SetNew,
		&update.Where)

	update.Update.Table = table
	update.Set.Columns = columns
	update.Where.Mandatory = true

	return update
}

func (u *updateStatementImpl) OPTIMIZER_HINTS(hints ...OptimizerHint) UpdateStatement {
	u.Update.OptimizerHints = hints
	return u
}

func (u *updateStatementImpl) SET(value interface{}, values ...interface{}) UpdateStatement {
	columnAssigment, isColumnAssigment := value.(ColumnAssigment)

	if isColumnAssigment {
		u.SetNew = []ColumnAssigment{columnAssigment}
		for _, value := range values {
			u.SetNew = append(u.SetNew, value.(ColumnAssigment))
		}
	} else {
		u.Set.Values = jet.UnwindRowFromValues(value, values)
	}

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
