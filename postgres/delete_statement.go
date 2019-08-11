package postgres

import "github.com/go-jet/jet/internal/jet"

type DeleteStatement interface {
	jet.Statement

	WHERE(expression BoolExpression) DeleteStatement

	RETURNING(projections ...jet.Projection) DeleteStatement
}

type deleteStatementImpl struct {
	jet.StatementImpl

	Delete    jet.ClauseStatementBegin
	Where     jet.ClauseWhere
	Returning ClauseReturning
}

func newDeleteStatement(table WritableTable) DeleteStatement {
	newDelete := &deleteStatementImpl{}
	newDelete.StatementImpl = jet.NewStatementImpl(Dialect, jet.DeleteStatementType, newDelete, &newDelete.Delete,
		&newDelete.Where, &newDelete.Returning)

	newDelete.Delete.Name = "DELETE FROM"
	newDelete.Delete.Tables = append(newDelete.Delete.Tables, table)
	newDelete.Where.Mandatory = true

	return newDelete
}

func (d *deleteStatementImpl) WHERE(expression BoolExpression) DeleteStatement {
	d.Where.Condition = expression
	return d
}

func (d *deleteStatementImpl) RETURNING(projections ...jet.Projection) DeleteStatement {
	d.Returning.Projections = projections
	return d
}
