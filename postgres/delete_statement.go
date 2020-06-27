package postgres

import "github.com/go-jet/jet/v2/internal/jet"

// DeleteStatement is interface for PostgreSQL DELETE statement
type DeleteStatement interface {
	jet.SerializerStatement

	WHERE(expression BoolExpression) DeleteStatement

	RETURNING(projections ...jet.Projection) DeleteStatement
}

type deleteStatementImpl struct {
	jet.SerializerStatement

	Delete    jet.ClauseStatementBegin
	Where     jet.ClauseWhere
	Returning clauseReturning
}

func newDeleteStatement(table WritableTable) DeleteStatement {
	newDelete := &deleteStatementImpl{}
	newDelete.SerializerStatement = jet.NewStatementImpl(Dialect, jet.DeleteStatementType, newDelete, &newDelete.Delete,
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
	d.Returning.ProjectionList = projections
	return d
}
