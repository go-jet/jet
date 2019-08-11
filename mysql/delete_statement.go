package mysql

import "github.com/go-jet/jet/internal/jet"

type DeleteStatement interface {
	jet.Statement

	WHERE(expression BoolExpression) Statement
}

type deleteStatementImpl struct {
	jet.StatementImpl

	Delete jet.ClauseStatementBegin
	Where  jet.ClauseWhere
}

func newDeleteStatement(table Table) DeleteStatement {
	newDelete := &deleteStatementImpl{}
	newDelete.StatementImpl = jet.NewStatementImpl(Dialect, jet.DeleteStatementType, newDelete, &newDelete.Delete,
		&newDelete.Where)

	newDelete.Delete.Name = "DELETE FROM"
	newDelete.Delete.Tables = append(newDelete.Delete.Tables, table)
	newDelete.Where.Mandatory = true

	return newDelete
}

func (d *deleteStatementImpl) WHERE(expression BoolExpression) Statement {
	d.Where.Condition = expression
	return d
}
