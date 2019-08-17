package postgres

import "github.com/go-jet/jet/internal/jet"

// InsertStatement is interface for SQL INSERT statements
type InsertStatement interface {
	Statement

	// Insert row of values
	VALUES(value interface{}, values ...interface{}) InsertStatement
	// Insert row of values, where value for each column is extracted from filed of structure data.
	// If data is not struct or there is no field for every column selected, this method will panic.
	MODEL(data interface{}) InsertStatement

	MODELS(data interface{}) InsertStatement

	QUERY(selectStatement SelectStatement) InsertStatement

	RETURNING(projections ...jet.Projection) InsertStatement
}

func newInsertStatement(table WritableTable, columns []jet.Column) InsertStatement {
	newInsert := &insertStatementImpl{}
	newInsert.SerializerStatement = jet.NewStatementImpl(Dialect, jet.InsertStatementType, newInsert,
		&newInsert.Insert, &newInsert.ValuesQuery, &newInsert.Returning)

	newInsert.Insert.Table = table
	newInsert.Insert.Columns = columns

	return newInsert
}

type insertStatementImpl struct {
	jet.SerializerStatement

	Insert      jet.ClauseInsert
	ValuesQuery jet.ClauseValuesQuery
	Returning   clauseReturning
}

func (i *insertStatementImpl) VALUES(value interface{}, values ...interface{}) InsertStatement {
	i.ValuesQuery.Rows = append(i.ValuesQuery.Rows, jet.UnwindRowFromValues(value, values))
	return i
}

func (i *insertStatementImpl) MODEL(data interface{}) InsertStatement {
	i.ValuesQuery.Rows = append(i.ValuesQuery.Rows, jet.UnwindRowFromModel(i.Insert.GetColumns(), data))
	return i
}

func (i *insertStatementImpl) MODELS(data interface{}) InsertStatement {
	i.ValuesQuery.Rows = append(i.ValuesQuery.Rows, jet.UnwindRowsFromModels(i.Insert.GetColumns(), data)...)
	return i
}

func (i *insertStatementImpl) RETURNING(projections ...jet.Projection) InsertStatement {
	i.Returning.Projections = projections
	return i
}

func (i *insertStatementImpl) QUERY(selectStatement SelectStatement) InsertStatement {
	i.ValuesQuery.Query = selectStatement
	return i
}
