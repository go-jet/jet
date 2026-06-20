package sqlite

import "github.com/go-jet/jet/v2/internal/jet"

// InsertStatement is interface for SQL INSERT statements
type InsertStatement interface {
	Statement

	VALUES(value interface{}, values ...interface{}) InsertStatement
	MODEL(data interface{}) InsertStatement
	MODELS(data interface{}) InsertStatement
	// QUERY inserts the rows produced by the provided query. The query is usually a SELECT, but any serializable
	// statement is accepted, such as a WITH (CTE) statement wrapping a SELECT.
	QUERY(query jet.SerializerStatement) InsertStatement
	DEFAULT_VALUES() InsertStatement

	ON_CONFLICT(indexExpressions ...jet.ColumnExpression) onConflict
	RETURNING(projections ...Projection) InsertStatement
}

func newInsertStatement(table Table, columns []jet.Column) InsertStatement {
	newInsert := &insertStatementImpl{
		DefaultValues: jet.ClauseOptional{Name: "DEFAULT VALUES", InNewLine: true},
	}

	newInsert.SerializerStatement = jet.NewStatementImpl(Dialect, jet.InsertStatementType, newInsert,
		&newInsert.Insert,
		&newInsert.ValuesQuery,
		&newInsert.DefaultValues,
		&newInsert.OnConflict,
		&newInsert.Returning,
	)

	newInsert.Insert.Table = table
	newInsert.Insert.Columns = columns
	newInsert.ValuesQuery.SkipSelectWrap = true

	return newInsert
}

type insertStatementImpl struct {
	jet.SerializerStatement

	Insert        jet.ClauseInsert
	ValuesQuery   jet.ClauseValuesQuery
	DefaultValues jet.ClauseOptional
	OnConflict    onConflictClause
	Returning     jet.ClauseReturning
}

func (is *insertStatementImpl) VALUES(value interface{}, values ...interface{}) InsertStatement {
	is.ValuesQuery.Rows = append(is.ValuesQuery.Rows, jet.UnwindRowFromValues(value, values))
	return is
}

// MODEL will insert row of values, where value for each column is extracted from filed of structure data.
// If data is not struct or there is no field for every column selected, this method will panic.
func (is *insertStatementImpl) MODEL(data interface{}) InsertStatement {
	is.ValuesQuery.Rows = append(is.ValuesQuery.Rows, jet.UnwindRowFromModel(is.Insert.GetColumns(), data))
	return is
}

func (is *insertStatementImpl) MODELS(data interface{}) InsertStatement {
	is.ValuesQuery.Rows = append(is.ValuesQuery.Rows, jet.UnwindRowsFromModels(is.Insert.GetColumns(), data)...)
	return is
}

func (is *insertStatementImpl) QUERY(query jet.SerializerStatement) InsertStatement {
	is.ValuesQuery.Query = query
	return is
}

func (is *insertStatementImpl) DEFAULT_VALUES() InsertStatement {
	is.DefaultValues.Show = true
	return is
}

func (is *insertStatementImpl) RETURNING(projections ...jet.Projection) InsertStatement {
	is.Returning.ProjectionList = projections
	return is
}

func (is *insertStatementImpl) ON_CONFLICT(indexExpressions ...jet.ColumnExpression) onConflict {
	is.OnConflict = onConflictClause{
		insertStatement:  is,
		indexExpressions: indexExpressions,
	}
	return &is.OnConflict
}
