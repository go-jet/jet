package cubrid

import "github.com/go-jet/jet/v2/internal/jet"

// ReplaceStatement is interface for CUBRID REPLACE INTO statement.
// REPLACE works like INSERT, but if a row with the same PK/unique key exists,
// it deletes the old row first, then inserts the new one.
type ReplaceStatement interface {
	Statement
	VALUES(value interface{}, values ...interface{}) ReplaceStatement
	MODEL(data interface{}) ReplaceStatement
	MODELS(data interface{}) ReplaceStatement
	QUERY(selectStatement SelectStatement) ReplaceStatement
}

// REPLACE_INTO creates a new REPLACE INTO statement for the given table.
func REPLACE_INTO(table Table, columns ...Column) ReplaceStatement {
	cols := make([]jet.Column, len(columns))
	for i, c := range columns {
		cols[i] = c
	}

	rs := &replaceStatementImpl{}
	rs.SerializerStatement = jet.NewStatementImpl(Dialect, jet.InsertStatementType, rs,
		&rs.Replace, &rs.ValuesQuery)
	rs.Replace.Table = table
	rs.Replace.Columns = cols
	return rs
}

type replaceStatementImpl struct {
	jet.SerializerStatement
	Replace     jet.ClauseReplaceInto
	ValuesQuery jet.ClauseValuesQuery
}

func (rs *replaceStatementImpl) VALUES(v interface{}, vs ...interface{}) ReplaceStatement {
	rs.ValuesQuery.Rows = append(rs.ValuesQuery.Rows, jet.UnwindRowFromValues(v, vs))
	return rs
}

func (rs *replaceStatementImpl) MODEL(data interface{}) ReplaceStatement {
	rs.ValuesQuery.Rows = append(rs.ValuesQuery.Rows, jet.UnwindRowFromModel(rs.Replace.GetColumns(), data))
	return rs
}

func (rs *replaceStatementImpl) MODELS(data interface{}) ReplaceStatement {
	rs.ValuesQuery.Rows = append(rs.ValuesQuery.Rows, jet.UnwindRowsFromModels(rs.Replace.GetColumns(), data)...)
	return rs
}

func (rs *replaceStatementImpl) QUERY(selectStatement SelectStatement) ReplaceStatement {
	rs.ValuesQuery.Query = selectStatement
	return rs
}
