package sqlbuilder

import (
	"database/sql"
	"errors"
	"github.com/sub0zero/go-sqlbuilder/sqlbuilder/execution"
	"github.com/sub0zero/go-sqlbuilder/types"
)

func serializeOrderByClauseList(orderByClauses []OrderByClause, out *queryData) error {

	for i, value := range orderByClauses {
		if i > 0 {
			out.WriteString(", ")
		}

		err := value.Serialize(out)

		if err != nil {
			return err
		}
	}

	return nil
}

func serializeClauseList(clauses []Clause, out *queryData) (err error) {

	for i, c := range clauses {
		if i > 0 {
			out.WriteString(", ")
		}

		if c == nil {
			return errors.New("nil clause.")
		}

		if err = c.Serialize(out); err != nil {
			return
		}
	}

	return nil
}

func serializeExpressionList(expressions []Expression, separator string, out *queryData) error {

	for i, value := range expressions {
		if i > 0 {
			out.WriteString(separator)
		}

		err := value.Serialize(out)

		if err != nil {
			return err
		}
	}

	return nil
}

func serializeProjectionList(projections []Projection, out *queryData) error {
	for i, col := range projections {
		if i > 0 {
			out.WriteString(", ")
		}
		if col == nil {
			return errors.New("Projection expression is nil.")
		}

		if err := col.SerializeForProjection(out); err != nil {
			return err
		}
	}

	return nil
}

func serializeColumnList(columns []Column, out *queryData) error {
	for i, col := range columns {
		if i > 0 {
			out.WriteByte(',')
		}

		if col == nil {
			return errors.New("nil column in columns list.")
		}

		out.WriteString(col.Name())
	}

	return nil
}

func Query(statement Statement, db types.Db, destination interface{}) error {
	query, args, err := statement.Sql()

	if err != nil {
		return err
	}

	return execution.Query(db, query, args, destination)
}

func Execute(statement Statement, db types.Db) (res sql.Result, err error) {
	query, args, err := statement.Sql()

	if err != nil {
		return
	}

	return db.Exec(query, args...)
}
