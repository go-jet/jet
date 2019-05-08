package sqlbuilder

import (
	"database/sql"
	"errors"
	"github.com/sub0zero/go-sqlbuilder/sqlbuilder/execution"
	"github.com/sub0zero/go-sqlbuilder/types"
)

func serializeOrderByClauseList(statement statementType, orderByClauses []orderByClause, out *queryData) error {

	for i, value := range orderByClauses {
		if i > 0 {
			out.writeString(", ")
		}

		err := value.serializeAsOrderBy(statement, out)

		if err != nil {
			return err
		}
	}

	return nil
}

func serializeGroupByClauseList(statement statementType, clauses []groupByClause, out *queryData) (err error) {

	for i, c := range clauses {
		if i > 0 {
			out.writeString(", ")
		}

		if c == nil {
			return errors.New("nil clause.")
		}

		if err = c.serializeForGroupBy(statement, out); err != nil {
			return
		}
	}

	return nil
}

func serializeClauseList(statement statementType, clauses []clause, out *queryData) (err error) {

	for i, c := range clauses {
		if i > 0 {
			out.writeString(", ")
		}

		if c == nil {
			return errors.New("nil clause.")
		}

		if err = c.serialize(statement, out); err != nil {
			return
		}
	}

	return nil
}

func serializeExpressionList(statement statementType, expressions []expression, separator string, out *queryData) error {

	for i, value := range expressions {
		if i > 0 {
			out.writeString(separator)
		}

		err := value.serialize(statement, out)

		if err != nil {
			return err
		}
	}

	return nil
}

func serializeProjectionList(statement statementType, projections []projection, out *queryData) error {
	for i, col := range projections {
		if i > 0 {
			out.writeString(", ")
		}
		if col == nil {
			return errors.New("projection expression is nil.")
		}

		if err := col.serializeForProjection(statement, out); err != nil {
			return err
		}
	}

	return nil
}

func serializeColumnList(statement statementType, columns []column, out *queryData) error {
	for i, col := range columns {
		if i > 0 {
			out.writeByte(',')
		}

		if col == nil {
			return errors.New("nil column in columns list.")
		}

		out.writeString(col.Name())
	}

	return nil
}

func Query(statement statement, db types.Db, destination interface{}) error {
	query, args, err := statement.Sql()

	if err != nil {
		return err
	}

	return execution.Query(db, query, args, destination)
}

func Execute(statement statement, db types.Db) (res sql.Result, err error) {
	query, args, err := statement.Sql()

	if err != nil {
		return
	}

	return db.Exec(query, args...)
}
