package sqlbuilder

import (
	"database/sql"
	"errors"
	"github.com/go-jet/jet/sqlbuilder/execution"
	"reflect"
)

func serializeOrderByClauseList(statement statementType, orderByClauses []OrderByClause, out *queryData) error {

	for i, value := range orderByClauses {
		if i > 0 {
			out.writeString(", ")
		}

		err := value.serializeForOrderBy(statement, out)

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

func serializeExpressionList(statement statementType, expressions []Expression, separator string, out *queryData) error {

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
			out.writeString(",")
			out.nextLine()
		}

		if col == nil {
			return errors.New("projection Expression is nil")
		}

		if err := col.serializeForProjection(statement, out); err != nil {
			return err
		}
	}

	return nil
}

func serializeColumnList(statement statementType, columns []Column, out *queryData) error {
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

func isNil(v interface{}) bool {
	return v == nil || (reflect.ValueOf(v).Kind() == reflect.Ptr && reflect.ValueOf(v).IsNil())
}

func columnListToProjectionList(columns []Column) []projection {
	var ret []projection

	for _, column := range columns {
		ret = append(ret, column)
	}

	return ret
}

func Query(statement Statement, db execution.Db, destination interface{}) error {
	query, args, err := statement.Sql()

	if err != nil {
		return err
	}

	return execution.Query(db, query, args, destination)
}

func Execute(statement Statement, db execution.Db) (res sql.Result, err error) {
	query, args, err := statement.Sql()

	if err != nil {
		return
	}

	return db.Exec(query, args...)
}
