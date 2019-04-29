package sqlbuilder

import (
	"database/sql"
	"github.com/dropbox/godropbox/errors"
	"github.com/sub0zero/go-sqlbuilder/types"
)

type UpdateStatement interface {
	Statement

	SET(values ...interface{}) UpdateStatement
	WHERE(expression BoolExpression) UpdateStatement
	RETURNING(projections ...Projection) UpdateStatement
}

func newUpdateStatement(table WritableTable, columns []Column) UpdateStatement {
	return &updateStatementImpl{
		table:   table,
		columns: columns,
	}
}

type updateStatementImpl struct {
	table        WritableTable
	columns      []Column
	updateValues []Clause
	where        BoolExpression
	returning    []Projection
}

func (u *updateStatementImpl) Query(db types.Db, destination interface{}) error {
	return Query(u, db, destination)
}

func (u *updateStatementImpl) Execute(db types.Db) (res sql.Result, err error) {
	return Execute(u, db)
}

func (u *updateStatementImpl) SET(values ...interface{}) UpdateStatement {

	for _, value := range values {
		if clause, ok := value.(Clause); ok {
			u.updateValues = append(u.updateValues, clause)
		} else {
			u.updateValues = append(u.updateValues, Literal(value))
		}
	}

	return u
}

func (u *updateStatementImpl) WHERE(expression BoolExpression) UpdateStatement {
	u.where = expression
	return u
}

func (u *updateStatementImpl) RETURNING(projections ...Projection) UpdateStatement {
	u.returning = projections
	return u
}

func (u *updateStatementImpl) Sql() (sql string, args []interface{}, err error) {
	out := &queryData{}
	out.WriteString("UPDATE ")

	if u.table == nil {
		return "", nil, errors.New("nil tableName.")
	}

	if err = u.table.SerializeSql(out); err != nil {
		return
	}

	if len(u.updateValues) == 0 {
		return "", nil, errors.New("No column updated.")
	}

	out.WriteString(" SET")

	if len(u.columns) > 1 {
		out.WriteString(" ( ")
	} else {
		out.WriteString(" ")
	}

	//for i, column := range u.columns {
	//	if i > 0 {
	//		out.WriteString(", ")
	//	}
	//
	//	out.WriteString(column.Name())
	//
	//	if err != nil {
	//		return
	//	}
	//}

	err = serializeColumnList(u.columns, out)

	if err != nil {
		return "", nil, err
	}

	if len(u.columns) > 1 {
		out.WriteString(" )")
	}

	out.WriteString(" =")

	if len(u.updateValues) > 1 {
		out.WriteString(" (")
	}

	for i, value := range u.updateValues {
		if i > 0 {
			out.WriteString(", ")
		}

		err = value.Serialize(out)

		if err != nil {
			return
		}
	}

	if len(u.updateValues) > 1 {
		out.WriteString(" )")
	}

	if u.where == nil {
		return "", nil, errors.New("Updating without a WHERE clause.")
	}

	out.WriteString(" WHERE ")
	if err = u.where.Serialize(out); err != nil {
		return
	}

	if len(u.returning) > 0 {
		out.WriteString(" RETURNING ")

		err = serializeProjectionList(u.returning, out)

		if err != nil {
			return
		}
	}

	return out.queryBuff.String(), out.args, nil
}
