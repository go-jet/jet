package sqlbuilder

import (
	"bytes"
	"database/sql"
	"github.com/sub0zero/go-sqlbuilder/sqlbuilder/execution"
	"github.com/sub0zero/go-sqlbuilder/types"
)

func serializeExpressionList(expressions []Expression, buf *bytes.Buffer) error {
	for i, value := range expressions {
		if i > 0 {
			buf.WriteString(", ")
		}

		err := value.SerializeSql(buf)

		if err != nil {
			return err
		}
	}

	return nil
}

func serializeProjectionList(projections []Projection, buf *bytes.Buffer) error {
	for i, value := range projections {
		if i > 0 {
			buf.WriteString(", ")
		}

		err := value.SerializeForProjection(buf)

		if err != nil {
			return err
		}
	}

	return nil
}

func Query(statement Statement, db types.Db, destination interface{}) error {
	query, err := statement.String()

	if err != nil {
		return err
	}

	return execution.Execute(db, query, destination)
}

func Execute(statement Statement, db types.Db) (res sql.Result, err error) {
	query, err := statement.String()

	if err != nil {
		return
	}

	res, err = db.Exec(query)

	return
}
