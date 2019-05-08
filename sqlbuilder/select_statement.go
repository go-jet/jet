package sqlbuilder

import (
	"database/sql"
	"github.com/dropbox/godropbox/errors"
	"github.com/sub0zero/go-sqlbuilder/types"
)

type selectStatement interface {
	statement
	expression

	DISTINCT() selectStatement
	FROM(table readableTable) selectStatement
	WHERE(expression boolExpression) selectStatement
	GROUP_BY(groupByClauses ...groupByClause) selectStatement
	HAVING(boolExpression boolExpression) selectStatement
	ORDER_BY(orderByClauses ...orderByClause) selectStatement

	LIMIT(limit int64) selectStatement
	OFFSET(offset int64) selectStatement

	FOR_UPDATE() selectStatement

	AsTable(alias string) expressionTable
}

func SELECT(projection ...projection) selectStatement {
	return newSelectStatement(nil, projection)
}

// NOTE: selectStatement purposely does not implement the Table interface since
// mysql's subquery performance is horrible.
type selectStatementImpl struct {
	expressionInterfaceImpl

	table       readableTable
	distinct    bool
	projections []projection
	where       boolExpression
	groupBy     []groupByClause
	having      boolExpression
	orderBy     []orderByClause

	limit, offset int64

	forUpdate bool
}

func defaultProjectionAliasing(projections []projection) []projection {
	aliasedProjections := []projection{}

	for _, projection := range projections {
		if column, ok := projection.(column); ok {
			aliasedProjections = append(aliasedProjections, column.DefaultAlias())
		} else if columnList, ok := projection.(ColumnList); ok {
			aliasedProjections = append(aliasedProjections, columnList.DefaultAlias()...)
		} else {
			aliasedProjections = append(aliasedProjections, projection)
		}
	}

	return aliasedProjections
}

func newSelectStatement(table readableTable, projections []projection) selectStatement {
	newSelect := &selectStatementImpl{
		table:       table,
		projections: defaultProjectionAliasing(projections),
		limit:       -1,
		offset:      -1,
		forUpdate:   false,
		distinct:    false,
	}

	newSelect.expressionInterfaceImpl.parent = newSelect

	return newSelect
}

func (s *selectStatementImpl) FROM(table readableTable) selectStatement {
	s.table = table
	return s
}

func (s *selectStatementImpl) serialize(statement statementType, out *queryData) error {

	out.writeString("(")

	err := s.serializeImpl(out)

	if err != nil {
		return err
	}

	out.writeString(")")

	return nil
}

func (s *selectStatementImpl) serializeImpl(out *queryData) error {

	out.writeString("SELECT ")

	if s.distinct {
		out.writeString("DISTINCT ")
	}

	if s.projections == nil || len(s.projections) == 0 {
		return errors.New("No column selected for projection.")
	}

	err := out.writeProjection(select_statement, s.projections)

	if err != nil {
		return err
	}

	out.writeString(" FROM ")

	if s.table == nil {
		return errors.Newf("nil tableName.")
	}

	if err := s.table.serialize(select_statement, out); err != nil {
		return err
	}

	if s.where != nil {
		err := out.writeWhere(select_statement, s.where)

		if err != nil {
			return nil
		}
	}

	if s.groupBy != nil && len(s.groupBy) > 0 {
		err := out.writeGroupBy(select_statement, s.groupBy)

		if err != nil {
			return err
		}
	}

	if s.having != nil {
		err := out.writeHaving(select_statement, s.having)

		if err != nil {
			return err
		}
	}

	if s.orderBy != nil {
		err := out.writeOrderBy(select_statement, s.orderBy)

		if err != nil {
			return err
		}
	}

	if s.limit >= 0 {
		out.writeString(" LIMIT ")
		out.insertArgument(s.limit)
	}

	if s.offset >= 0 {
		out.writeString(" OFFSET ")
		out.insertArgument(s.offset)
	}

	if s.forUpdate {
		out.writeString(" FOR UPDATE")
	}

	return nil
}

// Return the properly escaped SQL statement, against the specified database
func (q *selectStatementImpl) Sql() (query string, args []interface{}, err error) {
	queryData := queryData{}

	err = q.serializeImpl(&queryData)

	if err != nil {
		return "", nil, err
	}

	return queryData.buff.String(), queryData.args, nil
}

func (s *selectStatementImpl) AsTable(alias string) expressionTable {
	return &expressionTableImpl{
		statement: s,
		alias:     alias,
	}
}

func (q *selectStatementImpl) WHERE(expression boolExpression) selectStatement {
	q.where = expression
	return q
}

func (s *selectStatementImpl) GROUP_BY(groupByClauses ...groupByClause) selectStatement {
	s.groupBy = groupByClauses
	return s
}

func (q *selectStatementImpl) HAVING(expression boolExpression) selectStatement {
	q.having = expression
	return q
}

func (q *selectStatementImpl) ORDER_BY(clauses ...orderByClause) selectStatement {

	q.orderBy = clauses

	return q
}

func (q *selectStatementImpl) OFFSET(offset int64) selectStatement {
	q.offset = offset
	return q
}

func (q *selectStatementImpl) LIMIT(limit int64) selectStatement {
	q.limit = limit
	return q
}

func (q *selectStatementImpl) DISTINCT() selectStatement {
	q.distinct = true
	return q
}

func (q *selectStatementImpl) FOR_UPDATE() selectStatement {
	q.forUpdate = true
	return q
}

func (s *selectStatementImpl) Query(db types.Db, destination interface{}) error {
	return Query(s, db, destination)
}

func (u *selectStatementImpl) Execute(db types.Db) (res sql.Result, err error) {
	return Execute(u, db)
}

func NumExp(statement selectStatement) numericExpression {
	return newNumericExpressionWrap(statement)
}
