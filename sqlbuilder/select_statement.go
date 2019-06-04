package sqlbuilder

import (
	"database/sql"
	"github.com/dropbox/godropbox/errors"
	"github.com/sub0zero/go-sqlbuilder/sqlbuilder/execution"
)

type SelectStatement interface {
	Statement
	Expression
	hasRows()

	DISTINCT() SelectStatement
	FROM(table ReadableTable) SelectStatement
	WHERE(expression BoolExpression) SelectStatement
	GROUP_BY(groupByClauses ...groupByClause) SelectStatement
	HAVING(boolExpression BoolExpression) SelectStatement
	ORDER_BY(orderByClauses ...OrderByClause) SelectStatement

	LIMIT(limit int64) SelectStatement
	OFFSET(offset int64) SelectStatement

	FOR_UPDATE() SelectStatement

	AsTable(alias string) expressionTable
}

func SELECT(projection ...projection) SelectStatement {
	return newSelectStatement(nil, projection)
}

// NOTE: SelectStatement purposely does not implement the Table interface since
// mysql's subquery performance is horrible.
type selectStatementImpl struct {
	expressionInterfaceImpl
	isRowsType

	table       ReadableTable
	distinct    bool
	projections []projection
	where       BoolExpression
	groupBy     []groupByClause
	having      BoolExpression
	orderBy     []OrderByClause

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

func newSelectStatement(table ReadableTable, projections []projection) SelectStatement {
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

func (s *selectStatementImpl) FROM(table ReadableTable) SelectStatement {
	s.table = table
	return s
}

func (s *selectStatementImpl) serialize(statement statementType, out *queryData, options ...serializeOption) error {
	if s == nil {
		return errors.New("Select statement is nil. ")
	}
	out.writeString("(")

	out.increaseIdent()
	err := s.serializeImpl(out)
	out.decreaseIdent()

	if err != nil {
		return err
	}

	out.nextLine()
	out.writeString(")")

	return nil
}

func (s *selectStatementImpl) serializeImpl(out *queryData) error {
	if s == nil {
		return errors.New("Select statement is nil. ")
	}

	out.nextLine()
	out.writeString("SELECT")

	if s.distinct {
		out.writeString("DISTINCT")
	}

	if s.projections == nil || len(s.projections) == 0 {
		return errors.New("No column selected for projection.")
	}

	err := out.writeProjection(select_statement, s.projections)

	if err != nil {
		return err
	}

	if s.table != nil {
		if err := out.writeFrom(select_statement, s.table); err != nil {
			return err
		}
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
		out.nextLine()
		out.writeString("LIMIT")
		out.insertPreparedArgument(s.limit)
	}

	if s.offset >= 0 {
		out.nextLine()
		out.writeString("OFFSET")
		out.insertPreparedArgument(s.offset)
	}

	if s.forUpdate {
		out.nextLine()
		out.writeString("FOR UPDATE")
	}

	return nil
}

// Return the properly escaped SQL Statement, against the specified database
func (s *selectStatementImpl) Sql() (query string, args []interface{}, err error) {
	queryData := queryData{}

	err = s.serializeImpl(&queryData)

	if err != nil {
		return "", nil, err
	}

	query, args = queryData.finalize()

	return
}

func (s *selectStatementImpl) DebugSql() (query string, err error) {
	return DebugSql(s)
}

func (s *selectStatementImpl) AsTable(alias string) expressionTable {
	return &expressionTableImpl{
		statement: s,
		alias:     alias,
	}
}

func (s *selectStatementImpl) WHERE(expression BoolExpression) SelectStatement {
	s.where = expression
	return s
}

func (s *selectStatementImpl) GROUP_BY(groupByClauses ...groupByClause) SelectStatement {
	s.groupBy = groupByClauses
	return s
}

func (s *selectStatementImpl) HAVING(expression BoolExpression) SelectStatement {
	s.having = expression
	return s
}

func (s *selectStatementImpl) ORDER_BY(clauses ...OrderByClause) SelectStatement {

	s.orderBy = clauses

	return s
}

func (s *selectStatementImpl) OFFSET(offset int64) SelectStatement {
	s.offset = offset
	return s
}

func (s *selectStatementImpl) LIMIT(limit int64) SelectStatement {
	s.limit = limit
	return s
}

func (s *selectStatementImpl) DISTINCT() SelectStatement {
	s.distinct = true
	return s
}

func (s *selectStatementImpl) FOR_UPDATE() SelectStatement {
	s.forUpdate = true
	return s
}

func (s *selectStatementImpl) Query(db execution.Db, destination interface{}) error {
	return Query(s, db, destination)
}

func (s *selectStatementImpl) Execute(db execution.Db) (res sql.Result, err error) {
	return Execute(s, db)
}

func NumExp(expression Expression) FloatExpression {
	return newFloatExpressionWrap(expression)
}
