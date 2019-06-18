package sqlbuilder

import (
	"database/sql"
	"errors"
	"github.com/go-jet/jet/sqlbuilder/execution"
)

var (
	UPDATE        = newLock("UPDATE")
	NO_KEY_UPDATE = newLock("NO KEY UPDATE")
	SHARE         = newLock("SHARE")
	KEY_SHARE     = newLock("KEY SHARE")
)

type SelectStatement interface {
	Statement
	Expression

	DISTINCT() SelectStatement
	FROM(table ReadableTable) SelectStatement
	WHERE(expression BoolExpression) SelectStatement
	GROUP_BY(groupByClauses ...groupByClause) SelectStatement
	HAVING(boolExpression BoolExpression) SelectStatement
	ORDER_BY(orderByClauses ...OrderByClause) SelectStatement

	LIMIT(limit int64) SelectStatement
	OFFSET(offset int64) SelectStatement

	FOR(lock SelectLock) SelectStatement

	AsTable(alias string) ExpressionTable

	projections() []projection
}

func SELECT(projection1 projection, projections ...projection) SelectStatement {
	return newSelectStatement(nil, append([]projection{projection1}, projections...))
}

type selectStatementImpl struct {
	expressionInterfaceImpl

	table          ReadableTable
	distinct       bool
	projectionList []projection
	where          BoolExpression
	groupBy        []groupByClause
	having         BoolExpression
	orderBy        []OrderByClause

	limit, offset int64

	lockFor SelectLock
}

func newSelectStatement(table ReadableTable, projections []projection) SelectStatement {
	newSelect := &selectStatementImpl{
		table:          table,
		projectionList: projections,
		limit:          -1,
		offset:         -1,
		distinct:       false,
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
		return errors.New("Select expression is nil. ")
	}
	out.writeString("(")

	out.increaseIdent()
	err := s.serializeImpl(out)
	out.decreaseIdent()

	if err != nil {
		return err
	}

	out.newLine()
	out.writeString(")")

	return nil
}

func (s *selectStatementImpl) serializeImpl(out *queryData) error {
	if s == nil {
		return errors.New("Select expression is nil. ")
	}

	out.newLine()
	out.writeString("SELECT")

	if s.distinct {
		out.writeString("DISTINCT")
	}

	if len(s.projectionList) == 0 {
		return errors.New("no column selected for projection")
	}

	err := out.writeProjections(select_statement, s.projectionList)

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
		out.newLine()
		out.writeString("LIMIT")
		out.insertPreparedArgument(s.limit)
	}

	if s.offset >= 0 {
		out.newLine()
		out.writeString("OFFSET")
		out.insertPreparedArgument(s.offset)
	}

	if s.lockFor != nil {
		out.newLine()
		out.writeString("FOR")
		err := s.lockFor.serialize(select_statement, out)

		if err != nil {
			return err
		}
	}

	return nil
}

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

func (s *selectStatementImpl) projections() []projection {
	return s.projectionList
}

func (s *selectStatementImpl) AsTable(alias string) ExpressionTable {
	return newExpressionTable(s.parent, alias, s.projectionList)
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

func (s *selectStatementImpl) FOR(lock SelectLock) SelectStatement {
	s.lockFor = lock
	return s
}

type SelectLock interface {
	clause

	NOWAIT() SelectLock
	SKIP_LOCKED() SelectLock
}

type selectLockImpl struct {
	lockStrength       string
	noWait, skipLocked bool
}

func newLock(name string) func() SelectLock {
	return func() SelectLock {
		return newSelectLock(name)
	}
}

func newSelectLock(lockStrength string) SelectLock {
	return &selectLockImpl{lockStrength: lockStrength}
}

func (s *selectLockImpl) NOWAIT() SelectLock {
	s.noWait = true
	return s
}

func (s *selectLockImpl) SKIP_LOCKED() SelectLock {
	s.skipLocked = true
	return s
}

func (s *selectLockImpl) serialize(statement statementType, out *queryData, options ...serializeOption) error {
	out.writeString(s.lockStrength)

	if s.noWait {
		out.writeString("NOWAIT")
	}

	if s.skipLocked {
		out.writeString("SKIP LOCKED")
	}

	return nil
}

func (s *selectStatementImpl) Query(db execution.Db, destination interface{}) error {
	return Query(s, db, destination)
}

func (s *selectStatementImpl) Exec(db execution.Db) (res sql.Result, err error) {
	return Exec(s, db)
}
