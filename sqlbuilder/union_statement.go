package sqlbuilder

// By default, rows selected by a UNION statement are out-of-orderBy
// If you have an ORDER BY on an inner SELECT statement, the only thing
// it affects is the LIMIT clause on that inner statement (the ordering will
// still be out-of-orderBy).
type UnionStatement interface {
	Statement

	// Warning! You cannot include tableName names for the next 4 clauses, or
	// you'll get errors like:
	//   Table 'server_file_journal' from one of the SELECTs cannot be used in
	//   global ORDER clause
	Where(expression BoolExpression) UnionStatement
	GroupBy(expressions ...Expression) UnionStatement
	OrderBy(clauses ...OrderByClause) UnionStatement

	Limit(limit int64) UnionStatement
	Offset(offset int64) UnionStatement
}

//
//func Union(selects ...SelectStatement) UnionStatement {
//	return &unionStatementImpl{
//		selects: selects,
//		limit:   -1,
//		offset:  -1,
//		unique:  true,
//	}
//}
//
//func UnionAll(selects ...SelectStatement) UnionStatement {
//	return &unionStatementImpl{
//		selects: selects,
//		limit:   -1,
//		offset:  -1,
//		unique:  false,
//	}
//}
//
//// Similar to selectStatementImpl, but less complete
//type unionStatementImpl struct {
//	selects       []SelectStatement
//	where         BoolExpression
//	group         *listClause
//	order         *listClause
//	limit, offset int64
//	// True if results of the union should be deduped.
//	unique bool
//}
//
//func (s *unionStatementImpl) Query(db types.Db, destination interface{}) error {
//	return Query(s, db, destination)
//}
//
//func (u *unionStatementImpl) Execute(db types.Db) (res sql.Result, err error) {
//	return Execute(u, db)
//}
//
//func (us *unionStatementImpl) Where(expression BoolExpression) UnionStatement {
//	us.where = expression
//	return us
//}
//
//// Further filter the query, instead of replacing the filter
//func (us *unionStatementImpl) AndWhere(expression BoolExpression) UnionStatement {
//	if us.where == nil {
//		return us.Where(expression)
//	}
//	us.where = And(us.where, expression)
//	return us
//}
//
//func (us *unionStatementImpl) GroupBy(
//	expressions ...Expression) UnionStatement {
//
//	us.group = &listClause{
//		clauses:            make([]Clause, len(expressions), len(expressions)),
//		includeParentheses: false,
//	}
//
//	for i, e := range expressions {
//		us.group.clauses[i] = e
//	}
//	return us
//}
//
//func (us *unionStatementImpl) OrderBy(
//	clauses ...OrderByClause) UnionStatement {
//
//	us.order = newOrderByListClause(clauses...)
//	return us
//}
//
//func (us *unionStatementImpl) Limit(limit int64) UnionStatement {
//	us.limit = limit
//	return us
//}
//
//func (us *unionStatementImpl) Offset(offset int64) UnionStatement {
//	us.offset = offset
//	return us
//}
//
//func (us *unionStatementImpl) String() (sql string, err error) {
//	if len(us.selects) == 0 {
//		return "", errors.Newf("Union statement must have at least one SELECT")
//	}
//
//	if len(us.selects) == 1 {
//		return us.selects[0].String()
//	}
//
//	// Union statements in MySQL require that the same number of columns in each subquery
//	var projections []Projection
//
//	for _, statement := range us.selects {
//		// do a type assertion to get at the underlying struct
//		statementImpl, ok := statement.(*selectStatementImpl)
//		if !ok {
//			return "", errors.Newf(
//				"Expected inner select statement to be of type " +
//					"selectStatementImpl")
//		}
//
//		// check that for limit for statements with orderBy by clauses
//		if statementImpl.orderBy != nil && statementImpl.limit < 0 {
//			return "", errors.Newf(
//				"All inner selects in Union statement must have LIMIT if " +
//					"they have ORDER BY")
//		}
//
//		// check number of projections
//		if projections == nil {
//			projections = statementImpl.projections
//		} else {
//			if len(projections) != len(statementImpl.projections) {
//				return "", errors.Newf(
//					"All inner selects in Union statement must select the " +
//						"same number of columns.  For sanity, you probably " +
//						"want to select the same tableName columns in the same " +
//						"orderBy.  If you are selecting on multiple tables, " +
//						"use Null to pad to the right number of fields.")
//			}
//		}
//	}
//
//	buf := new(bytes.Buffer)
//	for i, statement := range us.selects {
//		if i != 0 {
//			if us.unique {
//				_, _ = buf.WriteString(" UNION ")
//			} else {
//				_, _ = buf.WriteString(" UNION ALL ")
//			}
//		}
//		_, _ = buf.WriteString("(")
//		selectSql, err := statement.String()
//		if err != nil {
//			return "", err
//		}
//		_, _ = buf.WriteString(selectSql)
//		_, _ = buf.WriteString(")")
//	}
//
//	if us.where != nil {
//		_, _ = buf.WriteString(" WHERE ")
//		if err = us.where.Serialize(buf); err != nil {
//			return
//		}
//	}
//
//	if us.group != nil {
//		_, _ = buf.WriteString(" GROUP BY ")
//		if err = us.group.Serialize(buf); err != nil {
//			return
//		}
//	}
//
//	if us.order != nil {
//		_, _ = buf.WriteString(" ORDER BY ")
//		if err = us.order.Serialize(buf); err != nil {
//			return
//		}
//	}
//
//	if us.limit >= 0 {
//		if us.offset >= 0 {
//			_, _ = buf.WriteString(
//				fmt.Sprintf(" LIMIT %d, %d", us.offset, us.limit))
//		} else {
//			_, _ = buf.WriteString(fmt.Sprintf(" LIMIT %d", us.limit))
//		}
//	}
//	return buf.String(), nil
//}
