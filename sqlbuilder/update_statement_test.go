package sqlbuilder

import (
	"fmt"
	"gotest.tools/assert"
	"testing"
)

//
// UPDATE statement tests =====================================================
//

func TestUpdate(t *testing.T) {
	stmt := table1.UPDATE(table1Col1, table1Col2).
		SET(table1.SELECT(table1Col2)).
		WHERE(table1Col1.EqL(2))

	stmtStr, err := stmt.String()

	assert.NilError(t, err)

	fmt.Println(stmtStr)
}

//func (s *StmtSuite) TestUpdateNilColumn(c *gc.C) {
//	stmt := table1.UPDATE().SET(nil, Literal(1))
//	_, err := stmt.String()
//	c.Assert(err, gc.NotNil)
//}
//
//func (s *StmtSuite) TestUpdateNilExpr(c *gc.C) {
//	stmt := table1.UPDATE().SET(table1Col1, nil)
//	_, err := stmt.String()
//	c.Assert(err, gc.NotNil)
//}
//
//func (s *StmtSuite) TestUpdateUnconditionally(c *gc.C) {
//	stmt := table1.UPDATE().SET(table1Col1, Literal(1))
//	_, err := stmt.String()
//	c.Assert(err, gc.NotNil)
//}
//
//func (s *StmtSuite) TestUpdateSingleValue(c *gc.C) {
//	stmt := table1.UPDATE().SET(table1Col1, Literal(1))
//	stmt.WHERE(EqL(table1Col2, 2))
//	sql, err := stmt.String()
//	c.Assert(err, gc.IsNil)
//
//	c.Assert(
//		sql,
//		gc.Equals,
//		"UPDATE db.table1 SET table1.col1=1 WHERE table1.col2=2")
//}
//
//func (s *StmtSuite) TestUpdateUsingDeferredLookupColumns(c *gc.C) {
//	stmt := table1.UPDATE().SET(table1.C("col1"), Literal(1))
//	stmt.WHERE(EqL(table1Col2, 2))
//	sql, err := stmt.String()
//	c.Assert(err, gc.IsNil)
//
//	c.Assert(
//		sql,
//		gc.Equals,
//		"UPDATE db.table1 SET table1.col1=1 WHERE table1.col2=2")
//}
//
//func (s *StmtSuite) TestUpdateMultiValues(c *gc.C) {
//	stmt := table1.UPDATE()
//	stmt.SET(table1Col1, Literal(1))
//	stmt.SET(table1Col2, Literal(2))
//	stmt.WHERE(EqL(table1Col2, 3))
//	sql, err := stmt.String()
//	c.Assert(err, gc.IsNil)
//
//	c.Assert(
//		sql,
//		gc.Equals,
//		"UPDATE db.table1 "+
//			"SET table1.col1=1, table1.col2=2 "+
//			"WHERE table1.col2=3")
//}
//
//func (s *StmtSuite) TestUpdateWithOrderBy(c *gc.C) {
//	stmt := table1.UPDATE().SET(table1Col1, Literal(1))
//	stmt.WHERE(EqL(table1Col2, 2))
//	stmt.OrderBy(table1Col2)
//	sql, err := stmt.String()
//	c.Assert(err, gc.IsNil)
//
//	c.Assert(
//		sql,
//		gc.Equals,
//		"UPDATE db.table1 "+
//			"SET table1.col1=1 "+
//			"WHERE table1.col2=2 "+
//			"ORDER BY table1.col2")
//}
//
//func (s *StmtSuite) TestUpdateWithLimit(c *gc.C) {
//	stmt := table1.UPDATE().SET(table1Col1, Literal(1))
//	stmt.WHERE(EqL(table1Col2, 2))
//	stmt.Limit(5)
//	sql, err := stmt.String()
//	c.Assert(err, gc.IsNil)
//
//	c.Assert(
//		sql,
//		gc.Equals,
//		"UPDATE db.table1 "+
//			"SET table1.col1=1 "+
//			"WHERE table1.col2=2 "+
//			"LIMIT 5")
//}
