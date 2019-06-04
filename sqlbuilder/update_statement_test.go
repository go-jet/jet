package sqlbuilder

import (
	"fmt"
	"gotest.tools/assert"
	"testing"
)

//
// UPDATE Statement tests =====================================================
//

func TestUpdate(t *testing.T) {
	stmt := table1.UPDATE(table1Col1, table1ColFloat).
		SET(table1.SELECT(table1ColFloat, table2Col3)).
		WHERE(table1Col1.EQ(Int(2))).
		RETURNING(table1Col1)

	stmtStr, _, err := stmt.Sql()

	assert.NilError(t, err)

	fmt.Println(stmtStr)

	assert.Equal(t, stmtStr, `
UPDATE db.table1 SET (col1,colFloat) = (
     SELECT table1.colFloat AS "table1.colFloat",
          table2.col3 AS "table2.col3"
     FROM db.table1
)
WHERE table1.col1 = $1
RETURNING table1.col1 AS "table1.col1";
`)
}

//func (s *StmtSuite) TestUpdateNilColumn(c *gc.C) {
//	stmt := table1.UPDATE().SET(nil, literal(1))
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
//	stmt := table1.UPDATE().SET(table1Col1, literal(1))
//	_, err := stmt.String()
//	c.Assert(err, gc.NotNil)
//}
//
//func (s *StmtSuite) TestUpdateSingleValue(c *gc.C) {
//	stmt := table1.UPDATE().SET(table1Col1, literal(1))
//	stmt.WHERE(EqString(table1ColFloat, 2))
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
//	stmt := table1.UPDATE().SET(table1.C("col1"), literal(1))
//	stmt.WHERE(EqString(table1ColFloat, 2))
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
//	stmt.SET(table1Col1, literal(1))
//	stmt.SET(table1ColFloat, literal(2))
//	stmt.WHERE(EqString(table1ColFloat, 3))
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
//	stmt := table1.UPDATE().SET(table1Col1, literal(1))
//	stmt.WHERE(EqString(table1ColFloat, 2))
//	stmt.ORDER_BY(table1ColFloat)
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
//	stmt := table1.UPDATE().SET(table1Col1, literal(1))
//	stmt.WHERE(EqString(table1ColFloat, 2))
//	stmt.LIMIT(5)
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
