package cubrid

// PRIOR marks the parent side of a hierarchical relationship in CONNECT BY.
//
//	SELECT(...).FROM(table).
//	    START_WITH(parentCol.IS_NULL()).
//	    CONNECT_BY(PRIOR(idCol).EQ(parentCol))
func PRIOR(expr Expression) Expression {
	return CustomExpression(Token("PRIOR"), expr)
}

// LEVEL is a pseudo-column returning the depth of hierarchy (1 = root).
var LEVEL = RawInt("LEVEL")

// ROWNUM is a pseudo-column returning sequential row number.
var ROWNUM = RawInt("ROWNUM")

// SYS_CONNECT_BY_PATH builds a path string from root to current node.
//
//	SYS_CONNECT_BY_PATH(name, '/')  =>  '/root/child/grandchild'
func SYS_CONNECT_BY_PATH(column StringExpression, separator string) StringExpression {
	return StringExp(Func("SYS_CONNECT_BY_PATH", column, String(separator)))
}

// CONNECT_BY_ROOT returns the root node's column value in a hierarchical query.
func CONNECT_BY_ROOT(expr Expression) Expression {
	return CustomExpression(Token("CONNECT_BY_ROOT"), expr)
}

// CONNECT_BY_ISLEAF returns 1 if the current row is a leaf node (no children).
var CONNECT_BY_ISLEAF = RawInt("CONNECT_BY_ISLEAF")

// CONNECT_BY_ISCYCLE returns 1 if the current row causes a cycle (requires NOCYCLE).
var CONNECT_BY_ISCYCLE = RawInt("CONNECT_BY_ISCYCLE")
