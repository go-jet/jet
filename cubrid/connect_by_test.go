package cubrid

import (
	"testing"
)

func TestPRIOR(t *testing.T) {
	assertSerialize(t, PRIOR(table1ColInt), "PRIOR table1.col_int")
}

func TestLEVEL(t *testing.T) {
	assertSerialize(t, LEVEL, "(LEVEL)")
}

func TestROWNUM(t *testing.T) {
	assertSerialize(t, ROWNUM, "(ROWNUM)")
}

func TestCONNECT_BY_ISLEAF(t *testing.T) {
	assertSerialize(t, CONNECT_BY_ISLEAF, "(CONNECT_BY_ISLEAF)")
}

func TestCONNECT_BY_ROOT(t *testing.T) {
	assertSerialize(t, CONNECT_BY_ROOT(table1ColString), "CONNECT_BY_ROOT table1.col_string")
}

func TestSYS_CONNECT_BY_PATH(t *testing.T) {
	assertSerialize(t, SYS_CONNECT_BY_PATH(table1ColString, "/"),
		"SYS_CONNECT_BY_PATH(table1.col_string, ?)", "/")
}

func TestSelectWithConnectBy(t *testing.T) {
	parentCol := IntegerColumn("parent_id")
	idCol := IntegerColumn("id")
	nameCol := StringColumn("name")
	tbl := NewTable("db", "employees", "", idCol, parentCol, nameCol)

	assertStatementSql(t,
		tbl.SELECT(idCol, nameCol, LEVEL).
			START_WITH(parentCol.IS_NULL()).
			CONNECT_BY(IntExp(PRIOR(idCol)).EQ(parentCol)),
		`
SELECT employees.id AS "employees.id",
     employees.name AS "employees.name",
     LEVEL
FROM db.employees
START WITH (employees.parent_id IS NULL)
CONNECT BY (PRIOR employees.id = employees.parent_id);
`)
}

func TestSelectWithConnectByNoCycle(t *testing.T) {
	parentCol := IntegerColumn("parent_id")
	idCol := IntegerColumn("id")
	tbl := NewTable("db", "tree", "", idCol, parentCol)

	assertStatementSql(t,
		tbl.SELECT(idCol, LEVEL).
			START_WITH(parentCol.IS_NULL()).
			CONNECT_BY_NOCYCLE(IntExp(PRIOR(idCol)).EQ(parentCol)),
		`
SELECT tree.id AS "tree.id",
     LEVEL
FROM db.tree
START WITH (tree.parent_id IS NULL)
CONNECT BY NOCYCLE (PRIOR tree.id = tree.parent_id);
`)
}

func TestSelectWithOrderSiblingsBy(t *testing.T) {
	parentCol := IntegerColumn("parent_id")
	idCol := IntegerColumn("id")
	nameCol := StringColumn("name")
	tbl := NewTable("db", "employees", "", idCol, parentCol, nameCol)

	assertStatementSql(t,
		tbl.SELECT(idCol, nameCol).
			START_WITH(parentCol.IS_NULL()).
			CONNECT_BY(IntExp(PRIOR(idCol)).EQ(parentCol)).
			ORDER_SIBLINGS_BY(nameCol.ASC()),
		`
SELECT employees.id AS "employees.id",
     employees.name AS "employees.name"
FROM db.employees
START WITH (employees.parent_id IS NULL)
CONNECT BY (PRIOR employees.id = employees.parent_id)
ORDER SIBLINGS BY employees.name ASC;
`)
}
