package jet

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClauseStartWith_Empty(t *testing.T) {
	c := &ClauseStartWith{}
	out := &SQLBuilder{}
	c.Serialize(SelectStatementType, out)
	require.Equal(t, 0, out.Buff.Len())
}

func TestClauseStartWith_WithCondition(t *testing.T) {
	c := &ClauseStartWith{Condition: table1ColBool}
	out := &SQLBuilder{Dialect: defaultDialect}
	c.Serialize(SelectStatementType, out)
	require.Contains(t, out.Buff.String(), "START WITH")
	require.Contains(t, out.Buff.String(), "col_bool")
}

func TestClauseStartWith_serialize(t *testing.T) {
	c := &ClauseStartWith{Condition: table1ColBool}
	out := &SQLBuilder{Dialect: defaultDialect}
	c.serialize(SelectStatementType, out)
	require.Contains(t, out.Buff.String(), "START WITH")
}

func TestClauseConnectBy_Empty(t *testing.T) {
	c := &ClauseConnectBy{}
	out := &SQLBuilder{}
	c.Serialize(SelectStatementType, out)
	require.Equal(t, 0, out.Buff.Len())
}

func TestClauseConnectBy_WithCondition(t *testing.T) {
	c := &ClauseConnectBy{Condition: table1ColBool}
	out := &SQLBuilder{Dialect: defaultDialect}
	c.Serialize(SelectStatementType, out)
	require.Contains(t, out.Buff.String(), "CONNECT BY")
	require.NotContains(t, out.Buff.String(), "NOCYCLE")
}

func TestClauseConnectBy_NoCycle(t *testing.T) {
	c := &ClauseConnectBy{Condition: table1ColBool, NoCycle: true}
	out := &SQLBuilder{Dialect: defaultDialect}
	c.Serialize(SelectStatementType, out)
	require.Contains(t, out.Buff.String(), "CONNECT BY NOCYCLE")
}

func TestClauseConnectBy_serialize(t *testing.T) {
	c := &ClauseConnectBy{Condition: table1ColBool}
	out := &SQLBuilder{Dialect: defaultDialect}
	c.serialize(SelectStatementType, out)
	require.Contains(t, out.Buff.String(), "CONNECT BY")
}

func TestClauseOrderSiblingsBy_Empty(t *testing.T) {
	c := &ClauseOrderSiblingsBy{}
	out := &SQLBuilder{}
	c.Serialize(SelectStatementType, out)
	require.Equal(t, 0, out.Buff.Len())
}

func TestClauseOrderSiblingsBy_WithList(t *testing.T) {
	c := &ClauseOrderSiblingsBy{
		List: []OrderByClause{
			table1Col1.ASC(),
			table1Col3.DESC(),
		},
	}
	out := &SQLBuilder{Dialect: defaultDialect}
	c.Serialize(SelectStatementType, out)
	result := out.Buff.String()
	require.Contains(t, result, "ORDER SIBLINGS BY")
	require.Contains(t, result, "col1")
	require.Contains(t, result, "col3")
}

func TestClauseOrderSiblingsBy_serialize(t *testing.T) {
	c := &ClauseOrderSiblingsBy{
		List: []OrderByClause{table1Col1.ASC()},
	}
	out := &SQLBuilder{Dialect: defaultDialect}
	c.serialize(SelectStatementType, out)
	require.Contains(t, out.Buff.String(), "ORDER SIBLINGS BY")
}

func TestClauseMergeInto_Empty(t *testing.T) {
	c := &ClauseMergeInto{}
	out := &SQLBuilder{}
	c.Serialize(SelectStatementType, out)
	require.Equal(t, 0, out.Buff.Len())
}

func TestClauseMergeInto_WithTarget(t *testing.T) {
	c := &ClauseMergeInto{Target: table1}
	out := &SQLBuilder{Dialect: defaultDialect}
	c.Serialize(SelectStatementType, out)
	require.Contains(t, out.Buff.String(), "MERGE INTO")
	require.Contains(t, out.Buff.String(), "table1")
}

func TestClauseMergeInto_serialize(t *testing.T) {
	c := &ClauseMergeInto{Target: table1}
	out := &SQLBuilder{Dialect: defaultDialect}
	c.serialize(SelectStatementType, out)
	require.Contains(t, out.Buff.String(), "MERGE INTO")
}

func TestClauseMergeUsing_Empty(t *testing.T) {
	c := &ClauseMergeUsing{}
	out := &SQLBuilder{}
	c.Serialize(SelectStatementType, out)
	require.Equal(t, 0, out.Buff.Len())
}

func TestClauseMergeUsing_WithSource(t *testing.T) {
	c := &ClauseMergeUsing{Source: table2}
	out := &SQLBuilder{Dialect: defaultDialect}
	c.Serialize(SelectStatementType, out)
	require.Contains(t, out.Buff.String(), "USING")
	require.Contains(t, out.Buff.String(), "table2")
}

func TestClauseMergeUsing_serialize(t *testing.T) {
	c := &ClauseMergeUsing{Source: table2}
	out := &SQLBuilder{Dialect: defaultDialect}
	c.serialize(SelectStatementType, out)
	require.Contains(t, out.Buff.String(), "USING")
}

func TestClauseMergeOn_Empty(t *testing.T) {
	c := &ClauseMergeOn{}
	out := &SQLBuilder{}
	c.Serialize(SelectStatementType, out)
	require.Equal(t, 0, out.Buff.Len())
}

func TestClauseMergeOn_WithCondition(t *testing.T) {
	c := &ClauseMergeOn{Condition: table1ColBool}
	out := &SQLBuilder{Dialect: defaultDialect}
	c.Serialize(SelectStatementType, out)
	require.Contains(t, out.Buff.String(), "ON")
	require.Contains(t, out.Buff.String(), "col_bool")
}

func TestClauseMergeOn_serialize(t *testing.T) {
	c := &ClauseMergeOn{Condition: table1ColBool}
	out := &SQLBuilder{Dialect: defaultDialect}
	c.serialize(SelectStatementType, out)
	require.Contains(t, out.Buff.String(), "ON")
}

func TestClauseWhenMatched_Empty(t *testing.T) {
	c := &ClauseWhenMatched{}
	out := &SQLBuilder{}
	c.Serialize(SelectStatementType, out)
	require.Equal(t, 0, out.Buff.Len())
}

func TestClauseWhenMatched_Update(t *testing.T) {
	c := &ClauseWhenMatched{
		IsUpdate: true,
		Sets: []ColumnAssigment{
			table1ColBool.SET(Bool(true)),
		},
	}
	out := &SQLBuilder{Dialect: defaultDialect}
	c.Serialize(SelectStatementType, out)
	result := out.Buff.String()
	require.Contains(t, result, "WHEN MATCHED THEN UPDATE SET")
}

func TestClauseWhenMatched_UpdateMultipleSets(t *testing.T) {
	c := &ClauseWhenMatched{
		IsUpdate: true,
		Sets: []ColumnAssigment{
			table1ColBool.SET(Bool(true)),
			table1ColInt.SET(Int(42)),
		},
	}
	out := &SQLBuilder{Dialect: defaultDialect}
	c.Serialize(SelectStatementType, out)
	result := out.Buff.String()
	require.Contains(t, result, "WHEN MATCHED THEN UPDATE SET")
	require.Contains(t, result, ",")
}

func TestClauseWhenMatched_Delete(t *testing.T) {
	c := &ClauseWhenMatched{
		IsDelete: true,
	}
	out := &SQLBuilder{Dialect: defaultDialect}
	c.Serialize(SelectStatementType, out)
	require.Contains(t, out.Buff.String(), "WHEN MATCHED THEN DELETE")
}

func TestClauseWhenMatched_serialize(t *testing.T) {
	c := &ClauseWhenMatched{IsDelete: true}
	out := &SQLBuilder{Dialect: defaultDialect}
	c.serialize(SelectStatementType, out)
	require.Contains(t, out.Buff.String(), "WHEN MATCHED THEN DELETE")
}

func TestClauseWhenNotMatched_Empty(t *testing.T) {
	c := &ClauseWhenNotMatched{}
	out := &SQLBuilder{}
	c.Serialize(SelectStatementType, out)
	require.Equal(t, 0, out.Buff.Len())
}

func TestClauseWhenNotMatched_WithColumnsAndValues(t *testing.T) {
	c := &ClauseWhenNotMatched{
		Columns: []Column{table1Col1, table1ColBool},
		Values:  []interface{}{int64(1), true},
	}
	out := &SQLBuilder{Dialect: defaultDialect}
	c.Serialize(SelectStatementType, out)
	result := out.Buff.String()
	require.Contains(t, result, "WHEN NOT MATCHED THEN INSERT (")
	require.Contains(t, result, "col1")
	require.Contains(t, result, "col_bool")
	require.Contains(t, result, ") VALUES (")
}

func TestClauseWhenNotMatched_serialize(t *testing.T) {
	c := &ClauseWhenNotMatched{
		Columns: []Column{table1Col1},
		Values:  []interface{}{int64(1)},
	}
	out := &SQLBuilder{Dialect: defaultDialect}
	c.serialize(SelectStatementType, out)
	require.Contains(t, out.Buff.String(), "WHEN NOT MATCHED THEN INSERT")
}

func TestClauseReplaceInto_Panic(t *testing.T) {
	defer func() {
		r := recover()
		require.NotNil(t, r, "expected panic for nil table")
	}()
	c := &ClauseReplaceInto{}
	out := &SQLBuilder{}
	c.Serialize(InsertStatementType, out)
}

func TestClauseReplaceInto_WithTable(t *testing.T) {
	c := &ClauseReplaceInto{Table: table1}
	out := &SQLBuilder{Dialect: defaultDialect}
	c.Serialize(InsertStatementType, out)
	result := out.Buff.String()
	require.Contains(t, result, "REPLACE INTO")
	require.Contains(t, result, "table1")
}

func TestClauseReplaceInto_WithTableAndColumns(t *testing.T) {
	c := &ClauseReplaceInto{
		Table:   table1,
		Columns: []Column{table1Col1, table1ColBool},
	}
	out := &SQLBuilder{Dialect: defaultDialect}
	c.Serialize(InsertStatementType, out)
	result := out.Buff.String()
	require.Contains(t, result, "REPLACE INTO")
	require.Contains(t, result, "col1")
	require.Contains(t, result, "col_bool")
}

func TestClauseReplaceInto_GetColumns(t *testing.T) {
	cols := []Column{table1Col1, table1ColBool}
	c := &ClauseReplaceInto{Table: table1, Columns: cols}
	require.Equal(t, cols, c.GetColumns())
}

func TestClauseReplaceInto_serialize(t *testing.T) {
	c := &ClauseReplaceInto{Table: table1}
	out := &SQLBuilder{Dialect: defaultDialect}
	c.serialize(InsertStatementType, out)
	require.Contains(t, out.Buff.String(), "REPLACE INTO")
}
