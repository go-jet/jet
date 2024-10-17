package jet

import "testing"

func TestProjectionAs(t *testing.T) {
	projectionList := ProjectionList{
		table1Col3,
		SUM(table1ColInt).AS("sum"),
		SUM(table1ColInt).AS("table.sum"),
		ProjectionList{
			table1ColBool,
			AVG(table1ColInt).AS("avg"),
			AVG(table1ColInt).AS("t.avg"),
		},
		ColumnList{table2Col3, table2Col4},
	}

	aliasedProjectionList := projectionList.As("new_alias.*")

	assertProjectionSerialize(t, aliasedProjectionList,
		`table1.col3 AS "new_alias.col3",
SUM(table1.col_int) AS "new_alias.sum",
SUM(table1.col_int) AS "new_alias.sum",
table1.col_bool AS "new_alias.col_bool",
AVG(table1.col_int) AS "new_alias.avg",
AVG(table1.col_int) AS "new_alias.avg",
table2.col3 AS "new_alias.col3",
table2.col4 AS "new_alias.col4"`)

	aliasedProjectionList = projectionList.As("")

	assertProjectionSerialize(t, aliasedProjectionList,
		`table1.col3 AS "col3",
SUM(table1.col_int) AS "sum",
SUM(table1.col_int) AS "sum",
table1.col_bool AS "col_bool",
AVG(table1.col_int) AS "avg",
AVG(table1.col_int) AS "avg",
table2.col3 AS "col3",
table2.col4 AS "col4"`)

	subQueryProjections := projectionList.fromImpl(NewSelectTable(nil, "subQuery", nil))

	assertProjectionSerialize(t, subQueryProjections,
		`"subQuery"."table1.col3" AS "table1.col3",
"subQuery".sum AS "sum",
"subQuery"."table.sum" AS "table.sum",
"subQuery"."table1.col_bool" AS "table1.col_bool",
"subQuery".avg AS "avg",
"subQuery"."t.avg" AS "t.avg",
"subQuery"."table2.col3" AS "table2.col3",
"subQuery"."table2.col4" AS "table2.col4"`)

	aliasedSubQueryProjectionList := subQueryProjections.(ProjectionList).As("subAlias")

	assertProjectionSerialize(t, aliasedSubQueryProjectionList,
		`"subQuery"."table1.col3" AS "subAlias.col3",
"subQuery".sum AS "subAlias.sum",
"subQuery"."table.sum" AS "subAlias.sum",
"subQuery"."table1.col_bool" AS "subAlias.col_bool",
"subQuery".avg AS "subAlias.avg",
"subQuery"."t.avg" AS "subAlias.avg",
"subQuery"."table2.col3" AS "subAlias.col3",
"subQuery"."table2.col4" AS "subAlias.col4"`)
}
