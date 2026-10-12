package postgres

import (
	"testing"

	"github.com/go-jet/jet/v2/internal/testutils"
)

func TestAggregateFunc_JSON_AGG_OrderBy(t *testing.T) {
	salary := IntegerColumn("salary")
	name := StringColumn("name")

	agg := JSON_AGG(
		Func("json_build_object", String("'salary'"), salary, String("'name'"), name),
	).ORDER_BY(salary.ASC())

	stmt := SELECT(
		IntegerColumn("company_id"),
		agg.AS("employees"),
	).FROM(NewTable("public", "employee", "e", salary, name))

	testutils.AssertStatementSql(t, stmt, `
SELECT company_id AS "company_id",
     json_agg((json_build_object($1::text, e.salary, $2::text, e.name)) ORDER BY e.salary ASC) AS "employees"
FROM public.employee AS e;
`, "'salary'", "'name'")
}

func TestAggregateFunc_JSON_AGG_OrderByDescNullsLast(t *testing.T) {
	salary := IntegerColumn("salary")
	name := StringColumn("name")

	agg := JSON_AGG(
		Func("json_build_object", String("'salary'"), salary, String("'name'"), name),
	).ORDER_BY(salary.DESC().NULLS_LAST())

	stmt := SELECT(
		IntegerColumn("company_id"),
		agg.AS("employees"),
	).FROM(NewTable("public", "employee", "e", salary, name))

	testutils.AssertStatementSql(t, stmt, `
SELECT company_id AS "company_id",
     json_agg((json_build_object($1::text, e.salary, $2::text, e.name)) ORDER BY e.salary DESC NULLS LAST) AS "employees"
FROM public.employee AS e;
`, "'salary'", "'name'")
}

func TestAggregateFunc_MultiColumnOrderBy(t *testing.T) {
	salary := IntegerColumn("salary")
	name := StringColumn("name")

	agg := JSON_AGG(name).ORDER_BY(salary.ASC(), name.DESC())

	stmt := SELECT(
		IntegerColumn("company_id"),
		agg.AS("employees"),
	).FROM(NewTable("public", "employee", "e", salary, name))

	testutils.AssertStatementSql(t, stmt, `
SELECT company_id AS "company_id",
     json_agg((e.name) ORDER BY e.salary ASC, e.name DESC) AS "employees"
FROM public.employee AS e;
`)
}

func TestAggregateFunc_WithoutOrderBy(t *testing.T) {
	salary := IntegerColumn("salary")
	name := StringColumn("name")

	agg := JSON_AGG(
		Func("json_build_object", String("'salary'"), salary, String("'name'"), name),
	)

	stmt := SELECT(
		IntegerColumn("company_id"),
		agg.AS("employees"),
	).FROM(NewTable("public", "employee", "e", salary, name))

	testutils.AssertStatementSql(t, stmt, `
SELECT company_id AS "company_id",
     json_agg((json_build_object($1::text, e.salary, $2::text, e.name))) AS "employees"
FROM public.employee AS e;
`, "'salary'", "'name'")
}

func TestAggregateFunc_ArrayAgg(t *testing.T) {
	name := StringColumn("name")

	agg := ARRAY_AGG(name).ORDER_BY(name.ASC())

	stmt := SELECT(
		IntegerColumn("company_id"),
		agg.AS("names"),
	).FROM(NewTable("public", "employee", "e", name))

	testutils.AssertStatementSql(t, stmt, `
SELECT company_id AS "company_id",
     array_agg((e.name) ORDER BY e.name ASC) AS "names"
FROM public.employee AS e;
`)
}
